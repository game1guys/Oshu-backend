/**
 * Razorpay: ride payment orders (customer pays Oshu), verify, webhook.
 * Requires RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET. Webhook: RAZORPAY_WEBHOOK_SECRET.
 */

import crypto from 'crypto';
import express from 'express';
import Razorpay from 'razorpay';
import { captainNetFromCustomerPayment } from './walletUtils.js';

function getRazorpay() {
  const key_id = process.env.RAZORPAY_KEY_ID;
  const key_secret = process.env.RAZORPAY_KEY_SECRET;
  if (!key_id || !key_secret) {
    return null;
  }
  return new Razorpay({ key_id, key_secret });
}

function verifyPaymentSignature(orderId, paymentId, signature, secret) {
  const body = `${orderId}|${paymentId}`;
  const expected = crypto.createHmac('sha256', secret).update(body).digest('hex');
  const sig = String(signature);
  if (expected.length !== sig.length) {
    return false;
  }
  return crypto.timingSafeEqual(Buffer.from(expected), Buffer.from(sig));
}

/**
 * Shared: apply online credit via RPC (idempotent).
 */
export async function applyOnlineCreditFromPayment(supabase, { rideId, orderId, paymentId, paymentEntity }) {
  const amountPaise = Number(paymentEntity?.amount);
  if (!Number.isFinite(amountPaise) || amountPaise <= 0) {
    return { ok: false, error: 'bad_payment_amount' };
  }

  const { data: ride, error: rErr } = await supabase.from('ride_requests').select('*').eq('id', rideId).maybeSingle();
  if (rErr || !ride) {
    return { ok: false, error: 'ride_not_found' };
  }
  if (ride.payment_status !== 'awaiting_payment') {
    return { ok: true, duplicate: true };
  }

  const expectedPaise = Math.round(Number(ride.final_payable_inr ?? ride.quoted_price_inr) * 100);
  if (expectedPaise > 0 && amountPaise !== expectedPaise) {
    return { ok: false, error: 'amount_mismatch' };
  }

  const net = captainNetFromCustomerPayment(ride.final_payable_inr ?? ride.quoted_price_inr);
  if (net <= 0) {
    return { ok: false, error: 'bad_net' };
  }

  const { data: rpcRaw, error: rpcErr } = await supabase.rpc('apply_captain_online_ride_credit', {
    p_ride_id: rideId,
    p_payment_id: paymentId,
    p_order_id: orderId,
    p_net_inr: net,
  });
  if (rpcErr) {
    return { ok: false, error: rpcErr.message };
  }
  const row = rpcRaw && typeof rpcRaw === 'object' && !Array.isArray(rpcRaw) ? rpcRaw : {};
  if (row.ok !== true) {
    return { ok: false, error: row.error ?? 'rpc_failed' };
  }
  return { ok: true, duplicate: Boolean(row.duplicate), result: row };
}

export function registerPaymentRoutes(app, { supabase, getUserIdFromAccessToken }) {
  const keySecret = process.env.RAZORPAY_KEY_SECRET;

  /** Customer: create Razorpay order for a completed ride awaiting online payment. */
  app.post('/api/payments/razorpay/order', async (req, res) => {
    const rzp = getRazorpay();
    if (!rzp) {
      return res.status(503).json({ error: 'Razorpay is not configured on the server' });
    }
    const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
    if (!token || !supabase) {
      return res.status(400).json({ error: 'Missing Authorization' });
    }
    const uid = await getUserIdFromAccessToken(token);
    if (!uid) {
      return res.status(401).json({ error: 'Invalid token' });
    }

    const rideId = req.body?.ride_id;
    if (!rideId || typeof rideId !== 'string') {
      return res.status(400).json({ error: 'ride_id is required' });
    }

    const { data: ride, error } = await supabase.from('ride_requests').select('*').eq('id', rideId).maybeSingle();
    if (error) {
      return res.status(500).json({ error: error.message });
    }
    if (!ride || ride.customer_id !== uid) {
      return res.status(404).json({ error: 'Ride not found' });
    }
    if (ride.status !== 'completed') {
      return res.status(409).json({ error: 'Ride is not completed' });
    }
    if (ride.payment_status !== 'awaiting_payment') {
      return res.status(409).json({ error: 'Payment is not required for this ride' });
    }

    const inr = Number(ride.final_payable_inr ?? ride.quoted_price_inr);
    if (!Number.isFinite(inr) || inr <= 0) {
      return res.status(400).json({ error: 'Invalid payable amount' });
    }
    const paise = Math.round(inr * 100);

    try {
      const order = await rzp.orders.create({
        amount: paise,
        currency: 'INR',
        receipt: String(rideId).slice(0, 40),
        notes: {
          ride_id: rideId,
          captain_id: ride.captain_id ?? '',
          customer_id: uid,
        },
      });

      await supabase.from('ride_requests').update({ razorpay_order_id: order.id }).eq('id', rideId);

      return res.json({
        key_id: process.env.RAZORPAY_KEY_ID,
        order_id: order.id,
        amount_paise: paise,
        currency: 'INR',
        ride_id: rideId,
        prefill: {
          name: 'Oshu ride',
          description: `Ride ${String(rideId).slice(0, 8)}`,
        },
      });
    } catch (e) {
      console.error('[payments] order create failed:', e?.message ?? e);
      return res.status(500).json({ error: e?.message ?? 'Could not create order' });
    }
  });

  /** Customer app: verify payment after Checkout success (primary path). */
  app.post('/api/payments/razorpay/verify', async (req, res) => {
    const rzp = getRazorpay();
    if (!rzp || !keySecret) {
      return res.status(503).json({ error: 'Razorpay is not configured' });
    }
    const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
    if (!token || !supabase) {
      return res.status(400).json({ error: 'Missing Authorization' });
    }
    const uid = await getUserIdFromAccessToken(token);
    if (!uid) {
      return res.status(401).json({ error: 'Invalid token' });
    }

    const { ride_id, razorpay_order_id, razorpay_payment_id, razorpay_signature } = req.body ?? {};
    if (!ride_id || !razorpay_order_id || !razorpay_payment_id || !razorpay_signature) {
      return res.status(400).json({ error: 'Missing payment fields' });
    }

    if (!verifyPaymentSignature(razorpay_order_id, razorpay_payment_id, razorpay_signature, keySecret)) {
      return res.status(400).json({ error: 'Invalid signature' });
    }

    let payment;
    try {
      payment = await rzp.payments.fetch(razorpay_payment_id);
    } catch (e) {
      return res.status(400).json({ error: 'Could not fetch payment' });
    }
    if (payment.order_id !== razorpay_order_id) {
      return res.status(400).json({ error: 'Order mismatch' });
    }
    if (payment.status !== 'captured' && payment.status !== 'authorized') {
      return res.status(409).json({ error: `Payment not captured (${payment.status})` });
    }

    const { data: ride } = await supabase.from('ride_requests').select('customer_id').eq('id', ride_id).maybeSingle();
    if (!ride || ride.customer_id !== uid) {
      return res.status(404).json({ error: 'Ride not found' });
    }

    const out = await applyOnlineCreditFromPayment(supabase, {
      rideId: ride_id,
      orderId: razorpay_order_id,
      paymentId: razorpay_payment_id,
      paymentEntity: payment,
    });
    if (!out.ok) {
      return res.status(400).json({ error: out.error ?? 'Could not apply payment' });
    }
    return res.json({ ok: true, duplicate: Boolean(out.duplicate) });
  });
}

/**
 * Raw-body webhook handler (mount with express.raw).
 */
export function registerRazorpayWebhook(app, supabase) {
  app.post('/api/webhooks/razorpay', express.raw({ type: 'application/json' }), async (req, res) => {
    const secret = process.env.RAZORPAY_WEBHOOK_SECRET;
    if (!secret) {
      return res.status(503).send('Webhook secret not configured');
    }

    const sig = req.headers['x-razorpay-signature'];
    if (!sig || typeof sig !== 'string') {
      return res.status(400).send('Missing signature');
    }

    const body = req.body instanceof Buffer ? req.body.toString('utf8') : String(req.body ?? '');
    try {
      const valid = Razorpay.validateWebhookSignature(body, sig, secret);
      if (!valid) {
        return res.status(400).send('Bad signature');
      }
    } catch {
      return res.status(400).send('Bad signature');
    }

    let payload;
    try {
      payload = JSON.parse(body);
    } catch {
      return res.status(400).send('Invalid JSON');
    }

    const event = payload.event;
    if (event !== 'payment.captured') {
      return res.json({ ok: true, ignored: event });
    }

    const payment = payload.payload?.payment?.entity;
    if (!payment?.id) {
      return res.json({ ok: true });
    }

    const orderId = payment.order_id;
    const notes = payment.notes ?? {};
    const rideId = notes.ride_id;
    if (!rideId || !orderId) {
      return res.json({ ok: true, note: 'no ride in notes' });
    }

    const out = await applyOnlineCreditFromPayment(supabase, {
      rideId,
      orderId,
      paymentId: payment.id,
      paymentEntity: payment,
    });
    if (!out.ok) {
      console.error('[webhook] apply credit failed', out.error);
    }
    return res.json({ ok: true });
  });
}
