/**
 * Captain wallet: summary, UPI, Razorpay X payouts (when configured).
 */

import { isPlausibleUpiVpa, minWithdrawInr, normalizeUpiVpa, platformFeePct } from './walletUtils.js';

const CAPTAIN_PLATFORM_DUE_CAP_INR = Math.max(
  0,
  Number(process.env.OSHU_CAPTAIN_PLATFORM_DUE_CAP_INR ?? 1000) || 1000,
);

async function razorpayPost(path, body) {
  const key_id = process.env.RAZORPAY_KEY_ID;
  const key_secret = process.env.RAZORPAY_KEY_SECRET;
  if (!key_id || !key_secret) {
    return { ok: false, error: 'Razorpay not configured' };
  }
  const auth = Buffer.from(`${key_id}:${key_secret}`).toString('base64');
  const r = await fetch(`https://api.razorpay.com${path}`, {
    method: 'POST',
    headers: {
      Authorization: `Basic ${auth}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(body),
  });
  const text = await r.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }
  if (!r.ok) {
    return { ok: false, error: json?.error?.description ?? json?.error ?? text ?? `HTTP ${r.status}` };
  }
  return { ok: true, data: json };
}

async function requireUser(supabase, getUserId, req, res) {
  const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
  if (!token || !supabase) {
    res.status(400).json({ error: 'Missing Authorization' });
    return null;
  }
  const uid = await getUserId(token);
  if (!uid) {
    res.status(401).json({ error: 'Invalid token' });
    return null;
  }
  return uid;
}

async function ensureContactAndFundAccount(supabase, profile) {
  const uid = profile.id;
  let contactId = profile.razorpay_contact_id;
  let fundId = profile.razorpay_fund_account_id;
  const vpa = normalizeUpiVpa(profile.captain_wallet_upi_id);

  if (contactId && fundId) {
    return { contactId, fundId };
  }

  const phone = String(profile.phone ?? '9999999999').replace(/\D/g, '').slice(-10);
  const contactBody = {
    name: String(profile.full_name ?? 'Captain').slice(0, 50),
    email: `${uid.slice(0, 8)}@captain.oshu.local`,
    contact: phone.length === 10 ? phone : '9999999999',
    type: 'vendor',
    reference_id: `oshu_cap_${uid.replace(/-/g, '').slice(0, 24)}`,
  };

  const c = await razorpayPost('/v1/contacts', contactBody);
  if (!c.ok) {
    return { error: c.error ?? 'contact_failed' };
  }
  contactId = c.data?.id;
  if (!contactId) {
    return { error: 'contact_no_id' };
  }

  const f = await razorpayPost('/v1/fund_accounts', {
    contact_id: contactId,
    account_type: 'vpa',
    vpa: { address: vpa },
  });
  if (!f.ok) {
    return { error: f.error ?? 'fund_account_failed' };
  }
  fundId = f.data?.id;
  if (!fundId) {
    return { error: 'fund_no_id' };
  }

  await supabase
    .from('profiles')
    .update({ razorpay_contact_id: contactId, razorpay_fund_account_id: fundId })
    .eq('id', uid);

  return { contactId, fundId };
}

export function registerCaptainWalletRoutes(app, { supabase, getUserIdFromAccessToken }) {
  /** GET /api/captain-wallet/summary */
  app.get('/api/captain-wallet/summary', async (req, res) => {
    const uid = await requireUser(supabase, getUserIdFromAccessToken, req, res);
    if (!uid) {
      return;
    }
    const { data: p, error } = await supabase
      .from('profiles')
      .select(
        'id, role, captain_wallet_balance_inr, captain_wallet_withdrawn_total_inr, captain_cod_total_inr, captain_online_credited_total_inr, captain_wallet_upi_id, captain_oshu_platform_due_inr',
      )
      .eq('id', uid)
      .maybeSingle();
    if (error) {
      return res.status(500).json({ error: error.message });
    }
    if (!p || p.role !== 'captain') {
      return res.status(403).json({ error: 'Captains only' });
    }

    const inOshu = Number(p.captain_wallet_balance_inr ?? 0);
    const withdrawn = Number(p.captain_wallet_withdrawn_total_inr ?? 0);
    const cod = Number(p.captain_cod_total_inr ?? 0);
    const onlineCredited = Number(p.captain_online_credited_total_inr ?? 0);
    const oshuPlatformDue = Number(p.captain_oshu_platform_due_inr ?? 0);
    const [{ data: qrRows }, { data: personalRows }] = await Promise.all([
      supabase
        .from('ride_requests')
        .select('final_payable_inr')
        .eq('captain_id', uid)
        .eq('status', 'completed')
        .eq('payment_status', 'paid_oshu_qr'),
      supabase
        .from('ride_requests')
        .select('final_payable_inr')
        .eq('captain_id', uid)
        .eq('status', 'completed')
        .or('payment_status.eq.paid_cod,captain_customer_collection.eq.captain_own'),
    ]);
    const lifetimeOshuQrCollectedInr = (qrRows ?? []).reduce(
      (sum, row) => sum + Number(row.final_payable_inr ?? 0),
      0,
    );
    const lifetimePersonalCollectionInr = (personalRows ?? []).reduce(
      (sum, row) => sum + Number(row.final_payable_inr ?? 0),
      0,
    );
    /** Cash / UPI the captain kept directly (COD + withdrawn). */
    const inOwnHands = cod + withdrawn;

    const savedVpa =
      p.captain_wallet_upi_id && isPlausibleUpiVpa(normalizeUpiVpa(p.captain_wallet_upi_id))
        ? normalizeUpiVpa(p.captain_wallet_upi_id)
        : null;

    return res.json({
      wallet_balance_inr: inOshu,
      lifetime_online_credited_inr: onlineCredited,
      lifetime_withdrawn_inr: withdrawn,
      lifetime_cod_inr: cod,
      /** INR platform share owed to Oshu (own-UPI collection path). */
      oshu_platform_due_inr: oshuPlatformDue,
      /** Lifetime total where customer paid Oshu company QR. */
      lifetime_oshu_qr_collected_inr: lifetimeOshuQrCollectedInr,
      /** Lifetime total where customer paid captain directly (personal UPI / cash). */
      lifetime_personal_collection_inr: lifetimePersonalCollectionInr,
      /** Max pending due before new rides are blocked (front-end shows a live meter). */
      oshu_platform_due_cap_inr: CAPTAIN_PLATFORM_DUE_CAP_INR,
      oshu_platform_fee_pct: platformFeePct(),
      /** Porter-style: money not held by Oshu (cash in pocket + already sent to self). */
      in_own_account_inr: inOwnHands,
      upi_saved: Boolean(savedVpa),
      /** Same VPA the captain saved (only returned to the signed-in captain). */
      saved_upi_vpa: savedVpa,
      min_withdraw_inr: minWithdrawInr(),
      razorpay_configured: Boolean(process.env.RAZORPAY_KEY_ID && process.env.RAZORPAY_KEY_SECRET),
      payouts_configured: Boolean(process.env.RAZORPAY_PAYOUT_ACCOUNT_NUMBER),
    });
  });

  /** GET /api/captain-wallet/ledger?page=&limit= */
  app.get('/api/captain-wallet/ledger', async (req, res) => {
    const uid = await requireUser(supabase, getUserIdFromAccessToken, req, res);
    if (!uid) {
      return;
    }
    const { data: p } = await supabase.from('profiles').select('role').eq('id', uid).maybeSingle();
    if (!p || p.role !== 'captain') {
      return res.status(403).json({ error: 'Captains only' });
    }
    const page = Math.max(0, Number(req.query.page ?? 0));
    const limit = Math.min(50, Math.max(1, Number(req.query.limit ?? 20)));
    const from = page * limit;

    const { data, error, count } = await supabase
      .from('captain_wallet_ledger')
      .select('*', { count: 'exact' })
      .eq('captain_id', uid)
      .order('created_at', { ascending: false })
      .range(from, from + limit - 1);
    if (error) {
      return res.status(500).json({ error: error.message });
    }
    return res.json({ entries: data ?? [], total: count ?? 0, page, limit });
  });

  /**
   * Captain: record UPI payment to Oshu against pending platform due (honor-based; reduces captain_oshu_platform_due_inr).
   */
  app.post('/api/captain-wallet/oshu-due-payment', async (req, res) => {
    const uid = await requireUser(supabase, getUserIdFromAccessToken, req, res);
    if (!uid) {
      return;
    }
    const { data: p } = await supabase.from('profiles').select('role').eq('id', uid).maybeSingle();
    if (!p || p.role !== 'captain') {
      return res.status(403).json({ error: 'Captains only' });
    }
    const amt = Number(req.body?.amount_inr);
    if (!Number.isFinite(amt) || amt <= 0) {
      return res.status(400).json({ error: 'Invalid amount_inr' });
    }
    const { data: rpcRaw, error: rpcErr } = await supabase.rpc('apply_captain_oshu_due_payment', {
      p_captain_id: uid,
      p_amount_inr: amt,
    });
    if (rpcErr) {
      return res.status(500).json({ error: rpcErr.message });
    }
    const result = rpcRaw && typeof rpcRaw === 'object' ? rpcRaw : {};
    if (result.ok !== true) {
      return res.status(400).json({ error: result.error ?? 'Could not apply payment' });
    }
    return res.json({
      ok: true,
      paid_inr: result.paid_inr ?? 0,
      remaining_due_inr: result.remaining_due_inr ?? 0,
    });
  });

  /** PATCH /api/captain-wallet/upi  body: { vpa } */
  app.patch('/api/captain-wallet/upi', async (req, res) => {
    const uid = await requireUser(supabase, getUserIdFromAccessToken, req, res);
    if (!uid) {
      return;
    }
    const { data: p } = await supabase.from('profiles').select('role').eq('id', uid).maybeSingle();
    if (!p || p.role !== 'captain') {
      return res.status(403).json({ error: 'Captains only' });
    }
    const vpa = normalizeUpiVpa(req.body?.vpa);
    if (!isPlausibleUpiVpa(vpa)) {
      return res.status(400).json({ error: 'Enter a valid UPI ID (e.g. name@paytm)' });
    }
    const { error } = await supabase
      .from('profiles')
      .update({
        captain_wallet_upi_id: vpa,
        razorpay_contact_id: null,
        razorpay_fund_account_id: null,
      })
      .eq('id', uid);
    if (error) {
      return res.status(500).json({ error: error.message });
    }
    return res.json({ ok: true, vpa });
  });

  /** POST /api/captain-wallet/withdraw  body: { amount_inr } */
  app.post('/api/captain-wallet/withdraw', async (req, res) => {
    const uid = await requireUser(supabase, getUserIdFromAccessToken, req, res);
    if (!uid) {
      return;
    }
    const { data: profile, error: pErr } = await supabase
      .from('profiles')
      .select(
        'id, role, full_name, phone, captain_wallet_balance_inr, captain_wallet_upi_id, razorpay_contact_id, razorpay_fund_account_id',
      )
      .eq('id', uid)
      .maybeSingle();
    if (pErr || !profile) {
      return res.status(500).json({ error: pErr?.message ?? 'Profile not found' });
    }
    if (profile.role !== 'captain') {
      return res.status(403).json({ error: 'Captains only' });
    }

    const minW = minWithdrawInr();
    const amt = Number(req.body?.amount_inr);
    if (!Number.isFinite(amt) || amt < minW) {
      return res.status(400).json({ error: `Minimum withdrawal is ₹${minW}` });
    }
    const bal = Number(profile.captain_wallet_balance_inr ?? 0);
    if (amt > bal) {
      return res.status(400).json({ error: 'Insufficient wallet balance' });
    }
    if (!isPlausibleUpiVpa(profile.captain_wallet_upi_id)) {
      return res.status(400).json({ error: 'Save your UPI ID in the app before withdrawing' });
    }

    const payoutAccount = process.env.RAZORPAY_PAYOUT_ACCOUNT_NUMBER;
    if (!payoutAccount) {
      return res.status(503).json({
        error:
          'UPI payouts are not configured yet (set RAZORPAY_PAYOUT_ACCOUNT_NUMBER from RazorpayX). Your balance is safe in Oshu.',
      });
    }

    const ensured = await ensureContactAndFundAccount(supabase, profile);
    if (ensured.error) {
      return res.status(502).json({ error: `Could not set up payout: ${ensured.error}` });
    }

    const { data: resv, error: rErr } = await supabase.rpc('reserve_captain_withdrawal', {
      p_captain_id: uid,
      p_amount_inr: amt,
    });
    if (rErr) {
      return res.status(500).json({ error: rErr.message });
    }
    const row = resv && typeof resv === 'object' ? resv : {};
    if (row.ok !== true) {
      return res.status(400).json({ error: row.error ?? 'Could not reserve withdrawal' });
    }
    const ledgerId = row.ledger_id;
    if (!ledgerId) {
      return res.status(500).json({ error: 'No ledger id' });
    }

    const paise = Math.round(amt * 100);
    const payoutBody = {
      account_number: payoutAccount,
      fund_account_id: ensured.fundId,
      amount: paise,
      currency: 'INR',
      mode: 'UPI',
      purpose: 'payout',
      queue_if_low_balance: true,
      reference_id: `oshu_wd_${String(ledgerId).replace(/-/g, '').slice(0, 20)}`,
      narration: 'Oshu captain wallet',
    };

    const pay = await razorpayPost('/v1/payouts', payoutBody);
    if (!pay.ok) {
      await supabase.rpc('release_captain_withdrawal_reserve', {
        p_ledger_id: ledgerId,
        p_captain_id: uid,
      });
      return res.status(502).json({ error: pay.error ?? 'Payout failed' });
    }

    const payoutId = pay.data?.id;
    if (!payoutId) {
      await supabase.rpc('release_captain_withdrawal_reserve', {
        p_ledger_id: ledgerId,
        p_captain_id: uid,
      });
      return res.status(502).json({ error: 'Payout response missing id' });
    }

    const { data: conf, error: cErr } = await supabase.rpc('confirm_captain_withdrawal', {
      p_ledger_id: ledgerId,
      p_captain_id: uid,
      p_payout_id: payoutId,
    });
    if (cErr) {
      console.error('[captain-wallet] confirm failed after payout — manual reconcile', cErr, payoutId);
      return res.status(500).json({ error: 'Payout sent but confirmation failed — support will reconcile' });
    }
    const crow = conf && typeof conf === 'object' ? conf : {};
    if (crow.ok !== true) {
      return res.status(500).json({ error: crow.error ?? 'Confirm failed' });
    }

    return res.json({ ok: true, razorpay_payout_id: payoutId, amount_inr: amt });
  });
}
