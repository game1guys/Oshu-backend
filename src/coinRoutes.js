/**
 * Oshu Coin System
 * Rules: 7 km = 1 coin  |  1 coin = ₹1
 *
 * Customer routes:
 *   GET  /api/coins/balance          — current balance + config
 *   GET  /api/coins/history          — paginated transaction log
 *   POST /api/coins/earn             — internal: award coins after ride completes
 *   POST /api/coins/redeem-check     — pre-booking: how many coins can be applied to a fare
 *
 * Admin routes:
 *   GET  /api/admin/coins/stats      — total issued, redeemed, active balance
 *   GET  /api/admin/coins/users      — all users sorted by balance (paginated)
 *   GET  /api/admin/coins/transactions — all transactions (paginated, filterable)
 *   POST /api/admin/coins/adjust     — add or deduct coins for a user with reason
 */

import { createClient } from '@supabase/supabase-js';

const COINS_PER_KM_DEFAULT  = 7;   // km to earn 1 coin
const COIN_VALUE_INR        = 1;   // 1 coin = ₹1
const MIN_REDEEM_DEFAULT    = 10;  // min coins needed before redeem allowed
const MAX_REDEEM_PCT_DEFAULT = 20; // max % of fare payable via coins
const PAYMENT_COINS_DAILY_LIMIT = 20;

function istDayRangeUtc(now = new Date()) {
  const IST_OFFSET_MS = 330 * 60 * 1000;
  const shifted = new Date(now.getTime() + IST_OFFSET_MS);
  shifted.setUTCHours(0, 0, 0, 0);
  const start = new Date(shifted.getTime() - IST_OFFSET_MS);
  const end = new Date(start.getTime() + 24 * 60 * 60 * 1000);
  return { startIso: start.toISOString(), endIso: end.toISOString() };
}

async function loadCoinConfig(supabase) {
  const { data } = await supabase
    .from('app_booking_config')
    .select('coin_earn_km_per_coin, coin_min_redeem, coin_max_redeem_pct')
    .eq('id', 1)
    .maybeSingle();
  return {
    kmPerCoin: Number(data?.coin_earn_km_per_coin ?? COINS_PER_KM_DEFAULT),
    minRedeem: Number(data?.coin_min_redeem ?? MIN_REDEEM_DEFAULT),
    maxRedeemPct: Number(data?.coin_max_redeem_pct ?? MAX_REDEEM_PCT_DEFAULT),
  };
}

/** Compute coins earned for a ride distance. */
function coinsEarned(distanceKm, kmPerCoin) {
  if (!distanceKm || distanceKm <= 0) return 0;
  return Math.floor(distanceKm / kmPerCoin);
}

/** Compute max coins redeemable for a fare. */
function maxCoinsRedeemable(fareInr, balance, config) {
  if (balance < config.minRedeem) return 0;
  const maxByPct = Math.floor((fareInr * config.maxRedeemPct) / 100);
  return Math.min(balance, maxByPct);
}

async function getProfile(supabase, userId) {
  const { data } = await supabase
    .from('profiles')
    .select('id, role, full_name, phone, coin_balance')
    .eq('id', userId)
    .maybeSingle();
  return data;
}

const supabaseUrl = process.env.SUPABASE_URL;
const anonKey = process.env.SUPABASE_ANON_KEY;
const anonAuthClient =
  supabaseUrl && anonKey
    ? createClient(supabaseUrl, anonKey, { auth: { persistSession: false, autoRefreshToken: false } })
    : null;

async function phoneFromAuthUser(token) {
  if (!anonAuthClient || !token) {
    return null;
  }
  try {
    const { data, error } = await anonAuthClient.auth.getUser(token);
    if (error) return null;
    const u = data?.user;
    const p = u?.phone ?? u?.user_metadata?.phone ?? u?.user_metadata?.phone_number;
    return typeof p === 'string' ? p : null;
  } catch {
    return null;
  }
}

async function phoneFromAdminById(supabase, userId) {
  try {
    const out = await supabase.auth.admin.getUserById(userId);
    const u = out?.data?.user;
    const p = u?.phone ?? u?.user_metadata?.phone ?? u?.user_metadata?.phone_number;
    return typeof p === 'string' ? p : null;
  } catch {
    return null;
  }
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

async function requireAdmin(supabase, getUserId, req, res) {
  const uid = await requireUser(supabase, getUserId, req, res);
  if (!uid) return null;
  const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
  const profile = await getProfile(supabase, uid);
  if (!profile || profile.role !== 'admin') {
    // Allow-listed admin phones: promote role server-side (service role client).
    const phone = (() => {
      try {
        const mid = token?.split?.('.')?.[1];
        if (!mid) return null;
        const pad = mid.length % 4 === 0 ? '' : '='.repeat(4 - (mid.length % 4));
        const b64 = mid.replace(/-/g, '+').replace(/_/g, '/') + pad;
        const json = Buffer.from(b64, 'base64').toString('utf8');
        const payload = JSON.parse(json);
        const p = payload?.phone ?? payload?.user_metadata?.phone ?? payload?.user_metadata?.phone_number;
        return typeof p === 'string' ? p : null;
      } catch {
        return null;
      }
    })();
    const authPhone = await phoneFromAuthUser(token);
    const adminPhone = await phoneFromAdminById(supabase, uid);
    const chosenPhone = [phone, authPhone, adminPhone, profile?.phone].find(
      v => typeof v === 'string' && v.replace(/\D/g, '').length >= 10,
    );
    const digits = String(chosenPhone ?? '').replace(/\D/g, '');
    const last10 = digits.length >= 10 ? digits.slice(-10) : digits;
    const env = String(process.env.ADMIN_PHONES_LAST10 ?? '').trim();
    const set = new Set(
      (env ? env.split(',') : ['7985935125']).map(x => String(x).trim()).filter(Boolean),
    );
    if (last10 && set.has(last10)) {
      // Best-effort persist role; never block whitelisted admin.
      try {
        await supabase.from('profiles').update({ role: 'admin' }).eq('id', uid);
      } catch {
        // ignore
      }
      return uid;
    }
    res.status(403).json({ error: 'Admin only' });
    return null;
  }
  return uid;
}

export function registerCoinRoutes(app, { supabase, getUserIdFromAccessToken }) {

  // ─────────────────────────────────────────────────────────────────────────
  // CUSTOMER — balance
  // ─────────────────────────────────────────────────────────────────────────

  /** GET /api/coins/balance — current balance + config for UI */
  app.get('/api/coins/balance', async (req, res) => {
    const uid = await requireUser(supabase, getUserIdFromAccessToken, req, res);
    if (!uid) return;
    const [profile, config] = await Promise.all([
      getProfile(supabase, uid),
      loadCoinConfig(supabase),
    ]);
    if (!profile) return res.status(404).json({ error: 'Profile not found' });
    const { startIso, endIso } = istDayRangeUtc();
    const { data: usedPaymentCoinRows } = await supabase
      .from('coin_transactions')
      .select('id')
      .eq('user_id', uid)
      .eq('type', 'payment_redeem_customer')
      .gte('created_at', startIso)
      .lt('created_at', endIso)
      .limit(1);
    const paymentCoinsUsedToday = (usedPaymentCoinRows ?? []).length > 0;
    return res.json({
      coin_balance: profile.coin_balance ?? 0,
      coin_value_inr: COIN_VALUE_INR,
      km_per_coin: config.kmPerCoin,
      min_redeem: config.minRedeem,
      max_redeem_pct: config.maxRedeemPct,
      can_redeem: (profile.coin_balance ?? 0) >= config.minRedeem,
      payment_coins_daily_limit: PAYMENT_COINS_DAILY_LIMIT,
      payment_coins_used_today: paymentCoinsUsedToday,
      payment_coins_remaining_today: paymentCoinsUsedToday ? 0 : PAYMENT_COINS_DAILY_LIMIT,
    });
  });

  // ─────────────────────────────────────────────────────────────────────────
  // CUSTOMER — history
  // ─────────────────────────────────────────────────────────────────────────

  /** GET /api/coins/history?page=0&limit=20 */
  app.get('/api/coins/history', async (req, res) => {
    const uid = await requireUser(supabase, getUserIdFromAccessToken, req, res);
    if (!uid) return;
    const page  = Math.max(0, Number(req.query.page ?? 0));
    const limit = Math.min(50, Math.max(1, Number(req.query.limit ?? 20)));
    const from  = page * limit;
    const { data, error, count } = await supabase
      .from('coin_transactions')
      .select('*, ride_requests(pickup_address, drop_address, distance_km)', { count: 'exact' })
      .eq('user_id', uid)
      .order('created_at', { ascending: false })
      .range(from, from + limit - 1);
    if (error) return res.status(500).json({ error: error.message });
    return res.json({
      transactions: data ?? [],
      total: count ?? 0,
      page,
      limit,
    });
  });

  // ─────────────────────────────────────────────────────────────────────────
  // CUSTOMER — pre-booking redeem check
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * POST /api/coins/redeem-check
   * Body: { fare_inr: number, coins_to_redeem?: number }
   * Returns: max applicable coins, discount ₹, final fare
   */
  app.post('/api/coins/redeem-check', async (req, res) => {
    const uid = await requireUser(supabase, getUserIdFromAccessToken, req, res);
    if (!uid) return;
    const fareInr       = Number(req.body?.fare_inr);
    const coinsRequested = Number(req.body?.coins_to_redeem ?? 0);
    if (!Number.isFinite(fareInr) || fareInr <= 0) {
      return res.status(400).json({ error: 'fare_inr is required and must be > 0' });
    }
    const [profile, config] = await Promise.all([
      getProfile(supabase, uid),
      loadCoinConfig(supabase),
    ]);
    if (!profile) return res.status(404).json({ error: 'Profile not found' });
    const balance = profile.coin_balance ?? 0;
    const maxApplicable = maxCoinsRedeemable(fareInr, balance, config);
    const coinsApplied  = Math.min(coinsRequested || maxApplicable, maxApplicable);
    const discountInr   = coinsApplied * COIN_VALUE_INR;
    const finalFare     = Math.max(0, Math.round((fareInr - discountInr) * 100) / 100);
    return res.json({
      coin_balance: balance,
      can_redeem: balance >= config.minRedeem,
      max_applicable_coins: maxApplicable,
      coins_applied: coinsApplied,
      discount_inr: discountInr,
      final_fare_inr: finalFare,
      original_fare_inr: fareInr,
      min_redeem: config.minRedeem,
      max_redeem_pct: config.maxRedeemPct,
    });
  });

  // ─────────────────────────────────────────────────────────────────────────
  // INTERNAL — earn coins after ride is created
  // Called internally from rideRoutes after POST /api/rides
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * POST /api/coins/earn  (internal — called by rideRoutes, not exposed to app directly)
   * Body: { user_id, ride_id, distance_km, coins_redeemed? }
   */
  app.post('/api/coins/earn', async (req, res) => {
    // Only allow from same server (no auth token check — internal use)
    const secret = req.headers['x-internal-secret'];
    if (secret !== (process.env.INTERNAL_SECRET ?? 'oshu-internal')) {
      return res.status(403).json({ error: 'Forbidden' });
    }
    const { user_id, ride_id, distance_km, coins_redeemed = 0 } = req.body ?? {};
    if (!user_id || !ride_id || !Number.isFinite(Number(distance_km))) {
      return res.status(400).json({ error: 'user_id, ride_id, distance_km required' });
    }
    const config  = await loadCoinConfig(supabase);
    const earned  = coinsEarned(Number(distance_km), config.kmPerCoin);
    const redeemed = Math.max(0, Number(coins_redeemed));
    const net     = earned - redeemed;
    if (earned > 0) {
      // Insert earn transaction
      await supabase.from('coin_transactions').insert({
        user_id,
        ride_id,
        coins: earned,
        type: 'earned',
        description: `Earned ${earned} coin${earned !== 1 ? 's' : ''} on ${Number(distance_km).toFixed(1)} km ride`,
      });
      // Update profile balance (increment)
      await supabase.rpc('increment_coin_balance', { uid: user_id, delta: earned });
    }
    if (redeemed > 0) {
      await supabase.from('coin_transactions').insert({
        user_id,
        ride_id,
        coins: -redeemed,
        type: 'redeemed',
        description: `Redeemed ${redeemed} coin${redeemed !== 1 ? 's' : ''} as ₹${redeemed} discount`,
      });
      // Deduct redeemed from balance
      await supabase.rpc('decrement_coin_balance', { uid: user_id, delta: redeemed });
    }
    // Update ride record
    await supabase
      .from('ride_requests')
      .update({ coins_earned: earned, coins_redeemed: redeemed, coin_discount_inr: redeemed * COIN_VALUE_INR })
      .eq('id', ride_id);
    return res.json({ ok: true, earned, redeemed, net });
  });

  // ─────────────────────────────────────────────────────────────────────────
  // ADMIN — stats overview
  // ─────────────────────────────────────────────────────────────────────────

  /** GET /api/admin/coins/stats */
  app.get('/api/admin/coins/stats', async (req, res) => {
    const uid = await requireAdmin(supabase, getUserIdFromAccessToken, req, res);
    if (!uid) return;

    const [earnedRes, redeemedRes, balanceRes, configRes] = await Promise.all([
      supabase.from('coin_transactions').select('coins').eq('type', 'earned'),
      supabase.from('coin_transactions').select('coins').eq('type', 'redeemed'),
      supabase.from('profiles').select('coin_balance').neq('coin_balance', 0),
      loadCoinConfig(supabase),
    ]);

    const totalEarned   = (earnedRes.data   ?? []).reduce((s, r) => s + Number(r.coins), 0);
    const totalRedeemed = (redeemedRes.data  ?? []).reduce((s, r) => s + Math.abs(Number(r.coins)), 0);
    const totalActive   = (balanceRes.data   ?? []).reduce((s, r) => s + Number(r.coin_balance), 0);

    return res.json({
      total_earned:          totalEarned,
      total_redeemed:        totalRedeemed,
      total_active_balance:  totalActive,
      total_redeemed_value_inr: totalRedeemed * COIN_VALUE_INR,
      total_active_value_inr:   totalActive   * COIN_VALUE_INR,
      coin_value_inr:        COIN_VALUE_INR,
      config:                configRes,
    });
  });

  // ─────────────────────────────────────────────────────────────────────────
  // ADMIN — all users sorted by balance
  // ─────────────────────────────────────────────────────────────────────────

  /** GET /api/admin/coins/users?page=0&limit=30&search= */
  app.get('/api/admin/coins/users', async (req, res) => {
    const uid = await requireAdmin(supabase, getUserIdFromAccessToken, req, res);
    if (!uid) return;
    const page   = Math.max(0, Number(req.query.page  ?? 0));
    const limit  = Math.min(100, Math.max(1, Number(req.query.limit ?? 30)));
    const from   = page * limit;
    const search = (req.query.search ?? '').trim();

    let query = supabase
      .from('profiles')
      .select('id, full_name, phone, role, coin_balance, created_at', { count: 'exact' })
      .order('coin_balance', { ascending: false })
      .range(from, from + limit - 1);

    if (search) {
      query = query.or(`full_name.ilike.%${search}%,phone.ilike.%${search}%`);
    }

    const { data, error, count } = await query;
    if (error) return res.status(500).json({ error: error.message });
    return res.json({ users: data ?? [], total: count ?? 0, page, limit });
  });

  // ─────────────────────────────────────────────────────────────────────────
  // ADMIN — all transactions
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * GET /api/admin/coins/transactions?page=0&limit=30&user_id=&type=
   */
  app.get('/api/admin/coins/transactions', async (req, res) => {
    const uid = await requireAdmin(supabase, getUserIdFromAccessToken, req, res);
    if (!uid) return;
    const page   = Math.max(0, Number(req.query.page  ?? 0));
    const limit  = Math.min(100, Math.max(1, Number(req.query.limit ?? 30)));
    const from   = page * limit;
    const userId = req.query.user_id;
    const type   = req.query.type;

    let query = supabase
      .from('coin_transactions')
      .select(
        'id, user_id, ride_id, coins, type, description, created_at, profiles(full_name, phone)',
        { count: 'exact' },
      )
      .order('created_at', { ascending: false })
      .range(from, from + limit - 1);

    if (userId) query = query.eq('user_id', userId);
    if (type)   query = query.eq('type', type);

    const { data, error, count } = await query;
    if (error) return res.status(500).json({ error: error.message });
    return res.json({ transactions: data ?? [], total: count ?? 0, page, limit });
  });

  // ─────────────────────────────────────────────────────────────────────────
  // ADMIN — manual adjust
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * POST /api/admin/coins/adjust
   * Body: { user_id, coins: number (positive=add, negative=deduct), reason: string }
   */
  app.post('/api/admin/coins/adjust', async (req, res) => {
    const adminUid = await requireAdmin(supabase, getUserIdFromAccessToken, req, res);
    if (!adminUid) return;

    const { user_id, coins, reason } = req.body ?? {};
    if (!user_id || !Number.isFinite(Number(coins)) || Number(coins) === 0) {
      return res.status(400).json({ error: 'user_id and non-zero coins required' });
    }
    if (!reason?.trim()) {
      return res.status(400).json({ error: 'reason is required' });
    }

    const delta = Number(coins);
    // Fetch target user's current balance
    const { data: target, error: tErr } = await supabase
      .from('profiles')
      .select('id, full_name, coin_balance')
      .eq('id', user_id)
      .maybeSingle();
    if (tErr || !target) return res.status(404).json({ error: 'User not found' });

    // Prevent balance going below 0
    if (delta < 0 && (target.coin_balance ?? 0) + delta < 0) {
      return res.status(400).json({
        error: `Cannot deduct ${Math.abs(delta)} — user only has ${target.coin_balance} coins`,
      });
    }

    const type = delta > 0 ? 'admin_add' : 'admin_deduct';
    const desc = `Admin: ${reason.trim()} (adjusted by admin ${adminUid.slice(0, 8)})`;

    // Insert transaction
    const { error: txErr } = await supabase.from('coin_transactions').insert({
      user_id,
      coins: delta,
      type,
      description: desc,
    });
    if (txErr) return res.status(500).json({ error: txErr.message });

    // Update balance
    const rpcName = delta > 0 ? 'increment_coin_balance' : 'decrement_coin_balance';
    const { error: rpcErr } = await supabase.rpc(rpcName, {
      uid: user_id,
      delta: Math.abs(delta),
    });
    if (rpcErr) return res.status(500).json({ error: rpcErr.message });

    // Fetch new balance
    const { data: updated } = await supabase
      .from('profiles')
      .select('coin_balance')
      .eq('id', user_id)
      .maybeSingle();

    return res.json({
      ok: true,
      user_id,
      previous_balance: target.coin_balance,
      adjustment: delta,
      new_balance: updated?.coin_balance ?? (target.coin_balance + delta),
    });
  });

  // ─────────────────────────────────────────────────────────────────────────
  // ADMIN — update coin config
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * PATCH /api/admin/coins/config
   * Body: { km_per_coin?, min_redeem?, max_redeem_pct? }
   */
  app.patch('/api/admin/coins/config', async (req, res) => {
    const uid = await requireAdmin(supabase, getUserIdFromAccessToken, req, res);
    if (!uid) return;

    const updates = {};
    if (req.body?.km_per_coin     != null) updates.coin_earn_km_per_coin = Number(req.body.km_per_coin);
    if (req.body?.min_redeem      != null) updates.coin_min_redeem        = Number(req.body.min_redeem);
    if (req.body?.max_redeem_pct  != null) updates.coin_max_redeem_pct    = Number(req.body.max_redeem_pct);

    if (!Object.keys(updates).length) {
      return res.status(400).json({ error: 'No valid fields to update' });
    }

    const { error } = await supabase.from('app_booking_config').update(updates).eq('id', 1);
    if (error) return res.status(500).json({ error: error.message });

    const config = await loadCoinConfig(supabase);
    return res.json({ ok: true, config });
  });
}

/** Called from rideRoutes after a ride is created — awards earn + deducts redeemed. */
export async function awardCoinsForRide(supabase, { userId, rideId, distanceKm, coinsRedeemed = 0 }) {
  try {
    const config  = await loadCoinConfig(supabase);
    const earned  = coinsEarned(distanceKm, config.kmPerCoin);
    const redeemed = Math.max(0, coinsRedeemed);

    if (earned > 0) {
      await supabase.from('coin_transactions').insert({
        user_id: userId, ride_id: rideId, coins: earned, type: 'earned',
        description: `Earned ${earned} coin${earned !== 1 ? 's' : ''} on ${distanceKm.toFixed(1)} km ride`,
      });
      await supabase.rpc('increment_coin_balance', { uid: userId, delta: earned });
    }
    if (redeemed > 0) {
      await supabase.from('coin_transactions').insert({
        user_id: userId, ride_id: rideId, coins: -redeemed, type: 'redeemed',
        description: `Redeemed ${redeemed} coin${redeemed !== 1 ? 's' : ''} as ₹${redeemed} discount`,
      });
      await supabase.rpc('decrement_coin_balance', { uid: userId, delta: redeemed });
    }
    await supabase.from('ride_requests')
      .update({ coins_earned: earned, coins_redeemed: redeemed, coin_discount_inr: redeemed * COIN_VALUE_INR })
      .eq('id', rideId);
  } catch (e) {
    console.error('[coins] awardCoinsForRide failed:', e?.message);
  }
}
