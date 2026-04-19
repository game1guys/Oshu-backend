/**
 * Oshu Admin Routes — full access to rides, users, captains, stats.
 *
 * GET  /api/admin/stats             — dashboard counts + revenue
 * GET  /api/admin/rides             — all rides (paginated, filterable)
 * GET  /api/admin/rides/:id         — single ride full detail
 * POST /api/admin/rides/:id/cancel  — admin force-cancel any ride
 * POST /api/admin/rides/:id/complete — admin force-complete any ride
 * GET  /api/admin/users             — all users (customers + captains + admins)
 * GET  /api/admin/users/:id         — single user full detail
 * PATCH /api/admin/users/:id/role   — change user role
 * GET  /api/admin/captains          — all captains with KYC status + vehicle info
 */

import { createClient } from '@supabase/supabase-js';

async function getProfile(supabase, userId) {
  const { data } = await supabase
    .from('profiles')
    .select('id, role, phone')
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

function last10Digits(phone) {
  const d = String(phone ?? '').replace(/\D/g, '');
  return d.length >= 10 ? d.slice(-10) : d;
}

function phoneFromAccessToken(token) {
  try {
    const mid = token.split('.')[1];
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
}

async function phoneFromAuthUser(token) {
  if (!anonAuthClient) {
    return null;
  }
  try {
    const { data, error } = await anonAuthClient.auth.getUser(token);
    if (error) {
      return null;
    }
    const u = data?.user;
    const p = u?.phone ?? u?.user_metadata?.phone ?? u?.user_metadata?.phone_number;
    if (typeof p !== 'string') return null;
    const digits = p.replace(/\D/g, '');
    return digits.length >= 10 ? p : null;
  } catch {
    return null;
  }
}

async function phoneFromAdminById(supabase, userId) {
  try {
    // Service role can read auth user reliably even when JWT lacks phone fields.
    const out = await supabase.auth.admin.getUserById(userId);
    const u = out?.data?.user;
    const p = u?.phone ?? u?.user_metadata?.phone ?? u?.user_metadata?.phone_number;
    if (typeof p !== 'string') return null;
    const digits = p.replace(/\D/g, '');
    return digits.length >= 10 ? p : null;
  } catch {
    return null;
  }
}

function pickFirstPhone(...candidates) {
  for (const c of candidates) {
    if (typeof c !== 'string') continue;
    const digits = c.replace(/\D/g, '');
    if (digits.length >= 10) return c;
  }
  return null;
}

function isWhitelistedAdminPhone(phone) {
  const env = String(process.env.ADMIN_PHONES_LAST10 ?? '').trim();
  const set = new Set(
    (env ? env.split(',') : ['7985935125']).map(x => String(x).trim()).filter(Boolean),
  );
  const last10 = last10Digits(phone);
  return Boolean(last10) && set.has(last10);
}

async function requireAdmin(supabase, getUserId, req, res) {
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
  const profile = await getProfile(supabase, uid);
  if (!profile || profile.role !== 'admin') {
    // Allow-listed admin phones: promote server-side (service role) so all admin APIs work.
    const phone = pickFirstPhone(
      phoneFromAccessToken(token),
      await phoneFromAuthUser(token),
      await phoneFromAdminById(supabase, uid),
      profile?.phone,
    );
    if (phone && isWhitelistedAdminPhone(phone)) {
      // Best-effort: persist admin role, but never block access if phone is whitelisted.
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

export function registerAdminRoutes(app, { supabase, getUserIdFromAccessToken }) {

  /**
   * DEV helper: diagnose why a token is not treated as admin.
   * GET /api/admin/debug-auth
   */
  app.get('/api/admin/debug-auth', async (req, res) => {
    const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
    if (!token || !supabase) {
      return res.status(400).json({ error: 'Missing Authorization' });
    }
    const uid = await getUserIdFromAccessToken(token);
    if (!uid) {
      return res.status(401).json({ error: 'Invalid token' });
    }
    const profile = await getProfile(supabase, uid);
    const jwtPhone = phoneFromAccessToken(token);
    const authPhone = await phoneFromAuthUser(token);
    const adminPhone = await phoneFromAdminById(supabase, uid);
    const chosen = pickFirstPhone(jwtPhone, authPhone, adminPhone, profile?.phone);
    const last10 = last10Digits(chosen);
    const whitelisted = chosen ? isWhitelistedAdminPhone(chosen) : false;
    return res.json({
      uid,
      profile_role: profile?.role ?? null,
      profile_phone: profile?.phone ?? null,
      jwt_phone: jwtPhone,
      auth_phone: authPhone,
      admin_phone: adminPhone,
      chosen_phone: chosen,
      chosen_last10: last10,
      whitelisted,
      env_ADMIN_PHONES_LAST10: String(process.env.ADMIN_PHONES_LAST10 ?? ''),
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  // STATS — dashboard overview
  // ──────────────────────────────────────────────────────────────────────────

  /**
   * GET /api/admin/stats
   * Returns: rides today/total, revenue, captains, customers, pending rides, active rides
   */
  app.get('/api/admin/stats', async (req, res) => {
    const uid = await requireAdmin(supabase, getUserIdFromAccessToken, req, res);
    if (!uid) return;

    const todayStart = new Date();
    todayStart.setHours(0, 0, 0, 0);

    const [
      ridesAll,
      ridesToday,
      pendingRides,
      activeRides,
      completedRides,
      cancelledRides,
      totalCustomers,
      totalCaptains,
      pendingCaptainKyc,
      revenueAll,
      revenueToday,
    ] = await Promise.all([
      supabase.from('ride_requests').select('id', { count: 'exact', head: true }),
      supabase.from('ride_requests').select('id', { count: 'exact', head: true }).gte('created_at', todayStart.toISOString()),
      supabase.from('ride_requests').select('id', { count: 'exact', head: true }).eq('status', 'pending'),
      supabase.from('ride_requests').select('id', { count: 'exact', head: true }).in('status', ['accepted', 'in_progress']),
      supabase.from('ride_requests').select('id', { count: 'exact', head: true }).eq('status', 'completed'),
      supabase.from('ride_requests').select('id', { count: 'exact', head: true }).eq('status', 'cancelled'),
      supabase.from('profiles').select('id', { count: 'exact', head: true }).eq('role', 'user'),
      supabase.from('profiles').select('id', { count: 'exact', head: true }).eq('role', 'captain'),
      supabase
        .from('profiles')
        .select('id', { count: 'exact', head: true })
        .eq('role', 'captain')
        .in('captain_kyc_status', ['submitted', 'under_review']),
      supabase.from('ride_requests').select('quoted_price_inr').eq('status', 'completed'),
      supabase.from('ride_requests').select('quoted_price_inr').eq('status', 'completed').gte('created_at', todayStart.toISOString()),
    ]);

    const totalRevenueInr = (revenueAll.data ?? []).reduce((s, r) => s + Number(r.quoted_price_inr ?? 0), 0);
    const todayRevenueInr = (revenueToday.data ?? []).reduce((s, r) => s + Number(r.quoted_price_inr ?? 0), 0);

    return res.json({
      rides: {
        total:     ridesAll.count ?? 0,
        today:     ridesToday.count ?? 0,
        pending:   pendingRides.count ?? 0,
        active:    activeRides.count ?? 0,
        completed: completedRides.count ?? 0,
        cancelled: cancelledRides.count ?? 0,
      },
      users: {
        customers: totalCustomers.count ?? 0,
        captains:  totalCaptains.count ?? 0,
      },
      kyc: {
        pending_captains: pendingCaptainKyc.count ?? 0,
      },
      revenue: {
        total_inr: Math.round(totalRevenueInr * 100) / 100,
        today_inr: Math.round(todayRevenueInr * 100) / 100,
      },
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  // RIDES — all rides
  // ──────────────────────────────────────────────────────────────────────────

  /**
   * GET /api/admin/rides?page=0&limit=30&status=&date=YYYY-MM-DD&search=
   */
  app.get('/api/admin/rides', async (req, res) => {
    const uid = await requireAdmin(supabase, getUserIdFromAccessToken, req, res);
    if (!uid) return;

    const page   = Math.max(0, Number(req.query.page  ?? 0));
    const limit  = Math.min(100, Math.max(1, Number(req.query.limit ?? 30)));
    const from   = page * limit;
    const status = req.query.status;
    const date   = req.query.date;   // YYYY-MM-DD
    const search = (req.query.search ?? '').trim();

    let query = supabase
      .from('ride_requests')
      .select(
        `id, status, vehicle_type, pickup_address, drop_address,
         distance_km, base_fare_inr, quoted_price_inr, coin_discount_inr,
         overtime_charge_inr, toll_inr, overweight_charge_inr, cargo_overweight_kg, final_payable_inr,
         coins_earned, coins_redeemed, created_at,
         customer_id, captain_id`,
        { count: 'exact' },
      )
      .order('created_at', { ascending: false })
      .range(from, from + limit - 1);

    if (status) query = query.eq('status', status);
    if (date) {
      const d = new Date(date);
      const next = new Date(d); next.setDate(next.getDate() + 1);
      query = query.gte('created_at', d.toISOString()).lt('created_at', next.toISOString());
    }
    // search by pickup/drop address substring
    if (search) {
      query = query.or(`pickup_address.ilike.%${search}%,drop_address.ilike.%${search}%`);
    }

    const { data, error, count } = await query;
    if (error) return res.status(500).json({ error: error.message });
    const ridesRaw = data ?? [];
    const ids = new Set();
    for (const r of ridesRaw) {
      if (r.customer_id) ids.add(r.customer_id);
      if (r.captain_id) ids.add(r.captain_id);
    }
    let peopleById = {};
    if (ids.size) {
      const { data: people } = await supabase
        .from('profiles')
        .select('id, full_name, phone')
        .in('id', Array.from(ids));
      for (const p of people ?? []) {
        peopleById[p.id] = p;
      }
    }
    const rides = ridesRaw.map(r => ({
      ...r,
      customer: r.customer_id ? peopleById[r.customer_id] ?? null : null,
      captain: r.captain_id ? peopleById[r.captain_id] ?? null : null,
    }));
    return res.json({ rides, total: count ?? 0, page, limit });
  });

  /** GET /api/admin/rides/:id — full ride details */
  app.get('/api/admin/rides/:id', async (req, res) => {
    const uid = await requireAdmin(supabase, getUserIdFromAccessToken, req, res);
    if (!uid) return;
    const { data, error } = await supabase
      .from('ride_requests')
      .select(`*`)
      .eq('id', req.params.id)
      .maybeSingle();
    if (error) return res.status(500).json({ error: error.message });
    if (!data)  return res.status(404).json({ error: 'Ride not found' });
    const customerId = data.customer_id;
    const captainId = data.captain_id;
    const ids = [customerId, captainId].filter(Boolean);
    let peopleById = {};
    if (ids.length) {
      const { data: people } = await supabase.from('profiles').select('*').in('id', ids);
      for (const p of people ?? []) {
        peopleById[p.id] = p;
      }
    }
    return res.json({
      ride: {
        ...data,
        customer: customerId ? peopleById[customerId] ?? null : null,
        captain: captainId ? peopleById[captainId] ?? null : null,
      },
    });
  });

  /**
   * GET /api/admin/rides/:id/monitor
   * Returns ride + best-effort last known captain location.
   * Location source: latest active trip for the ride's driver (assigned/in_progress) if present.
   */
  app.get('/api/admin/rides/:id/monitor', async (req, res) => {
    const uid = await requireAdmin(supabase, getUserIdFromAccessToken, req, res);
    if (!uid) return;

    const { data: ride, error } = await supabase
      .from('ride_requests')
      .select(`*`)
      .eq('id', req.params.id)
      .maybeSingle();
    if (error) return res.status(500).json({ error: error.message });
    if (!ride) return res.status(404).json({ error: 'Ride not found' });

    let location = null;
    const captainId = ride.captain_id;
    if (captainId) {
      const { data: trip } = await supabase
        .from('trips')
        .select('id, current_lat, current_lng, last_location_at, status')
        .eq('driver_id', captainId)
        .in('status', ['assigned', 'in_progress'])
        .order('updated_at', { ascending: false })
        .limit(1)
        .maybeSingle();
      if (trip?.current_lat != null && trip?.current_lng != null) {
        location = {
          trip_id: trip.id,
          lat: trip.current_lat,
          lng: trip.current_lng,
          at: trip.last_location_at,
          status: trip.status,
        };
      }
    }

    const customerId = ride.customer_id;
    const ids = [customerId, captainId].filter(Boolean);
    let peopleById = {};
    if (ids.length) {
      const { data: people } = await supabase.from('profiles').select('*').in('id', ids);
      for (const p of people ?? []) {
        peopleById[p.id] = p;
      }
    }
    return res.json({
      ride: {
        ...ride,
        customer: customerId ? peopleById[customerId] ?? null : null,
        captain: captainId ? peopleById[captainId] ?? null : null,
      },
      location,
    });
  });

  /** POST /api/admin/rides/:id/cancel — admin force-cancel */
  app.post('/api/admin/rides/:id/cancel', async (req, res) => {
    const uid = await requireAdmin(supabase, getUserIdFromAccessToken, req, res);
    if (!uid) return;
    const reason = req.body?.reason ?? 'Admin cancelled';
    const { data, error } = await supabase
      .from('ride_requests')
      .update({ status: 'cancelled', cancel_reason: String(reason) })
      .eq('id', req.params.id)
      .not('status', 'in', '("completed","cancelled")')
      .select('*')
      .maybeSingle();
    if (error) return res.status(500).json({ error: error.message });
    if (!data)  return res.status(404).json({ error: 'Ride not found or already completed/cancelled' });
    return res.json({ ok: true, ride: data });
  });

  /** POST /api/admin/rides/:id/complete — admin force-complete */
  app.post('/api/admin/rides/:id/complete', async (req, res) => {
    const uid = await requireAdmin(supabase, getUserIdFromAccessToken, req, res);
    if (!uid) return;
    const { data, error } = await supabase
      .from('ride_requests')
      .update({ status: 'completed' })
      .eq('id', req.params.id)
      .not('status', 'in', '("completed","cancelled")')
      .select('*')
      .maybeSingle();
    if (error) return res.status(500).json({ error: error.message });
    if (!data)  return res.status(404).json({ error: 'Ride not found or already finalised' });
    return res.json({ ok: true, ride: data });
  });

  // ──────────────────────────────────────────────────────────────────────────
  // INSIGHTS — earnings / spend / referrals
  // ──────────────────────────────────────────────────────────────────────────

  /**
   * GET /api/admin/insights/captain-earnings?days=30
   * Returns: per-captain totals for completed rides (simple aggregation for MVP).
   */
  app.get('/api/admin/insights/captain-earnings', async (req, res) => {
    const uid = await requireAdmin(supabase, getUserIdFromAccessToken, req, res);
    if (!uid) return;

    const days = Math.min(365, Math.max(1, Number(req.query.days ?? 30)));
    const since = new Date();
    since.setDate(since.getDate() - days);

    const { data, error } = await supabase
      .from('ride_requests')
      .select('captain_id, quoted_price_inr, created_at')
      .eq('status', 'completed')
      .gte('created_at', since.toISOString());
    if (error) return res.status(500).json({ error: error.message });

    const rows = data ?? [];
    const map = new Map();
    for (const r of rows) {
      const id = r.captain_id;
      if (!id) continue;
      const cur = map.get(id) ?? { captain_id: id, rides: 0, total_inr: 0 };
      cur.rides += 1;
      cur.total_inr += Number(r.quoted_price_inr ?? 0);
      map.set(id, cur);
    }
    const list = Array.from(map.values()).sort((a, b) => b.total_inr - a.total_inr);

    const ids = list.slice(0, 500).map(x => x.captain_id);
    let profiles = [];
    if (ids.length) {
      const { data: pd } = await supabase
        .from('profiles')
        .select('id, full_name, phone, captain_oshu_id, coin_balance, captain_kyc_status')
        .in('id', ids);
      profiles = pd ?? [];
    }
    const pMap = Object.fromEntries(profiles.map(p => [p.id, p]));
    const enriched = list.slice(0, 500).map(x => ({ ...x, profile: pMap[x.captain_id] ?? null }));
    return res.json({ days, captains: enriched });
  });

  /**
   * GET /api/admin/insights/customer-spend?days=30
   * Returns: per-customer spend totals for completed rides (simple aggregation for MVP).
   */
  app.get('/api/admin/insights/customer-spend', async (req, res) => {
    const uid = await requireAdmin(supabase, getUserIdFromAccessToken, req, res);
    if (!uid) return;

    const days = Math.min(365, Math.max(1, Number(req.query.days ?? 30)));
    const since = new Date();
    since.setDate(since.getDate() - days);

    const { data, error } = await supabase
      .from('ride_requests')
      .select('customer_id, quoted_price_inr, coin_discount_inr, created_at')
      .eq('status', 'completed')
      .gte('created_at', since.toISOString());
    if (error) return res.status(500).json({ error: error.message });

    const rows = data ?? [];
    const map = new Map();
    for (const r of rows) {
      const id = r.customer_id;
      if (!id) continue;
      const cur = map.get(id) ?? { customer_id: id, rides: 0, total_inr: 0, coin_discount_inr: 0 };
      cur.rides += 1;
      cur.total_inr += Number(r.quoted_price_inr ?? 0);
      cur.coin_discount_inr += Number(r.coin_discount_inr ?? 0);
      map.set(id, cur);
    }
    const list = Array.from(map.values()).sort((a, b) => b.total_inr - a.total_inr);

    const ids = list.slice(0, 500).map(x => x.customer_id);
    let profiles = [];
    if (ids.length) {
      const { data: pd } = await supabase
        .from('profiles')
        .select('id, full_name, phone, coin_balance, customer_user_type, customer_monthly_order_range')
        .in('id', ids);
      profiles = pd ?? [];
    }
    const pMap = Object.fromEntries(profiles.map(p => [p.id, p]));
    const enriched = list.slice(0, 500).map(x => ({ ...x, profile: pMap[x.customer_id] ?? null }));
    return res.json({ days, customers: enriched });
  });

  /**
   * GET /api/admin/referrals?page=0&limit=30&role=user|captain&search=
   * Returns referral codes + relationships for both customers and captains.
   */
  app.get('/api/admin/referrals', async (req, res) => {
    const uid = await requireAdmin(supabase, getUserIdFromAccessToken, req, res);
    if (!uid) return;

    const page = Math.max(0, Number(req.query.page ?? 0));
    const limit = Math.min(100, Math.max(1, Number(req.query.limit ?? 30)));
    const from = page * limit;
    const role = req.query.role; // user|captain
    const search = (req.query.search ?? '').trim();

    let q = supabase
      .from('profiles')
      .select(
        `id, role, full_name, phone, coin_balance,
         referral_code, referred_by, referral_applied_at,
         captain_referral_code, captain_referred_by, captain_referral_applied_at,
         created_at`,
        { count: 'exact' },
      )
      .order('created_at', { ascending: false })
      .range(from, from + limit - 1);
    if (role) q = q.eq('role', role);
    if (search) q = q.or(`full_name.ilike.%${search}%,phone.ilike.%${search}%,referral_code.ilike.%${search}%`);

    const { data, error, count } = await q;
    if (error) return res.status(500).json({ error: error.message });
    return res.json({ users: data ?? [], total: count ?? 0, page, limit });
  });

  // ──────────────────────────────────────────────────────────────────────────
  // USERS — all profiles
  // ──────────────────────────────────────────────────────────────────────────

  /**
   * GET /api/admin/onboarding?page=0&limit=30&role=user|captain&status=completed|pending&search=
   * A single view for who onboarded (and who is pending).
   */
  app.get('/api/admin/onboarding', async (req, res) => {
    const uid = await requireAdmin(supabase, getUserIdFromAccessToken, req, res);
    if (!uid) return;

    const page = Math.max(0, Number(req.query.page ?? 0));
    const limit = Math.min(100, Math.max(1, Number(req.query.limit ?? 30)));
    const from = page * limit;
    const role = req.query.role; // user|captain
    const status = req.query.status; // completed|pending
    const search = (req.query.search ?? '').trim();

    let q = supabase
      .from('profiles')
      .select(
        `id, role, full_name, phone, avatar_url, created_at,
         profile_completed_at, customer_user_type, customer_monthly_order_range,
         captain_vehicle_submitted_at, captain_kyc_status, captain_oshu_id, coin_balance`,
        { count: 'exact' },
      )
      .order('created_at', { ascending: false })
      .range(from, from + limit - 1);

    if (role) q = q.eq('role', role);
    if (status === 'completed') q = q.not('profile_completed_at', 'is', null);
    if (status === 'pending') q = q.is('profile_completed_at', null);
    if (search) q = q.or(`full_name.ilike.%${search}%,phone.ilike.%${search}%`);

    const [listRes, counts] = await Promise.all([
      q,
      Promise.all([
        supabase.from('profiles').select('id', { count: 'exact', head: true }).eq('role', 'user'),
        supabase.from('profiles').select('id', { count: 'exact', head: true }).eq('role', 'captain'),
        supabase.from('profiles').select('id', { count: 'exact', head: true }).eq('role', 'user').not('profile_completed_at', 'is', null),
        supabase.from('profiles').select('id', { count: 'exact', head: true }).eq('role', 'captain').not('profile_completed_at', 'is', null),
      ]),
    ]);

    if (listRes.error) return res.status(500).json({ error: listRes.error.message });

    const [uAll, cAll, uDone, cDone] = counts;
    return res.json({
      users: listRes.data ?? [],
      total: listRes.count ?? 0,
      page,
      limit,
      summary: {
        customers_total: uAll.count ?? 0,
        captains_total: cAll.count ?? 0,
        customers_completed: uDone.count ?? 0,
        captains_completed: cDone.count ?? 0,
      },
    });
  });

  /**
   * GET /api/admin/users?page=0&limit=30&role=&search=
   */
  app.get('/api/admin/users', async (req, res) => {
    const uid = await requireAdmin(supabase, getUserIdFromAccessToken, req, res);
    if (!uid) return;

    const page   = Math.max(0, Number(req.query.page  ?? 0));
    const limit  = Math.min(100, Math.max(1, Number(req.query.limit ?? 30)));
    const from   = page * limit;
    const role   = req.query.role;
    const search = (req.query.search ?? '').trim();

    let query = supabase
      .from('profiles')
      .select(
        `id, role, full_name, phone, avatar_url, coin_balance,
         profile_completed_at, created_at,
         captain_kyc_status, captain_oshu_id,
         customer_user_type, customer_monthly_order_range`,
        { count: 'exact' },
      )
      .order('created_at', { ascending: false })
      .range(from, from + limit - 1);

    if (role)   query = query.eq('role', role);
    if (search) query = query.or(`full_name.ilike.%${search}%,phone.ilike.%${search}%`);

    const { data, error, count } = await query;
    if (error) return res.status(500).json({ error: error.message });
    return res.json({ users: data ?? [], total: count ?? 0, page, limit });
  });

  /** GET /api/admin/users/:id — full profile + ride counts */
  app.get('/api/admin/users/:id', async (req, res) => {
    const uid = await requireAdmin(supabase, getUserIdFromAccessToken, req, res);
    if (!uid) return;

    const [profileRes, ridesRes, vehicleRes] = await Promise.all([
      supabase.from('profiles').select('*').eq('id', req.params.id).maybeSingle(),
      supabase
        .from('ride_requests')
        .select('id, status, quoted_price_inr, created_at, customer_id, captain_id, pickup_address, drop_address')
        .or(`customer_id.eq.${req.params.id},captain_id.eq.${req.params.id}`)
        .order('created_at', { ascending: false })
        .limit(20),
      supabase.from('vehicles').select('*').eq('driver_id', req.params.id).maybeSingle(),
    ]);

    if (!profileRes.data) return res.status(404).json({ error: 'User not found' });
    return res.json({
      user:    profileRes.data,
      rides:   ridesRes.data ?? [],
      vehicle: vehicleRes.data ?? null,
    });
  });

  /** PATCH /api/admin/users/:id/role — change user role */
  app.patch('/api/admin/users/:id/role', async (req, res) => {
    const uid = await requireAdmin(supabase, getUserIdFromAccessToken, req, res);
    if (!uid) return;
    const role = req.body?.role;
    const allowed = ['user', 'captain', 'admin'];
    if (!allowed.includes(role)) {
      return res.status(400).json({ error: `role must be one of: ${allowed.join(', ')}` });
    }
    const { data, error } = await supabase
      .from('profiles')
      .update({ role })
      .eq('id', req.params.id)
      .select('id, role, full_name, phone')
      .maybeSingle();
    if (error) return res.status(500).json({ error: error.message });
    if (!data)  return res.status(404).json({ error: 'User not found' });
    return res.json({ ok: true, user: data });
  });

  // ──────────────────────────────────────────────────────────────────────────
  // CAPTAINS — dedicated view with KYC + vehicle
  // ──────────────────────────────────────────────────────────────────────────

  /**
   * GET /api/admin/captains?page=0&limit=30&kyc_status=&search=
   */
  app.get('/api/admin/captains', async (req, res) => {
    const uid = await requireAdmin(supabase, getUserIdFromAccessToken, req, res);
    if (!uid) return;

    const page      = Math.max(0, Number(req.query.page  ?? 0));
    const limit     = Math.min(100, Math.max(1, Number(req.query.limit ?? 30)));
    const from      = page * limit;
    const kycStatus = req.query.kyc_status;
    const search    = (req.query.search ?? '').trim();

    let query = supabase
      .from('profiles')
      .select(
        `id, full_name, phone, avatar_url, coin_balance,
         captain_kyc_status, captain_oshu_id, captain_vehicle_submitted_at,
         profile_completed_at, created_at`,
        { count: 'exact' },
      )
      .eq('role', 'captain')
      .order('created_at', { ascending: false })
      .range(from, from + limit - 1);

    if (kycStatus) query = query.eq('captain_kyc_status', kycStatus);
    if (search)    query = query.or(`full_name.ilike.%${search}%,phone.ilike.%${search}%`);

    const { data, error, count } = await query;
    if (error) return res.status(500).json({ error: error.message });

    // Fetch vehicle info for all captains in this page
    const captainIds = (data ?? []).map(c => c.id);
    let vehicles = [];
    if (captainIds.length) {
      const { data: vd } = await supabase
        .from('vehicles')
        .select('driver_id, vehicle_type, license_plate')
        .in('driver_id', captainIds);
      vehicles = vd ?? [];
    }
    const vehicleMap = Object.fromEntries(vehicles.map(v => [v.driver_id, v]));
    const captains = (data ?? []).map(c => ({ ...c, vehicle: vehicleMap[c.id] ?? null }));

    return res.json({ captains, total: count ?? 0, page, limit });
  });
}
