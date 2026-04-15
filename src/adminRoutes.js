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

async function getProfile(supabase, userId) {
  const { data } = await supabase
    .from('profiles')
    .select('id, role')
    .eq('id', userId)
    .maybeSingle();
  return data;
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
    res.status(403).json({ error: 'Admin only' });
    return null;
  }
  return uid;
}

export function registerAdminRoutes(app, { supabase, getUserIdFromAccessToken }) {

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
         coins_earned, coins_redeemed, created_at,
         customer:customer_id ( id, full_name, phone ),
         captain:driver_id    ( id, full_name, phone )`,
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
    return res.json({ rides: data ?? [], total: count ?? 0, page, limit });
  });

  /** GET /api/admin/rides/:id — full ride details */
  app.get('/api/admin/rides/:id', async (req, res) => {
    const uid = await requireAdmin(supabase, getUserIdFromAccessToken, req, res);
    if (!uid) return;
    const { data, error } = await supabase
      .from('ride_requests')
      .select(`*, customer:customer_id(*), captain:driver_id(*)`)
      .eq('id', req.params.id)
      .maybeSingle();
    if (error) return res.status(500).json({ error: error.message });
    if (!data)  return res.status(404).json({ error: 'Ride not found' });
    return res.json({ ride: data });
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
  // USERS — all profiles
  // ──────────────────────────────────────────────────────────────────────────

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
      supabase.from('ride_requests').select('id, status, quoted_price_inr, created_at').or(`customer_id.eq.${req.params.id},driver_id.eq.${req.params.id}`).order('created_at', { ascending: false }).limit(20),
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
