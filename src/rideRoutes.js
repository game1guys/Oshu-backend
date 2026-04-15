/**
 * Customer ride booking, per-km pricing, captain accept.
 */

import { haversineKm } from './geo.js';
import { awardCoinsForRide } from './coinRoutes.js';

async function getProfile(supabase, userId) {
  const { data, error } = await supabase.from('profiles').select('id, role, full_name').eq('id', userId).maybeSingle();
  if (error) {
    return null;
  }
  return data;
}

/** Full row fields needed for customer ride pricing / discounts. */
async function getCustomerProfileForPricing(supabase, userId) {
  const { data, error } = await supabase
    .from('profiles')
    .select(
      'id, role, full_name, customer_user_type, customer_monthly_order_range, customer_personalization_completed_at',
    )
    .eq('id', userId)
    .maybeSingle();
  if (error) {
    return null;
  }
  return data;
}

/**
 * Discount % from one-time personalization (server is source of truth).
 * Individual: welcome rate; seller: higher volume → higher %.
 */
function customerDiscountPercentFromProfile(profile) {
  if (!profile?.customer_personalization_completed_at) {
    return 0;
  }
  const t = profile.customer_user_type;
  if (t === 'individual') {
    return 5;
  }
  if (t === 'seller') {
    const r = profile.customer_monthly_order_range;
    if (r === '0-5') {
      return 5;
    }
    if (r === '6-10') {
      return 8;
    }
    if (r === '20+') {
      return 12;
    }
  }
  return 0;
}

function roundMoney(n) {
  return Math.round(n * 100) / 100;
}

function quoteFromRow(distanceKm, row) {
  const perKm = Number(row.price_per_km_inr);
  const minFare = Number(row.min_fare_inr ?? 0);
  const raw = distanceKm * perKm;
  return roundMoney(Math.max(minFare, raw));
}

/** Pickup lat/lng → pricing_zones.id (India bbox + future regional polygons). */
async function resolvePricingZoneId(supabase, pickupLat, pickupLng) {
  const { data, error } = await supabase.rpc('resolve_pricing_zone_id', {
    p_lat: pickupLat,
    p_lng: pickupLng,
  });
  if (error) {
    console.warn('[Oshu] resolve_pricing_zone_id', error.message);
    return null;
  }
  return data ?? null;
}

async function getPricingZoneIdBySlug(supabase, slug) {
  const { data } = await supabase.from('pricing_zones').select('id').eq('slug', slug).maybeSingle();
  return data?.id ?? null;
}

async function loadManpowerInr(supabase) {
  const { data } = await supabase.from('app_booking_config').select('manpower_helper_inr').eq('id', 1).maybeSingle();
  return roundMoney(Number(data?.manpower_helper_inr ?? 199));
}

/** @param {{ rate_inr?: unknown, is_active?: boolean, slug?: string } | null} row */
function packagingFeeFromRow(row) {
  if (!row) {
    return 0;
  }
  const slug = row.slug;
  const active = row.is_active !== false;
  if (!active && slug !== 'none') {
    return null;
  }
  return roundMoney(Number(row.rate_inr ?? 0));
}

function totalFromParts(base, packagingFee, manpowerFee) {
  return roundMoney(base + packagingFee + manpowerFee);
}

/**
 * Max cargo kg for a vehicle_pricing row: DB value, else catalog default (pre-migration / null).
 * Align with supabase migration 012_vehicle_pricing_max_weight_capacity_kg.sql.
 */
const VEHICLE_PRICING_DEFAULT_MAX_KG = {
  bike: 25,
  scooter: 35,
  auto: 200,
  e_rickshaw: 400,
  electric_3w: 500,
  tata_ace: 750,
  pickup: 1000,
  mini_truck: 1500,
  truck: 7000,
  canter: 4000,
  container_20ft: 12000,
  container_32ft: 20000,
  flatbed: 10000,
  electric_4w: 400,
  van: 800,
};

function maxCargoKgForPricingRow(row) {
  const fromDb = Number(row.max_weight_capacity_kg);
  if (Number.isFinite(fromDb) && fromDb > 0) {
    return fromDb;
  }
  const fb = VEHICLE_PRICING_DEFAULT_MAX_KG[row.vehicle_type];
  return typeof fb === 'number' ? fb : NaN;
}

async function resolvePackaging(supabase, packagingTypeId) {
  if (packagingTypeId == null || packagingTypeId === '') {
    return { row: null, fee: 0 };
  }
  const { data: row, error } = await supabase.from('packaging_types').select('*').eq('id', packagingTypeId).maybeSingle();
  if (error) {
    return { error: error.message };
  }
  if (!row) {
    return { error: 'Unknown packaging type' };
  }
  const fee = packagingFeeFromRow(row);
  if (fee === null) {
    return { error: 'Packaging type is not available' };
  }
  return { row, fee };
}

const DISPATCH_RADIUS_KM = 5;
/** Rough urban speed for ETA captain → pickup (km/h). */
const CAPTAIN_ETA_SPEED_KMH = 25;

function genHandshakePin() {
  return String(1000 + Math.floor(Math.random() * 9000));
}

async function loadRideServiceDefaults(supabase) {
  const { data } = await supabase
    .from('app_booking_config')
    .select('ride_included_service_minutes, ride_overtime_inr_per_min')
    .eq('id', 1)
    .maybeSingle();
  return {
    includedMin: Math.max(0, Number(data?.ride_included_service_minutes ?? 45)),
    overtimePerMin: Math.max(0, Number(data?.ride_overtime_inr_per_min ?? 10)),
  };
}

/** Notify captains within 5 km with matching vehicle type (Socket.IO rooms driver:{id}). */
async function notifyCaptainsNearby(io, supabase, ride) {
  if (!io || !supabase || ride?.pickup_lat == null) {
    return;
  }
  const staleBefore = new Date(Date.now() - 20 * 60 * 1000).toISOString();
  const { data: presences, error } = await supabase
    .from('captain_presence')
    .select('driver_id, lat, lng')
    .eq('is_available', true)
    .gte('updated_at', staleBefore);
  if (error || !presences?.length) {
    return;
  }
  for (const row of presences) {
    const d = haversineKm(row.lat, row.lng, ride.pickup_lat, ride.pickup_lng);
    if (d > DISPATCH_RADIUS_KM) {
      continue;
    }
    const { data: v } = await supabase.from('vehicles').select('type').eq('driver_id', row.driver_id).maybeSingle();
    if (!v || v.type !== ride.vehicle_type) {
      continue;
    }
    const safeRide = { ...ride };
    delete safeRide.handshake_pin;
    io.to(`driver:${row.driver_id}`).emit('ride:offer', {
      ride: safeRide,
      km_to_pickup: roundMoney(d),
    });
  }
}

function emitRide(io, ride) {
  if (io && ride?.id) {
    const safe = { ...ride };
    delete safe.handshake_pin;
    io.emit('ride:update', { ride: safe });
  }
}

export function registerRideRoutes(app, { supabase, getUserIdFromAccessToken, io }) {
  /** Public: per-km rates for a pricing zone. Optional ?pickup_lat=&pickup_lng= resolves zone; else India (in). */
  app.get('/api/pricing', async (req, res) => {
    if (!supabase) {
      return res.status(503).json({ error: 'Database not configured' });
    }
    const lat = req.query.pickup_lat != null ? Number(req.query.pickup_lat) : NaN;
    const lng = req.query.pickup_lng != null ? Number(req.query.pickup_lng) : NaN;
    let zoneId = null;
    if (!Number.isNaN(lat) && !Number.isNaN(lng)) {
      zoneId = await resolvePricingZoneId(supabase, lat, lng);
    } else {
      zoneId = await getPricingZoneIdBySlug(supabase, 'in');
    }
    if (!zoneId) {
      return res.status(400).json({
        error:
          'No pricing for this location. Oshu currently serves pickup points within India only.',
      });
    }
    const { data: zoneMeta } = await supabase.from('pricing_zones').select('id, slug, name').eq('id', zoneId).maybeSingle();
    const { data, error } = await supabase
      .from('vehicle_pricing')
      .select('*')
      .eq('zone_id', zoneId)
      .order('vehicle_type');
    if (error) {
      return res.status(500).json({ error: error.message });
    }
    return res.json({ pricing: data ?? [], pricing_zone: zoneMeta ?? null });
  });

  /** Customer: segment discount from personalization (home / account banners). */
  app.get('/api/customer/discount-eligibility', async (req, res) => {
    const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
    if (!token || !supabase) {
      return res.status(400).json({ error: 'Missing Authorization' });
    }
    const uid = await getUserIdFromAccessToken(token);
    if (!uid) {
      return res.status(401).json({ error: 'Invalid token' });
    }
    const profile = await getCustomerProfileForPricing(supabase, uid);
    if (!profile || profile.role !== 'user') {
      return res.status(403).json({ error: 'Customers only' });
    }
    const pct = customerDiscountPercentFromProfile(profile);
    const label =
      pct > 0
        ? profile.customer_user_type === 'individual'
          ? 'Individual welcome offer'
          : 'Business seller offer'
        : null;
    return res.json({
      personalized: Boolean(profile.customer_personalization_completed_at),
      customer_discount_percent: pct,
      discount_label: label,
      customer_user_type: profile.customer_user_type ?? null,
    });
  });

  /** Admin: list / upsert pricing row. */
  app.get('/api/admin/pricing', async (req, res) => {
    const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
    if (!token || !supabase) {
      return res.status(400).json({ error: 'Missing Authorization' });
    }
    const uid = await getUserIdFromAccessToken(token);
    if (!uid) {
      return res.status(401).json({ error: 'Invalid token' });
    }
    const profile = await getProfile(supabase, uid);
    if (!profile || profile.role !== 'admin') {
      return res.status(403).json({ error: 'Admin only' });
    }
    const zoneSlug =
      typeof req.query.zone === 'string' && req.query.zone.trim() ? req.query.zone.trim() : 'in';
    const zoneId = await getPricingZoneIdBySlug(supabase, zoneSlug);
    if (!zoneId) {
      return res.status(400).json({ error: 'Unknown pricing zone' });
    }
    const { data: zones } = await supabase.from('pricing_zones').select('id, slug, name, priority').order('name');
    const { data, error } = await supabase
      .from('vehicle_pricing')
      .select('*')
      .eq('zone_id', zoneId)
      .order('vehicle_type');
    if (error) {
      return res.status(500).json({ error: error.message });
    }
    return res.json({ pricing: data ?? [], pricing_zones: zones ?? [], zone_slug: zoneSlug });
  });

  app.put('/api/admin/pricing/:vehicleType', async (req, res) => {
    const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
    if (!token || !supabase) {
      return res.status(400).json({ error: 'Missing Authorization' });
    }
    const uid = await getUserIdFromAccessToken(token);
    if (!uid) {
      return res.status(401).json({ error: 'Invalid token' });
    }
    const profile = await getProfile(supabase, uid);
    if (!profile || profile.role !== 'admin') {
      return res.status(403).json({ error: 'Admin only' });
    }
    const vehicleType = req.params.vehicleType;
    const b = req.body ?? {};
    const zoneSlug =
      typeof b.zone_slug === 'string' && b.zone_slug.trim() ? b.zone_slug.trim() : 'in';
    const zoneId = await getPricingZoneIdBySlug(supabase, zoneSlug);
    if (!zoneId) {
      return res.status(400).json({ error: 'Unknown pricing zone' });
    }
    const price_per_km_inr = typeof b.price_per_km_inr === 'number' ? b.price_per_km_inr : NaN;
    const min_fare_inr = typeof b.min_fare_inr === 'number' ? b.min_fare_inr : 0;
    if (Number.isNaN(price_per_km_inr) || price_per_km_inr < 0) {
      return res.status(400).json({ error: 'Invalid price_per_km_inr' });
    }
    const { data: existingRow } = await supabase
      .from('vehicle_pricing')
      .select('*')
      .eq('vehicle_type', vehicleType)
      .eq('zone_id', zoneId)
      .maybeSingle();
    let max_weight_capacity_kg =
      typeof b.max_weight_capacity_kg === 'number' && !Number.isNaN(b.max_weight_capacity_kg) && b.max_weight_capacity_kg > 0
        ? roundMoney(b.max_weight_capacity_kg)
        : null;
    if (max_weight_capacity_kg == null && existingRow != null && Number(existingRow.max_weight_capacity_kg) > 0) {
      max_weight_capacity_kg = Number(existingRow.max_weight_capacity_kg);
    }
    if (max_weight_capacity_kg == null || max_weight_capacity_kg <= 0) {
      return res.status(400).json({ error: 'Invalid or missing max_weight_capacity_kg (positive kg)' });
    }
    const { data, error } = await supabase
      .from('vehicle_pricing')
      .upsert(
        {
          zone_id: zoneId,
          vehicle_type: vehicleType,
          price_per_km_inr,
          min_fare_inr,
          max_weight_capacity_kg,
          updated_at: new Date().toISOString(),
        },
        { onConflict: 'zone_id,vehicle_type' },
      )
      .select('*')
      .single();
    if (error) {
      return res.status(500).json({ error: error.message });
    }
    return res.json({ pricing: data });
  });

  /** Customer: cargo categories, packaging list, manpower rate (for booking UI). */
  app.get('/api/booking-options', async (req, res) => {
    const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
    if (!token || !supabase) {
      return res.status(400).json({ error: 'Missing Authorization' });
    }
    const uid = await getUserIdFromAccessToken(token);
    if (!uid) {
      return res.status(401).json({ error: 'Invalid token' });
    }
    const profile = await getProfile(supabase, uid);
    if (!profile || profile.role !== 'user') {
      return res.status(403).json({ error: 'Customers only' });
    }
    const [catRes, packRes, mpInr] = await Promise.all([
      supabase.from('cargo_space_categories').select('id, slug, label, sort_order').order('sort_order'),
      supabase
        .from('packaging_types')
        .select('id, slug, label, rate_inr, is_active, sort_order')
        .or('is_active.eq.true,slug.eq.none')
        .order('sort_order'),
      loadManpowerInr(supabase),
    ]);
    if (catRes.error) {
      return res.status(500).json({ error: catRes.error.message });
    }
    if (packRes.error) {
      return res.status(500).json({ error: packRes.error.message });
    }
    const cargo_materials = [
      { value: 'plastic', label: 'Plastic' },
      { value: 'wood', label: 'Wood' },
      { value: 'metal', label: 'Metal' },
      { value: 'mixed', label: 'Mixed' },
      { value: 'other', label: 'Other' },
    ];
    return res.json({
      cargo_categories: catRes.data ?? [],
      cargo_materials,
      packaging_types: packRes.data ?? [],
      manpower_helper_inr: mpInr,
    });
  });

  /** Admin: packaging types. */
  app.get('/api/admin/packaging-types', async (req, res) => {
    const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
    if (!token || !supabase) {
      return res.status(400).json({ error: 'Missing Authorization' });
    }
    const uid = await getUserIdFromAccessToken(token);
    if (!uid) {
      return res.status(401).json({ error: 'Invalid token' });
    }
    const profile = await getProfile(supabase, uid);
    if (!profile || profile.role !== 'admin') {
      return res.status(403).json({ error: 'Admin only' });
    }
    const { data, error } = await supabase.from('packaging_types').select('*').order('sort_order');
    if (error) {
      return res.status(500).json({ error: error.message });
    }
    return res.json({ packaging_types: data ?? [] });
  });

  app.put('/api/admin/packaging-types/:id', async (req, res) => {
    const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
    if (!token || !supabase) {
      return res.status(400).json({ error: 'Missing Authorization' });
    }
    const uid = await getUserIdFromAccessToken(token);
    if (!uid) {
      return res.status(401).json({ error: 'Invalid token' });
    }
    const profile = await getProfile(supabase, uid);
    if (!profile || profile.role !== 'admin') {
      return res.status(403).json({ error: 'Admin only' });
    }
    const id = req.params.id;
    const b = req.body ?? {};
    const patch = {};
    if (typeof b.label === 'string') {
      patch.label = b.label.trim();
    }
    if (typeof b.rate_inr === 'number' && !Number.isNaN(b.rate_inr) && b.rate_inr >= 0) {
      patch.rate_inr = b.rate_inr;
    }
    if (typeof b.is_active === 'boolean') {
      patch.is_active = b.is_active;
    }
    if (typeof b.sort_order === 'number' && !Number.isNaN(b.sort_order)) {
      patch.sort_order = b.sort_order;
    }
    if (Object.keys(patch).length === 0) {
      return res.status(400).json({ error: 'Nothing to update' });
    }
    patch.updated_at = new Date().toISOString();
    const { data, error } = await supabase.from('packaging_types').update(patch).eq('id', id).select('*').maybeSingle();
    if (error) {
      return res.status(500).json({ error: error.message });
    }
    if (!data) {
      return res.status(404).json({ error: 'Not found' });
    }
    return res.json({ packaging_type: data });
  });

  app.get('/api/admin/booking-config', async (req, res) => {
    const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
    if (!token || !supabase) {
      return res.status(400).json({ error: 'Missing Authorization' });
    }
    const uid = await getUserIdFromAccessToken(token);
    if (!uid) {
      return res.status(401).json({ error: 'Invalid token' });
    }
    const profile = await getProfile(supabase, uid);
    if (!profile || profile.role !== 'admin') {
      return res.status(403).json({ error: 'Admin only' });
    }
    const { data: cfg } = await supabase.from('app_booking_config').select('*').eq('id', 1).maybeSingle();
    const mp = await loadManpowerInr(supabase);
    const svc = await loadRideServiceDefaults(supabase);
    return res.json({
      manpower_helper_inr: mp,
      ride_included_service_minutes: cfg?.ride_included_service_minutes ?? svc.includedMin,
      ride_overtime_inr_per_min: cfg?.ride_overtime_inr_per_min ?? svc.overtimePerMin,
    });
  });

  app.put('/api/admin/booking-config', async (req, res) => {
    const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
    if (!token || !supabase) {
      return res.status(400).json({ error: 'Missing Authorization' });
    }
    const uid = await getUserIdFromAccessToken(token);
    if (!uid) {
      return res.status(401).json({ error: 'Invalid token' });
    }
    const profile = await getProfile(supabase, uid);
    if (!profile || profile.role !== 'admin') {
      return res.status(403).json({ error: 'Admin only' });
    }
    const b = req.body ?? {};
    const { data: cur } = await supabase.from('app_booking_config').select('*').eq('id', 1).maybeSingle();
    const manpower_helper_inr =
      typeof b.manpower_helper_inr === 'number' && !Number.isNaN(b.manpower_helper_inr) && b.manpower_helper_inr >= 0
        ? roundMoney(b.manpower_helper_inr)
        : NaN;
    if (Number.isNaN(manpower_helper_inr)) {
      return res.status(400).json({ error: 'Invalid manpower_helper_inr' });
    }
    let ride_included_service_minutes =
      typeof b.ride_included_service_minutes === 'number' &&
      !Number.isNaN(b.ride_included_service_minutes) &&
      b.ride_included_service_minutes >= 0
        ? Math.floor(b.ride_included_service_minutes)
        : cur?.ride_included_service_minutes ?? 45;
    let ride_overtime_inr_per_min =
      typeof b.ride_overtime_inr_per_min === 'number' &&
      !Number.isNaN(b.ride_overtime_inr_per_min) &&
      b.ride_overtime_inr_per_min >= 0
        ? roundMoney(b.ride_overtime_inr_per_min)
        : roundMoney(Number(cur?.ride_overtime_inr_per_min ?? 10));
    const patch = {
      id: 1,
      manpower_helper_inr,
      ride_included_service_minutes,
      ride_overtime_inr_per_min,
      updated_at: new Date().toISOString(),
    };
    const { data, error } = await supabase.from('app_booking_config').upsert(patch, { onConflict: 'id' }).select('*').single();
    if (error) {
      return res.status(500).json({ error: error.message });
    }
    return res.json({
      manpower_helper_inr: Number(data.manpower_helper_inr),
      ride_included_service_minutes: Number(data.ride_included_service_minutes ?? 45),
      ride_overtime_inr_per_min: Number(data.ride_overtime_inr_per_min ?? 10),
    });
  });

  /** Customer: distance + estimated fare for every vehicle class. */
  app.post('/api/rides/quote', async (req, res) => {
    const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
    if (!token || !supabase) {
      return res.status(400).json({ error: 'Missing Authorization' });
    }
    const uid = await getUserIdFromAccessToken(token);
    if (!uid) {
      return res.status(401).json({ error: 'Invalid token' });
    }
    const profile = await getCustomerProfileForPricing(supabase, uid);
    if (!profile || profile.role !== 'user') {
      return res.status(403).json({ error: 'Customers only' });
    }
    const discountPct = customerDiscountPercentFromProfile(profile);
    const b = req.body ?? {};
    const pickup_lat = Number(b.pickup_lat);
    const pickup_lng = Number(b.pickup_lng);
    const drop_lat = Number(b.drop_lat);
    const drop_lng = Number(b.drop_lng);
    if ([pickup_lat, pickup_lng, drop_lat, drop_lng].some(x => Number.isNaN(x))) {
      return res.status(400).json({ error: 'Invalid coordinates' });
    }
    const distance_km = roundMoney(haversineKm(pickup_lat, pickup_lng, drop_lat, drop_lng));
    const bq = req.body ?? {};
    const packaging_type_id =
      typeof bq.packaging_type_id === 'string' && bq.packaging_type_id.trim() ? bq.packaging_type_id.trim() : null;
    const manpower_requested = Boolean(bq.manpower_requested);
    const weightRaw = bq.weight_kg != null && bq.weight_kg !== '' ? Number(bq.weight_kg) : NaN;
    const filterWeightKg = Number.isFinite(weightRaw) && weightRaw > 0 ? weightRaw : null;
    const manpowerInr = await loadManpowerInr(supabase);
    const svcDefaults = await loadRideServiceDefaults(supabase);
    const packResolved = await resolvePackaging(supabase, packaging_type_id);
    if ('error' in packResolved) {
      return res.status(400).json({ error: packResolved.error });
    }
    const packaging_fee_inr = packResolved.fee;
    const manpower_fee_inr = manpower_requested ? manpowerInr : 0;
    const pricingZoneId = await resolvePricingZoneId(supabase, pickup_lat, pickup_lng);
    if (!pricingZoneId) {
      return res.status(400).json({
        error:
          'Pickup is outside our India service area. Choose pickup and drop locations within India.',
      });
    }
    const { data: zoneMeta } = await supabase
      .from('pricing_zones')
      .select('id, slug, name')
      .eq('id', pricingZoneId)
      .maybeSingle();
    const { data: rows, error } = await supabase
      .from('vehicle_pricing')
      .select('*')
      .eq('zone_id', pricingZoneId)
      .order('vehicle_type');
    if (error) {
      return res.status(500).json({ error: error.message });
    }
    const eligible =
      filterWeightKg != null
        ? (rows ?? []).filter(r => {
            const cap = maxCargoKgForPricingRow(r);
            return Number.isFinite(cap) && cap >= filterWeightKg;
          })
        : (rows ?? []);
    const options = eligible.map(row => {
      const base_fare_inr = quoteFromRow(distance_km, row);
      const subtotal = totalFromParts(base_fare_inr, packaging_fee_inr, manpower_fee_inr);
      const discount_inr = roundMoney(subtotal * (discountPct / 100));
      const estimated_price_inr = roundMoney(Math.max(0, subtotal - discount_inr));
      const capKg = maxCargoKgForPricingRow(row);
      return {
        vehicle_type: row.vehicle_type,
        price_per_km_inr: Number(row.price_per_km_inr),
        min_fare_inr: Number(row.min_fare_inr ?? 0),
        ...(Number.isFinite(capKg) ? { max_weight_capacity_kg: capKg } : {}),
        base_fare_inr,
        packaging_fee_inr,
        manpower_fee_inr,
        estimated_subtotal_inr: subtotal,
        customer_discount_percent: discountPct,
        discount_inr,
        estimated_price_inr,
      };
    });
    return res.json({
      distance_km,
      packaging_fee_inr,
      manpower_fee_inr,
      manpower_helper_inr: manpowerInr,
      ride_included_service_minutes: svcDefaults.includedMin,
      ride_overtime_inr_per_min: svcDefaults.overtimePerMin,
      cargo_weight_kg: filterWeightKg,
      customer_discount_percent: discountPct,
      customer_discount_label:
        discountPct > 0
          ? profile.customer_user_type === 'individual'
            ? 'Individual welcome offer'
            : 'Business seller offer'
          : null,
      pricing_zone: zoneMeta ?? null,
      options,
    });
  });

  /** Customer: create a pending ride request (price computed server-side). */
  app.post('/api/rides', async (req, res) => {
    const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
    if (!token || !supabase) {
      return res.status(400).json({ error: 'Missing Authorization' });
    }
    const uid = await getUserIdFromAccessToken(token);
    if (!uid) {
      return res.status(401).json({ error: 'Invalid token' });
    }
    const profile = await getCustomerProfileForPricing(supabase, uid);
    if (!profile || profile.role !== 'user') {
      return res.status(403).json({ error: 'Customers only' });
    }
    const discountPct = customerDiscountPercentFromProfile(profile);
    const b = req.body ?? {};
    const pickup_lat = Number(b.pickup_lat);
    const pickup_lng = Number(b.pickup_lng);
    const drop_lat = Number(b.drop_lat);
    const drop_lng = Number(b.drop_lng);
    const vehicle_type = typeof b.vehicle_type === 'string' ? b.vehicle_type.trim() : '';
    const cargo_description = typeof b.cargo_description === 'string' ? b.cargo_description.trim() : '';
    const weight_kg = b.weight_kg != null ? Number(b.weight_kg) : null;
    const pickup_address = typeof b.pickup_address === 'string' ? b.pickup_address.trim() : '';
    const drop_address = typeof b.drop_address === 'string' ? b.drop_address.trim() : '';
    let cargo_category_id =
      typeof b.cargo_category_id === 'string' && b.cargo_category_id.trim() ? b.cargo_category_id.trim() : null;
    const cargo_category_slug =
      typeof b.cargo_category_slug === 'string' && b.cargo_category_slug.trim()
        ? b.cargo_category_slug.trim()
        : null;
    if (!cargo_category_id && cargo_category_slug) {
      const { data: catBySlug } = await supabase
        .from('cargo_space_categories')
        .select('id')
        .eq('slug', cargo_category_slug)
        .maybeSingle();
      cargo_category_id = catBySlug?.id ?? null;
    }
    const cargo_material =
      typeof b.cargo_material === 'string' && b.cargo_material.trim() ? b.cargo_material.trim() : null;
    const cargo_material_other =
      typeof b.cargo_material_other === 'string' ? b.cargo_material_other.trim() : '';
    const packaging_type_id =
      typeof b.packaging_type_id === 'string' && b.packaging_type_id.trim() ? b.packaging_type_id.trim() : null;
    const manpower_requested = Boolean(b.manpower_requested);
    const coins_to_redeem = Math.max(0, Math.floor(Number(b.coins_to_redeem ?? 0)));
    const allowedMaterials = new Set(['plastic', 'wood', 'metal', 'mixed', 'other']);
    if (cargo_material && !allowedMaterials.has(cargo_material)) {
      return res.status(400).json({ error: 'Invalid cargo_material' });
    }
    if (cargo_material === 'other' && !cargo_material_other) {
      return res.status(400).json({ error: 'Please specify the material when you choose Other' });
    }
    if ([pickup_lat, pickup_lng, drop_lat, drop_lng].some(x => Number.isNaN(x)) || !vehicle_type) {
      return res.status(400).json({ error: 'Invalid pickup/drop/vehicle' });
    }
    if (cargo_category_id) {
      const { data: catRow } = await supabase.from('cargo_space_categories').select('id').eq('id', cargo_category_id).maybeSingle();
      if (!catRow) {
        return res.status(400).json({ error: 'Invalid cargo category' });
      }
    }
    const packResolved = await resolvePackaging(supabase, packaging_type_id);
    if ('error' in packResolved) {
      return res.status(400).json({ error: packResolved.error });
    }
    const packaging_fee_inr = packResolved.fee;
    const manpowerInr = await loadManpowerInr(supabase);
    const manpower_fee_inr = manpower_requested ? manpowerInr : 0;
    const distance_km = roundMoney(haversineKm(pickup_lat, pickup_lng, drop_lat, drop_lng));
    const pricingZoneId = await resolvePricingZoneId(supabase, pickup_lat, pickup_lng);
    if (!pricingZoneId) {
      return res.status(400).json({
        error:
          'Pickup is outside our India service area. Choose pickup and drop locations within India.',
      });
    }
    const { data: priceRow, error: pErr } = await supabase
      .from('vehicle_pricing')
      .select('*')
      .eq('zone_id', pricingZoneId)
      .eq('vehicle_type', vehicle_type)
      .maybeSingle();
    if (pErr || !priceRow) {
      return res.status(400).json({ error: 'Unknown vehicle type for this region' });
    }
    const typeMaxKg = maxCargoKgForPricingRow(priceRow);
    if (
      weight_kg != null &&
      !Number.isNaN(weight_kg) &&
      weight_kg > 0 &&
      Number.isFinite(typeMaxKg) &&
      weight_kg > typeMaxKg
    ) {
      return res.status(400).json({
        error: 'This vehicle class cannot carry your cargo weight. Choose a vehicle with higher capacity.',
      });
    }
    const base_fare_inr = quoteFromRow(distance_km, priceRow);
    const subtotal_before_discount_inr = totalFromParts(base_fare_inr, packaging_fee_inr, manpower_fee_inr);
    const discount_inr = roundMoney(subtotal_before_discount_inr * (discountPct / 100));
    const subtotal_after_segment_discount = roundMoney(Math.max(0, subtotal_before_discount_inr - discount_inr));

    // Validate and apply coin redemption
    let coinsApplied = 0;
    let coinDiscountInr = 0;
    if (coins_to_redeem > 0) {
      const { data: profileWithCoins } = await supabase
        .from('profiles').select('coin_balance').eq('id', uid).maybeSingle();
      const balance = Number(profileWithCoins?.coin_balance ?? 0);
      // Cap: max 20% of fare, and can't exceed balance
      const maxByPct = Math.floor(subtotal_after_segment_discount * 0.2);
      coinsApplied = Math.min(coins_to_redeem, balance, maxByPct);
      coinDiscountInr = coinsApplied;
    }
    const quoted_price_inr = roundMoney(Math.max(0, subtotal_after_segment_discount - coinDiscountInr));

    const svcDefaults = await loadRideServiceDefaults(supabase);
    const handshake_pin = genHandshakePin();

    const { data: ride, error } = await supabase
      .from('ride_requests')
      .insert({
        customer_id: uid,
        pickup_lat,
        pickup_lng,
        drop_lat,
        drop_lng,
        pickup_address: pickup_address || null,
        drop_address: drop_address || null,
        cargo_category_id: cargo_category_id || null,
        cargo_material: cargo_material || null,
        cargo_material_other: cargo_material === 'other' ? cargo_material_other || null : null,
        cargo_description: cargo_description || null,
        weight_kg: weight_kg != null && !Number.isNaN(weight_kg) ? weight_kg : null,
        packaging_type_id: packResolved.row?.id ?? null,
        packaging_fee_inr,
        manpower_requested,
        manpower_fee_inr,
        vehicle_type,
        status: 'pending',
        distance_km,
        base_fare_inr,
        subtotal_before_discount_inr,
        customer_discount_percent: discountPct,
        coins_redeemed: coinsApplied,
        coin_discount_inr: coinDiscountInr,
        quoted_price_inr,
        handshake_pin,
        included_service_minutes: svcDefaults.includedMin,
        overtime_inr_per_min: svcDefaults.overtimePerMin,
      })
      .select('*')
      .single();
    if (error) {
      return res.status(500).json({ error: error.message });
    }
    // Award coins for ride (earn based on distance, deduct redeemed) — fire and forget
    void awardCoinsForRide(supabase, {
      userId: uid,
      rideId: ride.id,
      distanceKm: distance_km,
      coinsRedeemed: coinsApplied,
    });
    const coinsWillEarn = Math.floor(distance_km / 7);
    emitRide(io, ride);
    void notifyCaptainsNearby(io, supabase, ride);
    return res.json({ ride, coins_earned: coinsWillEarn, coins_redeemed: coinsApplied });
  });

  /** Customer: my rides. */
  app.get('/api/rides/my', async (req, res) => {
    const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
    if (!token || !supabase) {
      return res.status(400).json({ error: 'Missing Authorization' });
    }
    const uid = await getUserIdFromAccessToken(token);
    if (!uid) {
      return res.status(401).json({ error: 'Invalid token' });
    }
    const profile = await getProfile(supabase, uid);
    if (!profile || profile.role !== 'user') {
      return res.status(403).json({ error: 'Customers only' });
    }
    const { data, error } = await supabase
      .from('ride_requests')
      .select('*')
      .eq('customer_id', uid)
      .order('created_at', { ascending: false })
      .limit(100);
    if (error) {
      return res.status(500).json({ error: error.message });
    }
    return res.json({ rides: data ?? [] });
  });

  /** Captain: pending requests within 5 km of pickup, same vehicle class as yours. */
  app.get('/api/rides/pending', async (req, res) => {
    const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
    if (!token || !supabase) {
      return res.status(400).json({ error: 'Missing Authorization' });
    }
    const uid = await getUserIdFromAccessToken(token);
    if (!uid) {
      return res.status(401).json({ error: 'Invalid token' });
    }
    const profile = await getProfile(supabase, uid);
    if (!profile || profile.role !== 'captain') {
      return res.status(403).json({ error: 'Captains only' });
    }
    const capLat = parseFloat(String(req.query.lat ?? ''));
    const capLng = parseFloat(String(req.query.lng ?? ''));
    const hasCap = !Number.isNaN(capLat) && !Number.isNaN(capLng);
    if (!hasCap) {
      return res.status(400).json({ error: 'lat and lng required for nearby dispatch' });
    }
    const { data: myVehicle } = await supabase.from('vehicles').select('type').eq('driver_id', uid).maybeSingle();
    if (!myVehicle?.type) {
      return res.status(400).json({ error: 'Register your vehicle before accepting jobs' });
    }
    const { data: rows, error } = await supabase
      .from('ride_requests')
      .select('*')
      .eq('status', 'pending')
      .eq('vehicle_type', myVehicle.type)
      .order('created_at', { ascending: true })
      .limit(80);
    if (error) {
      return res.status(500).json({ error: error.message });
    }
    const { data: dismissRows } = await supabase
      .from('ride_request_dismissals')
      .select('ride_id')
      .eq('captain_id', uid);
    const dismissed = new Set((dismissRows ?? []).map(d => d.ride_id));
    const list = [];
    for (const r of rows ?? []) {
      if (dismissed.has(r.id)) {
        continue;
      }
      const km = roundMoney(haversineKm(capLat, capLng, r.pickup_lat, r.pickup_lng));
      if (km <= DISPATCH_RADIUS_KM) {
        list.push({ ...r, km_to_pickup: km });
      }
    }
    list.sort((a, b) => (a.km_to_pickup ?? 0) - (b.km_to_pickup ?? 0));
    const sanitized = list.map(r => {
      const x = { ...r };
      delete x.handshake_pin;
      return x;
    });
    return res.json({ rides: sanitized });
  });

  /** Captain: my accepted / active rides. */
  app.get('/api/rides/captain-my', async (req, res) => {
    const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
    if (!token || !supabase) {
      return res.status(400).json({ error: 'Missing Authorization' });
    }
    const uid = await getUserIdFromAccessToken(token);
    if (!uid) {
      return res.status(401).json({ error: 'Invalid token' });
    }
    const profile = await getProfile(supabase, uid);
    if (!profile || profile.role !== 'captain') {
      return res.status(403).json({ error: 'Captains only' });
    }
    const { data, error } = await supabase
      .from('ride_requests')
      .select('*')
      .eq('captain_id', uid)
      .in('status', ['accepted', 'in_progress'])
      .order('updated_at', { ascending: false });
    if (error) {
      return res.status(500).json({ error: error.message });
    }
    return res.json({ rides: data ?? [] });
  });

  /** Captain: accept a pending ride (first wins). */
  app.post('/api/rides/:id/accept', async (req, res) => {
    const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
    if (!token || !supabase) {
      return res.status(400).json({ error: 'Missing Authorization' });
    }
    const uid = await getUserIdFromAccessToken(token);
    if (!uid) {
      return res.status(401).json({ error: 'Invalid token' });
    }
    const profile = await getProfile(supabase, uid);
    if (!profile || profile.role !== 'captain') {
      return res.status(403).json({ error: 'Captains only' });
    }
    const id = req.params.id;
    const at = new Date().toISOString();
    const { data, error } = await supabase
      .from('ride_requests')
      .update({
        captain_id: uid,
        status: 'accepted',
        accepted_at: at,
        updated_at: at,
      })
      .eq('id', id)
      .eq('status', 'pending')
      .select('*')
      .maybeSingle();
    if (error) {
      return res.status(500).json({ error: error.message });
    }
    if (!data) {
      return res.status(409).json({ error: 'Already taken or not found' });
    }
    emitRide(io, data);
    return res.json({ ride: data });
  });

  /** Captain: stop showing this pending request in my list (other captains unaffected). */
  app.post('/api/rides/:id/decline', async (req, res) => {
    const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
    if (!token || !supabase) {
      return res.status(400).json({ error: 'Missing Authorization' });
    }
    const uid = await getUserIdFromAccessToken(token);
    if (!uid) {
      return res.status(401).json({ error: 'Invalid token' });
    }
    const profile = await getProfile(supabase, uid);
    if (!profile || profile.role !== 'captain') {
      return res.status(403).json({ error: 'Captains only' });
    }
    const id = req.params.id;
    const { data: row, error: fetchErr } = await supabase.from('ride_requests').select('id, status').eq('id', id).maybeSingle();
    if (fetchErr) {
      return res.status(500).json({ error: fetchErr.message });
    }
    if (!row || row.status !== 'pending') {
      return res.status(409).json({ error: 'Can only decline open requests' });
    }
    const { error } = await supabase.from('ride_request_dismissals').insert({
      ride_id: id,
      captain_id: uid,
    });
    if (error && error.code !== '23505') {
      return res.status(500).json({ error: error.message });
    }
    return res.json({ ok: true });
  });

  /** Captain: mark accepted → in progress (customer tells you the 4-digit handshake PIN). */
  app.post('/api/rides/:id/start', async (req, res) => {
    const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
    if (!token || !supabase) {
      return res.status(400).json({ error: 'Missing Authorization' });
    }
    const uid = await getUserIdFromAccessToken(token);
    if (!uid) {
      return res.status(401).json({ error: 'Invalid token' });
    }
    const profile = await getProfile(supabase, uid);
    if (!profile || profile.role !== 'captain') {
      return res.status(403).json({ error: 'Captains only' });
    }
    const id = req.params.id;
    const pin = String(req.body?.handshake_pin ?? '').replace(/\D/g, '');
    const { data: row, error: fetchErr } = await supabase.from('ride_requests').select('*').eq('id', id).maybeSingle();
    if (fetchErr) {
      return res.status(500).json({ error: fetchErr.message });
    }
    if (!row || row.captain_id !== uid || row.status !== 'accepted') {
      return res.status(409).json({ error: 'Cannot start' });
    }
    const expected = String(row.handshake_pin ?? '').trim();
    if (!expected || pin !== expected) {
      return res.status(400).json({ error: 'Invalid handshake PIN from customer' });
    }
    const at = new Date().toISOString();
    const { data, error } = await supabase
      .from('ride_requests')
      .update({
        status: 'in_progress',
        ride_started_at: at,
        updated_at: at,
      })
      .eq('id', id)
      .eq('captain_id', uid)
      .eq('status', 'accepted')
      .select('*')
      .maybeSingle();
    if (error) {
      return res.status(500).json({ error: error.message });
    }
    if (!data) {
      return res.status(409).json({ error: 'Cannot start' });
    }
    emitRide(io, data);
    return res.json({ ride: data });
  });

  /** Captain: complete delivery (adds overtime beyond included service minutes). */
  app.post('/api/rides/:id/complete', async (req, res) => {
    const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
    if (!token || !supabase) {
      return res.status(400).json({ error: 'Missing Authorization' });
    }
    const uid = await getUserIdFromAccessToken(token);
    if (!uid) {
      return res.status(401).json({ error: 'Invalid token' });
    }
    const profile = await getProfile(supabase, uid);
    if (!profile || profile.role !== 'captain') {
      return res.status(403).json({ error: 'Captains only' });
    }
    const id = req.params.id;
    const { data: row, error: rowErr } = await supabase.from('ride_requests').select('*').eq('id', id).maybeSingle();
    if (rowErr) {
      return res.status(500).json({ error: rowErr.message });
    }
    if (!row || row.captain_id !== uid || row.status !== 'in_progress') {
      return res.status(409).json({ error: 'Cannot complete' });
    }
    const end = new Date();
    const start = row.ride_started_at ? new Date(row.ride_started_at) : null;
    const included = Math.max(0, Number(row.included_service_minutes ?? 45));
    const rate = Math.max(0, Number(row.overtime_inr_per_min ?? 10));
    const baseFare = Number(row.quoted_price_inr ?? 0);
    let overtimeMin = 0;
    let overtimeCharge = 0;
    if (start && !Number.isNaN(start.getTime())) {
      const durationMin = Math.max(0, Math.ceil((end.getTime() - start.getTime()) / 60000));
      overtimeMin = Math.max(0, durationMin - included);
      overtimeCharge = roundMoney(overtimeMin * rate);
    }
    const finalPayable = roundMoney(baseFare + overtimeCharge);
    const at = end.toISOString();
    const { data, error } = await supabase
      .from('ride_requests')
      .update({
        status: 'completed',
        ride_completed_at: at,
        updated_at: at,
        overtime_minutes: overtimeMin,
        overtime_charge_inr: overtimeCharge,
        final_payable_inr: finalPayable,
      })
      .eq('id', id)
      .eq('captain_id', uid)
      .eq('status', 'in_progress')
      .select('*')
      .maybeSingle();
    if (error) {
      return res.status(500).json({ error: error.message });
    }
    if (!data) {
      return res.status(409).json({ error: 'Cannot complete' });
    }
    emitRide(io, data);
    return res.json({ ride: data });
  });

  /** Captain: past completed jobs. */
  app.get('/api/rides/captain-history', async (req, res) => {
    const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
    if (!token || !supabase) {
      return res.status(400).json({ error: 'Missing Authorization' });
    }
    const uid = await getUserIdFromAccessToken(token);
    if (!uid) {
      return res.status(401).json({ error: 'Invalid token' });
    }
    const profile = await getProfile(supabase, uid);
    if (!profile || profile.role !== 'captain') {
      return res.status(403).json({ error: 'Captains only' });
    }
    const { data, error } = await supabase
      .from('ride_requests')
      .select('*')
      .eq('captain_id', uid)
      .eq('status', 'completed')
      .order('updated_at', { ascending: false })
      .limit(50);
    if (error) {
      return res.status(500).json({ error: error.message });
    }
    return res.json({ rides: data ?? [] });
  });

  /** Customer (or captain on job): captain live location + ETA to pickup. */
  app.get('/api/rides/:id/tracking', async (req, res) => {
    const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
    if (!token || !supabase) {
      return res.status(400).json({ error: 'Missing Authorization' });
    }
    const uid = await getUserIdFromAccessToken(token);
    if (!uid) {
      return res.status(401).json({ error: 'Invalid token' });
    }
    const id = req.params.id;
    const { data: row, error } = await supabase.from('ride_requests').select('*').eq('id', id).maybeSingle();
    if (error) {
      return res.status(500).json({ error: error.message });
    }
    if (!row) {
      return res.status(404).json({ error: 'Not found' });
    }
    if (row.customer_id !== uid && row.captain_id !== uid) {
      return res.status(403).json({ error: 'Forbidden' });
    }
    if (!row.captain_id || !['accepted', 'in_progress'].includes(row.status)) {
      const r0 = { ...row };
      if (uid === row.captain_id) {
        delete r0.handshake_pin;
      }
      return res.json({
        ride: r0,
        captain_lat: null,
        captain_lng: null,
        eta_minutes_to_pickup: null,
        eta_minutes_to_drop: null,
      });
    }
    const { data: pres } = await supabase
      .from('captain_presence')
      .select('lat, lng, updated_at')
      .eq('driver_id', row.captain_id)
      .maybeSingle();
    let captain_lat = pres?.lat ?? null;
    let captain_lng = pres?.lng ?? null;
    let etaPickup = null;
    let etaDrop = null;
    if (captain_lat != null && captain_lng != null && row.pickup_lat != null && row.pickup_lng != null) {
      if (row.status === 'accepted') {
        const km = haversineKm(captain_lat, captain_lng, row.pickup_lat, row.pickup_lng);
        etaPickup = Math.max(1, Math.ceil((km / CAPTAIN_ETA_SPEED_KMH) * 60));
      }
    }
    if (
      captain_lat != null &&
      captain_lng != null &&
      row.status === 'in_progress' &&
      row.drop_lat != null &&
      row.drop_lng != null
    ) {
      const kmDrop = haversineKm(captain_lat, captain_lng, row.drop_lat, row.drop_lng);
      etaDrop = Math.max(1, Math.ceil((kmDrop / CAPTAIN_ETA_SPEED_KMH) * 60));
    }
    const rOut = { ...row };
    if (uid === row.captain_id) {
      delete rOut.handshake_pin;
    }
    return res.json({
      ride: rOut,
      captain_lat,
      captain_lng,
      eta_minutes_to_pickup: etaPickup,
      eta_minutes_to_drop: etaDrop,
    });
  });

  /** Single ride by id — after all static /api/rides/... paths. */
  app.get('/api/rides/:id', async (req, res) => {
    const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
    if (!token || !supabase) {
      return res.status(400).json({ error: 'Missing Authorization' });
    }
    const uid = await getUserIdFromAccessToken(token);
    if (!uid) {
      return res.status(401).json({ error: 'Invalid token' });
    }
    const profile = await getProfile(supabase, uid);
    if (!profile) {
      return res.status(403).json({ error: 'Forbidden' });
    }
    const id = req.params.id;
    const { data: row, error } = await supabase.from('ride_requests').select('*').eq('id', id).maybeSingle();
    if (error) {
      return res.status(500).json({ error: error.message });
    }
    if (!row) {
      return res.status(404).json({ error: 'Not found' });
    }
    const ok =
      profile.role === 'admin' ||
      row.customer_id === uid ||
      row.captain_id === uid ||
      (profile.role === 'captain' && row.status === 'pending');
    if (!ok) {
      return res.status(403).json({ error: 'Forbidden' });
    }
    const out = { ...row };
    if (profile.role === 'captain' && row.captain_id === uid) {
      delete out.handshake_pin;
    }
    return res.json({ ride: out });
  });

  /** Customer: cancel pending ride. */
  app.post('/api/rides/:id/cancel', async (req, res) => {
    const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
    if (!token || !supabase) {
      return res.status(400).json({ error: 'Missing Authorization' });
    }
    const uid = await getUserIdFromAccessToken(token);
    if (!uid) {
      return res.status(401).json({ error: 'Invalid token' });
    }
    const profile = await getProfile(supabase, uid);
    if (!profile || profile.role !== 'user') {
      return res.status(403).json({ error: 'Customers only' });
    }
    const id = req.params.id;
    const { data, error } = await supabase
      .from('ride_requests')
      .update({ status: 'cancelled', updated_at: new Date().toISOString() })
      .eq('id', id)
      .eq('customer_id', uid)
      .eq('status', 'pending')
      .select('*')
      .maybeSingle();
    if (error) {
      return res.status(500).json({ error: error.message });
    }
    if (!data) {
      return res.status(409).json({ error: 'Cannot cancel' });
    }
    emitRide(io, data);
    return res.json({ ride: data });
  });
}
