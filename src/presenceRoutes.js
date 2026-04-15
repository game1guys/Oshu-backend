/**
 * Captain presence (lat/lng) for customer nearby map; Node uses service-role Supabase.
 */

import { haversineKm } from './geo.js';

async function getProfile(supabase, userId) {
  const { data, error } = await supabase.from('profiles').select('id, role, full_name').eq('id', userId).maybeSingle();
  if (error) {
    return null;
  }
  return data;
}

export function registerPresenceRoutes(app, { supabase, getUserIdFromAccessToken }) {
  /** Captain: upsert last position + availability (when partner is online for allocation). */
  app.post('/api/captain/presence', async (req, res) => {
    const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
    if (!token) {
      return res.status(400).json({ error: 'Missing Authorization' });
    }
    if (!supabase) {
      return res.status(503).json({ error: 'Database not configured' });
    }
    const uid = await getUserIdFromAccessToken(token);
    if (!uid) {
      return res.status(401).json({ error: 'Invalid token' });
    }
    const profile = await getProfile(supabase, uid);
    if (!profile || profile.role !== 'captain') {
      return res.status(403).json({ error: 'Captains only' });
    }
    const b = req.body ?? {};
    const lat = typeof b.lat === 'number' ? b.lat : NaN;
    const lng = typeof b.lng === 'number' ? b.lng : NaN;
    const is_available = typeof b.is_available === 'boolean' ? b.is_available : true;
    if (Number.isNaN(lat) || Number.isNaN(lng) || lat < -90 || lat > 90 || lng < -180 || lng > 180) {
      return res.status(400).json({ error: 'Invalid lat/lng' });
    }
    const { data, error } = await supabase
      .from('captain_presence')
      .upsert(
        {
          driver_id: uid,
          lat,
          lng,
          is_available,
          updated_at: new Date().toISOString(),
        },
        { onConflict: 'driver_id' },
      )
      .select('*')
      .single();
    if (error) {
      return res.status(500).json({ error: error.message });
    }
    return res.json({ presence: data });
  });

  /** Customer: partners within radius (km) with recent heartbeat. */
  app.get('/api/nearby-captains', async (req, res) => {
    const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
    if (!token) {
      return res.status(400).json({ error: 'Missing Authorization' });
    }
    if (!supabase) {
      return res.status(503).json({ error: 'Database not configured' });
    }
    const uid = await getUserIdFromAccessToken(token);
    if (!uid) {
      return res.status(401).json({ error: 'Invalid token' });
    }
    const profile = await getProfile(supabase, uid);
    if (!profile || profile.role !== 'user') {
      return res.status(403).json({ error: 'Customers only' });
    }
    const qLat = parseFloat(String(req.query.lat ?? ''));
    const qLng = parseFloat(String(req.query.lng ?? ''));
    const radiusKm = Math.min(50, Math.max(1, parseFloat(String(req.query.radius_km ?? '15')) || 15));
    if (Number.isNaN(qLat) || Number.isNaN(qLng)) {
      return res.status(400).json({ error: 'Query lat and lng required' });
    }

    const staleBefore = new Date(Date.now() - 20 * 60 * 1000).toISOString();
    const { data: rows, error } = await supabase
      .from('captain_presence')
      .select('driver_id, lat, lng, is_available, updated_at')
      .eq('is_available', true)
      .gte('updated_at', staleBefore);
    if (error) {
      return res.status(500).json({ error: error.message });
    }

    const list = [];
    for (const row of rows ?? []) {
      const d = haversineKm(qLat, qLng, row.lat, row.lng);
      if (d <= radiusKm) {
        const { data: v } = await supabase
          .from('vehicles')
          .select('type, registration_number')
          .eq('driver_id', row.driver_id)
          .maybeSingle();
        const { data: p } = await supabase
          .from('profiles')
          .select('full_name, captain_kyc_status')
          .eq('id', row.driver_id)
          .maybeSingle();
        list.push({
          driver_id: row.driver_id,
          lat: row.lat,
          lng: row.lng,
          distance_km: Math.round(d * 10) / 10,
          updated_at: row.updated_at,
          vehicle_type: v?.type ?? null,
          registration_number: v?.registration_number ?? null,
          partner_name: p?.full_name ?? 'Partner',
          kyc_status: p?.captain_kyc_status ?? null,
        });
      }
    }
    list.sort((a, b) => a.distance_km - b.distance_km);
    return res.json({ captains: list });
  });
}
