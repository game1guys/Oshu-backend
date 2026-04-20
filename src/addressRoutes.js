/**
 * Saved Addresses API
 *
 * GET    /api/addresses          – list current user's saved addresses
 * POST   /api/addresses          – upsert (create or replace) a saved address
 * DELETE /api/addresses/:id      – delete one saved address
 */

export function registerAddressRoutes(app, { supabase, getUserIdFromAccessToken }) {
  /**
   * GET /api/location/search?q=...&lat=..&lng=..&country=IN
   * Public endpoint for app location suggestions.
   * Tries Google Places first (if key is configured), falls back to OSM Nominatim.
   */
  app.get('/api/location/search', async (req, res) => {
    const q = String(req.query?.q ?? '').trim();
    if (q.length < 2) {
      return res.json({ suggestions: [] });
    }
    const lat = Number(req.query?.lat);
    const lng = Number(req.query?.lng);
    const hasNear = Number.isFinite(lat) && Number.isFinite(lng);
    const country = String(req.query?.country ?? 'IN').trim().toUpperCase();
    const mapsKey = process.env.GOOGLE_MAPS_API_KEY;
    const qLower = q.toLowerCase();

    const toSuggestion = (id, label, la, lo) => ({
      id: String(id),
      label: String(label),
      lat: Number(la),
      lng: Number(lo),
    });

    const distanceKm = (lat1, lng1, lat2, lng2) => {
      const toRad = v => (v * Math.PI) / 180;
      const dLat = toRad(lat2 - lat1);
      const dLng = toRad(lng2 - lng1);
      const a =
        Math.sin(dLat / 2) * Math.sin(dLat / 2) +
        Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLng / 2) * Math.sin(dLng / 2);
      return 6371 * (2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)));
    };

    const dedupe = rows => {
      const out = [];
      const seen = new Set();
      for (const r of rows) {
        if (!Number.isFinite(r.lat) || !Number.isFinite(r.lng) || !r.label) continue;
        const k = `${r.lat.toFixed(6)}:${r.lng.toFixed(6)}`;
        if (seen.has(k)) continue;
        seen.add(k);
        out.push(r);
      }
      return out;
    };

    const rankRows = rows =>
      rows
        .map(r => {
          const label = String(r.label ?? '');
          const labelLower = label.toLowerCase();
          const starts = labelLower.startsWith(qLower) ? 1 : 0;
          const includes = labelLower.includes(qLower) ? 1 : 0;
          const near =
            hasNear && Number.isFinite(r.lat) && Number.isFinite(r.lng)
              ? distanceKm(lat, lng, Number(r.lat), Number(r.lng))
              : 999999;
          return {
            ...r,
            _scoreStarts: starts,
            _scoreIncludes: includes,
            _scoreNear: near,
          };
        })
        .sort((a, b) => {
          if (b._scoreStarts !== a._scoreStarts) return b._scoreStarts - a._scoreStarts;
          if (b._scoreIncludes !== a._scoreIncludes) return b._scoreIncludes - a._scoreIncludes;
          return a._scoreNear - b._scoreNear;
        })
        .map(({ _scoreStarts, _scoreIncludes, _scoreNear, ...rest }) => rest);

    // 1) Google Places text search (best for landmarks like colleges).
    if (mapsKey) {
      try {
        const g = new URLSearchParams({
          query: `${q}, India`,
          key: mapsKey,
          language: 'en',
          region: country.toLowerCase(),
        });
        g.set('type', 'establishment');
        if (hasNear) {
          g.set('location', `${lat},${lng}`);
          g.set('radius', '30000');
        }
        const gUrl = `https://maps.googleapis.com/maps/api/place/textsearch/json?${g.toString()}`;
        const gr = await fetch(gUrl);
        if (gr.ok) {
          const gj = await gr.json();
          const arr = Array.isArray(gj?.results) ? gj.results : [];
          const googleRows = rankRows(
            dedupe(
              arr
                .filter(x => String(x?.formatted_address ?? x?.name ?? '').toLowerCase().includes('india'))
                .map((x, i) =>
                  toSuggestion(
                    x?.place_id ?? `g-${i}`,
                    x?.formatted_address ?? x?.name ?? '',
                    x?.geometry?.location?.lat,
                    x?.geometry?.location?.lng,
                  ),
                ),
            ),
          ).slice(0, 12);
          if (googleRows.length > 0) {
            return res.json({ suggestions: googleRows, provider: 'google' });
          }
        }
      } catch {
        // Continue to fallback.
      }
    }

    // 1b) Google Place Autocomplete fallback (great for micro/local names).
    if (mapsKey) {
      try {
        const ap = new URLSearchParams({
          input: q,
          key: mapsKey,
          language: 'en',
          components: `country:${country.toLowerCase()}`,
        });
        if (hasNear) {
          ap.set('location', `${lat},${lng}`);
          ap.set('radius', '30000');
          ap.set('strictbounds', 'true');
        }
        const apUrl = `https://maps.googleapis.com/maps/api/place/autocomplete/json?${ap.toString()}`;
        const apr = await fetch(apUrl);
        if (apr.ok) {
          const apj = await apr.json();
          const preds = Array.isArray(apj?.predictions) ? apj.predictions : [];
          const placeIds = preds.map(p => p?.place_id).filter(Boolean).slice(0, 8);
          if (placeIds.length > 0) {
            const details = await Promise.all(
              placeIds.map(async pid => {
                try {
                  const dp = new URLSearchParams({
                    place_id: String(pid),
                    key: mapsKey,
                    fields: 'place_id,name,formatted_address,geometry/location',
                  });
                  const dr = await fetch(`https://maps.googleapis.com/maps/api/place/details/json?${dp.toString()}`);
                  if (!dr.ok) return null;
                  const dj = await dr.json();
                  const r = dj?.result;
                  if (!r?.geometry?.location) return null;
                  const label = String(r?.formatted_address ?? r?.name ?? '');
                  if (!label.toLowerCase().includes('india')) return null;
                  return toSuggestion(
                    r?.place_id ?? `gd-${pid}`,
                    label,
                    r?.geometry?.location?.lat,
                    r?.geometry?.location?.lng,
                  );
                } catch {
                  return null;
                }
              }),
            );
            const rows = rankRows(dedupe(details.filter(Boolean))).slice(0, 12);
            if (rows.length > 0) {
              return res.json({ suggestions: rows, provider: 'google-autocomplete' });
            }
          }
        }
      } catch {
        // Continue to Nominatim fallback.
      }
    }

    // 2) Fallback: Nominatim
    try {
      const p = new URLSearchParams({
        format: 'json',
        q,
        limit: '12',
        addressdetails: '1',
      });
      if (country) {
        p.set('countrycodes', country.toLowerCase());
      }
      if (hasNear) {
        p.set('lat', String(lat));
        p.set('lon', String(lng));
      }
      const nr = await fetch(`https://nominatim.openstreetmap.org/search?${p.toString()}`, {
        headers: { 'User-Agent': 'OshuBackend/1.0 (support@oshu.local)' },
      });
      if (!nr.ok) {
        return res.json({ suggestions: [] });
      }
      const nj = await nr.json();
      const rows = rankRows(
        dedupe(
        (Array.isArray(nj) ? nj : []).map((x, i) =>
          toSuggestion(x?.place_id ?? `n-${i}`, x?.display_name ?? '', Number(x?.lat), Number(x?.lon)),
        ),
      ),
      ).slice(0, 12);
      return res.json({ suggestions: rows, provider: 'nominatim' });
    } catch {
      return res.json({ suggestions: [] });
    }
  });

  /** GET /api/addresses */
  app.get('/api/addresses', async (req, res) => {
    const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
    if (!token || !supabase) return res.status(400).json({ error: 'Missing Authorization' });

    const uid = await getUserIdFromAccessToken(token);
    if (!uid) return res.status(401).json({ error: 'Invalid token' });

    const { data, error } = await supabase
      .from('saved_addresses')
      .select('id, label, name, address, lat, lng, created_at')
      .eq('user_id', uid)
      .order('created_at', { ascending: true });

    if (error) return res.status(500).json({ error: error.message });
    return res.json({ addresses: data ?? [] });
  });

  /**
   * POST /api/addresses
   * Body: { label: 'Home'|'Shop'|'Other', name?: string, address: string, lat: number, lng: number }
   * Upserts by (user_id, label) — so saving Home again replaces the old one.
   */
  app.post('/api/addresses', async (req, res) => {
    const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
    if (!token || !supabase) return res.status(400).json({ error: 'Missing Authorization' });

    const uid = await getUserIdFromAccessToken(token);
    if (!uid) return res.status(401).json({ error: 'Invalid token' });

    const { label, name, address, lat, lng } = req.body ?? {};
    const allowed = ['Home', 'Shop', 'Other'];
    if (!allowed.includes(label))  return res.status(400).json({ error: 'label must be Home, Shop, or Other' });
    if (!address?.trim())          return res.status(400).json({ error: 'address is required' });
    if (typeof lat !== 'number' || typeof lng !== 'number')
      return res.status(400).json({ error: 'lat and lng (numbers) are required' });

    const { data, error } = await supabase
      .from('saved_addresses')
      .upsert(
        { user_id: uid, label, name: name?.trim() || null, address: address.trim(), lat, lng },
        { onConflict: 'user_id,label' },
      )
      .select('id, label, name, address, lat, lng, created_at')
      .maybeSingle();

    if (error) return res.status(500).json({ error: error.message });
    return res.status(201).json({ ok: true, address: data });
  });

  /** DELETE /api/addresses/:id */
  app.delete('/api/addresses/:id', async (req, res) => {
    const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
    if (!token || !supabase) return res.status(400).json({ error: 'Missing Authorization' });

    const uid = await getUserIdFromAccessToken(token);
    if (!uid) return res.status(401).json({ error: 'Invalid token' });

    const { error } = await supabase
      .from('saved_addresses')
      .delete()
      .eq('id', req.params.id)
      .eq('user_id', uid);   // ensures users can only delete their own

    if (error) return res.status(500).json({ error: error.message });
    return res.json({ ok: true });
  });
}
