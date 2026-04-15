/**
 * Saved Addresses API
 *
 * GET    /api/addresses          – list current user's saved addresses
 * POST   /api/addresses          – upsert (create or replace) a saved address
 * DELETE /api/addresses/:id      – delete one saved address
 */

export function registerAddressRoutes(app, { supabase, getUserIdFromAccessToken }) {

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
