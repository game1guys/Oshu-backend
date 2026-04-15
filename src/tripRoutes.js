/**
 * REST: active trip for captain. Location updates prefer WebSocket (tripSocket.js).
 */

export function registerTripRoutes(app, { supabase, getUserIdFromAccessToken }) {
  app.get('/api/trips/active-captain', async (req, res) => {
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
    const { data, error } = await supabase
      .from('trips')
      .select('*')
      .eq('driver_id', uid)
      .in('status', ['assigned', 'in_progress'])
      .order('created_at', { ascending: false })
      .limit(1)
      .maybeSingle();
    if (error) {
      return res.status(500).json({ error: error.message });
    }
    return res.json({ trip: data ?? null });
  });
}
