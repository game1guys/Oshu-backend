/**
 * Socket.IO: real-time captain location per trip. Rooms: trip:{tripId}, driver:{userId}
 */

export function attachTripSocket(io, { supabase, getUserIdFromAccessToken }) {
  io.use(async (socket, next) => {
    try {
      const token = socket.handshake.auth?.token;
      if (!token || typeof token !== 'string') {
        return next(new Error('Unauthorized'));
      }
      const uid = await getUserIdFromAccessToken(token);
      if (!uid) {
        return next(new Error('Unauthorized'));
      }
      socket.data.userId = uid;
      return next();
    } catch {
      return next(new Error('Unauthorized'));
    }
  });

  io.on('connection', socket => {
    const userId = socket.data.userId;
    socket.join(`driver:${userId}`);

    socket.on('join_trip', async (payload, cb) => {
      const tripId = payload?.tripId;
      if (!tripId || typeof tripId !== 'string') {
        if (typeof cb === 'function') {
          cb({ ok: false, error: 'tripId required' });
        }
        return;
      }
      if (!supabase) {
        if (typeof cb === 'function') {
          cb({ ok: false, error: 'server unavailable' });
        }
        return;
      }
      const { data: trip, error } = await supabase
        .from('trips')
        .select('driver_id')
        .eq('id', tripId)
        .maybeSingle();
      if (error || !trip || trip.driver_id !== userId) {
        if (typeof cb === 'function') {
          cb({ ok: false, error: 'forbidden' });
        }
        return;
      }
      socket.join(`trip:${tripId}`);
      if (typeof cb === 'function') {
        cb({ ok: true });
      }
    });

    socket.on('location', async payload => {
      const tripId = payload?.tripId;
      const lat = payload?.lat;
      const lng = payload?.lng;
      const heading = payload?.heading;
      if (!tripId || typeof lat !== 'number' || typeof lng !== 'number') {
        return;
      }
      if (!supabase) {
        return;
      }
      const { data: trip, error } = await supabase
        .from('trips')
        .select('driver_id, status')
        .eq('id', tripId)
        .maybeSingle();
      if (error || !trip || trip.driver_id !== userId) {
        return;
      }
      if (trip.status === 'completed' || trip.status === 'cancelled') {
        return;
      }
      const at = new Date().toISOString();
      const { error: upErr } = await supabase
        .from('trips')
        .update({
          current_lat: lat,
          current_lng: lng,
          last_location_at: at,
          status: 'in_progress',
          updated_at: at,
        })
        .eq('id', tripId);
      if (upErr) {
        return;
      }
      io.to(`trip:${tripId}`).emit('trip:location', {
        tripId,
        lat,
        lng,
        heading: typeof heading === 'number' ? heading : null,
        at,
        driverId: userId,
      });
    });

    socket.on('disconnect', () => {
      socket.leave(`driver:${userId}`);
    });
  });
}
