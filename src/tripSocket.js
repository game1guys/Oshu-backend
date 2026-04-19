/**
 * Socket.IO: real-time captain location per trip. Rooms: trip:{tripId}, driver:{userId}
 * Ephemeral ride chat: ride_chat:{rideRequestId} — not stored in DB; 10 msgs per customer + 10 per captain.
 */

const RIDE_CHAT_MAX_CUSTOMER = 10;
const RIDE_CHAT_MAX_CAPTAIN = 10;
const RIDE_CHAT_MAX_LEN = 800;

/** @type {Map<string, { customer: number; captain: number }>} */
const rideChatCounts = new Map();

function rideChatKey(rideId) {
  return String(rideId ?? '');
}

function getRideChatCounts(rideId) {
  const k = rideChatKey(rideId);
  if (!rideChatCounts.has(k)) {
    rideChatCounts.set(k, { customer: 0, captain: 0 });
  }
  return rideChatCounts.get(k);
}

async function assertRideChatParticipant(supabase, rideId, userId) {
  const { data: ride, error } = await supabase
    .from('ride_requests')
    .select('id, status, customer_id, captain_id')
    .eq('id', rideId)
    .maybeSingle();
  if (error || !ride) {
    return { ok: false, error: 'ride not found' };
  }
  /** Chat only before trip starts (PIN / in_progress). */
  if (ride.status !== 'accepted') {
    return { ok: false, error: 'chat only while waiting to start the trip' };
  }
  if (ride.customer_id === userId) {
    return { ok: true, role: 'customer' };
  }
  if (ride.captain_id === userId) {
    return { ok: true, role: 'captain' };
  }
  return { ok: false, error: 'forbidden' };
}

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

    socket.on('join_ride_chat', async (payload, cb) => {
      const rideId = payload?.rideId;
      if (!rideId || typeof rideId !== 'string') {
        if (typeof cb === 'function') {
          cb({ ok: false, error: 'rideId required' });
        }
        return;
      }
      if (!supabase) {
        if (typeof cb === 'function') {
          cb({ ok: false, error: 'server unavailable' });
        }
        return;
      }
      const gate = await assertRideChatParticipant(supabase, rideId, userId);
      if (!gate.ok) {
        if (typeof cb === 'function') {
          cb({ ok: false, error: gate.error });
        }
        return;
      }
      const c = getRideChatCounts(rideId);
      socket.join(`ride_chat:${rideId}`);
      if (typeof cb === 'function') {
        cb({
          ok: true,
          customerSent: c.customer,
          captainSent: c.captain,
          customerMax: RIDE_CHAT_MAX_CUSTOMER,
          captainMax: RIDE_CHAT_MAX_CAPTAIN,
        });
      }
    });

    socket.on('leave_ride_chat', payload => {
      const rideId = payload?.rideId;
      if (rideId && typeof rideId === 'string') {
        socket.leave(`ride_chat:${rideId}`);
      }
    });

    socket.on('ride_chat_send', async (payload, cb) => {
      const rideId = payload?.rideId;
      const textRaw = payload?.text;
      if (!rideId || typeof rideId !== 'string') {
        if (typeof cb === 'function') {
          cb({ ok: false, error: 'rideId required' });
        }
        return;
      }
      if (!supabase) {
        if (typeof cb === 'function') {
          cb({ ok: false, error: 'server unavailable' });
        }
        return;
      }
      const gate = await assertRideChatParticipant(supabase, rideId, userId);
      if (!gate.ok) {
        if (typeof cb === 'function') {
          cb({ ok: false, error: gate.error });
        }
        return;
      }
      const text = String(textRaw ?? '')
        .trim()
        .replace(/\s+/g, ' ');
      if (!text) {
        if (typeof cb === 'function') {
          cb({ ok: false, error: 'empty message' });
        }
        return;
      }
      if (text.length > RIDE_CHAT_MAX_LEN) {
        if (typeof cb === 'function') {
          cb({ ok: false, error: 'message too long' });
        }
        return;
      }
      const counts = getRideChatCounts(rideId);
      const side = gate.role;
      if (side === 'customer' && counts.customer >= RIDE_CHAT_MAX_CUSTOMER) {
        if (typeof cb === 'function') {
          cb({
            ok: false,
            error: 'customer limit',
            customerSent: counts.customer,
            captainSent: counts.captain,
          });
        }
        return;
      }
      if (side === 'captain' && counts.captain >= RIDE_CHAT_MAX_CAPTAIN) {
        if (typeof cb === 'function') {
          cb({
            ok: false,
            error: 'captain limit',
            customerSent: counts.customer,
            captainSent: counts.captain,
          });
        }
        return;
      }
      if (side === 'customer') {
        counts.customer += 1;
      } else {
        counts.captain += 1;
      }
      const sentAt = new Date().toISOString();
      io.to(`ride_chat:${rideId}`).emit('ride_chat_message', {
        rideId,
        from: side,
        text,
        sentAt,
        customerSent: counts.customer,
        captainSent: counts.captain,
      });
      if (typeof cb === 'function') {
        cb({
          ok: true,
          customerSent: counts.customer,
          captainSent: counts.captain,
        });
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
