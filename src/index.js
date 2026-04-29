import 'dotenv/config';
import cors from 'cors';
import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import { createClient } from '@supabase/supabase-js';
import { registerKycRoutes } from './kycRoutes.js';
import { registerTripRoutes } from './tripRoutes.js';
import { registerPresenceRoutes } from './presenceRoutes.js';
import { registerRideRoutes } from './rideRoutes.js';
import { registerCoinRoutes } from './coinRoutes.js';
import { registerAdminRoutes } from './adminRoutes.js';
import { registerAddressRoutes } from './addressRoutes.js';
import { registerReferralRoutes } from './referralRoutes.js';
import { registerPaymentRoutes, registerRazorpayWebhook } from './paymentRoutes.js';
import { registerCaptainWalletRoutes } from './captainWalletRoutes.js';
import { registerAuthBootstrapRoutes } from './authBootstrapRoutes.js';
import { attachTripSocket } from './tripSocket.js';

const app = express();
const port = Number(process.env.PORT) || 3001;

const supabaseUrl = process.env.SUPABASE_URL;
const serviceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
const anonKey = process.env.SUPABASE_ANON_KEY;

if (!supabaseUrl || !serviceKey) {
  console.warn(
    '[oshu-backend] Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY — set them in .env',
  );
}
if (!anonKey) {
  console.warn('[oshu-backend] Missing SUPABASE_ANON_KEY — dev OTP bypass cannot complete sessions');
}

const supabase =
  supabaseUrl && serviceKey
    ? createClient(supabaseUrl, serviceKey, {
        auth: { persistSession: false, autoRefreshToken: false },
      })
    : null;

async function seedAdminUser() {
  if (!supabase) return;
  const email = 'admin@oshu.in';
  const password = 'Test@123';
  
  console.log(`[seed] Ensuring admin user: ${email}`);
  
  const { data: userData, error: userError } = await supabase.auth.admin.createUser({
    email,
    password,
    email_confirm: true,
    user_metadata: { full_name: 'Oshu Admin' }
  });

  let userId = userData?.user?.id;

  if (userError) {
    if (userError.message.includes('already registered') || userError.message.includes('already exists')) {
      const { data: users } = await supabase.auth.admin.listUsers();
      const existingUser = users?.users?.find(u => u.email === email);
      if (existingUser) {
        userId = existingUser.id;
        await supabase.auth.admin.updateUserById(userId, { password });
      }
    } else {
      console.error('[seed] Error creating admin:', userError.message);
    }
  }

  if (userId) {
    await supabase.from('profiles').upsert({
      id: userId,
      role: 'admin',
      full_name: 'Oshu Admin'
    });
    console.log('[seed] Admin profile ensured.');
  }
}

seedAdminUser();

/** User JWT validation — same as phone-session (anon client). Service-role getUser(jwt) can fail for user access tokens. */
const userAuthClient =
  supabaseUrl && anonKey
    ? createClient(supabaseUrl, anonKey, {
        auth: { persistSession: false, autoRefreshToken: false },
      })
    : null;

function subFromAccessToken(token) {
  try {
    const mid = token.split('.')[1];
    if (!mid) {
      return null;
    }
    const pad = mid.length % 4 === 0 ? '' : '='.repeat(4 - (mid.length % 4));
    const b64 = mid.replace(/-/g, '+').replace(/_/g, '/') + pad;
    const json = Buffer.from(b64, 'base64').toString('utf8');
    const payload = JSON.parse(json);
    return typeof payload.sub === 'string' ? payload.sub : null;
  } catch {
    return null;
  }
}

/** Resolve auth user id from access token (anon getUser, then service, then JWT sub). */
async function getUserIdFromAccessToken(token) {
  if (userAuthClient) {
    const { data, error } = await userAuthClient.auth.getUser(token);
    if (!error && data?.user?.id) {
      return data.user.id;
    }
  }
  if (supabase) {
    const { data, error } = await supabase.auth.getUser(token);
    if (!error && data?.user?.id) {
      return data.user.id;
    }
  }
  return subFromAccessToken(token);
}

app.use(cors());
/** Razorpay webhook must see raw body — register before express.json(). */
registerRazorpayWebhook(app, supabase);
app.use(express.json({ limit: '50mb' }));

/**
 * Captain single-device guard:
 * if a captain logs in on a new device, old devices are rejected on subsequent API calls.
 */
app.use('/api', async (req, res, next) => {
  try {
    const p = String(req.path ?? '');
    if (
      p.startsWith('/dev/') ||
      p === '/health' ||
      p === '/db/health' ||
      p === '/auth/bootstrap' ||
      p === '/auth/device-login' ||
      p.startsWith('/webhooks/')
    ) {
      return next();
    }
    const auth = req.headers.authorization;
    const token = typeof auth === 'string' ? auth.replace(/^Bearer\s+/i, '').trim() : '';
    if (!token || !supabase) {
      return next();
    }
    const uid = await getUserIdFromAccessToken(token);
    if (!uid) {
      return next();
    }
    const { data: profile } = await supabase
      .from('profiles')
      .select('role, active_device_id')
      .eq('id', uid)
      .maybeSingle();
    if (!profile || profile.role !== 'captain') {
      return next();
    }
    const activeDeviceId = String(profile.active_device_id ?? '').trim();
    if (!activeDeviceId) {
      return next();
    }
    const reqDeviceId = String(req.headers['x-oshu-device-id'] ?? '').trim();
    if (!reqDeviceId || reqDeviceId !== activeDeviceId) {
      res.setHeader('X-Oshu-Force-Logout', '1');
      return res.status(401).json({
        error: 'Session moved to another device. Please login again on this device.',
        code: 'captain_session_replaced',
      });
    }
    return next();
  } catch {
    return next();
  }
});

registerKycRoutes(app, { supabase, getUserIdFromAccessToken });
registerTripRoutes(app, { supabase, getUserIdFromAccessToken });
registerPresenceRoutes(app, { supabase, getUserIdFromAccessToken });
registerCoinRoutes(app, { supabase, getUserIdFromAccessToken });
registerAdminRoutes(app, { supabase, getUserIdFromAccessToken });
registerAddressRoutes(app, { supabase, getUserIdFromAccessToken });
registerReferralRoutes(app, { supabase, getUserIdFromAccessToken });
registerPaymentRoutes(app, { supabase, getUserIdFromAccessToken });
registerCaptainWalletRoutes(app, { supabase, getUserIdFromAccessToken });
registerAuthBootstrapRoutes(app, { supabase, getUserIdFromAccessToken });

/** Whether dev OTP bypass is on — helps verify the running process picked up `.env` after deploy/restart. */
function healthPayload() {
  return {
    ok: true,
    service: 'oshu-backend',
    uptime_s: Math.round(process.uptime()),
    now: new Date().toISOString(),
    dev_otp_bypass_enabled: process.env.ALLOW_DEV_OTP_BYPASS === 'true',
  };
}

app.get('/health', (_req, res) => {
  res.json(healthPayload());
});

// Alias (some reverse proxies only forward /api/*).
app.get('/api/health', (_req, res) => {
  res.json(healthPayload());
});

app.get('/db/health', async (_req, res) => {
  if (!supabase) {
    return res.status(503).json({ ok: false, error: 'Supabase not configured' });
  }
  const { error } = await supabase.from('profiles').select('id').limit(1);
  if (error) {
    return res.status(503).json({ ok: false, error: error.message });
  }
  return res.json({ ok: true, supabase: true });
});

/** Dev-only routes: mounted at `/dev/*` and `/api/dev/*` so reverse proxies that only forward `/api/*` still work. */
const devRouter = express.Router();

/**
 * Dev only (__DEV__ app): mint session on the server so the Android emulator never calls
 * Supabase Auth over HTTPS (RN often throws "Network request failed" on verifyOtp).
 * Returns access_token + refresh_token for supabase.auth.setSession() on the client.
 * Requires ALLOW_DEV_OTP_BYPASS=true and SUPABASE_ANON_KEY in .env
 */
devRouter.post('/phone-session', async (req, res) => {
  if (process.env.ALLOW_DEV_OTP_BYPASS !== 'true') {
    return res.status(404).json({ error: 'not found' });
  }
  const phone = req.body?.phone;
  if (!phone || typeof phone !== 'string' || !supabase || !supabaseUrl || !anonKey) {
    return res.status(400).json({ error: 'Invalid request or server missing Supabase keys' });
  }
  const digits = phone.replace(/\D/g, '');
  const email = `dev${digits}@oshu-local.test`;
  const { data: linkData, error: linkErr } = await supabase.auth.admin.generateLink({
    type: 'magiclink',
    email,
  });
  const otp = linkData?.properties?.email_otp;
  if (linkErr || !otp) {
    return res.status(500).json({ error: linkErr?.message ?? 'generateLink failed' });
  }

  const userClient = createClient(supabaseUrl, anonKey, {
    auth: { persistSession: false, autoRefreshToken: false },
  });
  const { data: authData, error: vErr } = await userClient.auth.verifyOtp({
    email,
    token: otp,
    type: 'email',
  });
  if (vErr || !authData?.session) {
    return res.status(500).json({ error: vErr?.message ?? 'verifyOtp failed on server' });
  }
  const uid = authData.session.user.id;
  try {
    await supabase.from('profiles').update({ phone }).eq('id', uid);
  } catch (e) {
    console.warn('[oshu-backend] dev phone-session: profile phone update failed', e?.message ?? e);
  }
  /** Full session — RN Android can persist this without calling supabase.auth.setSession (which always hits _getUser). */
  return res.json({ session: authData.session });
});

/**
 * Dev: load profile + vehicle from the server so the RN emulator never calls Supabase REST (often "Network request failed").
 */
devRouter.get('/profile', async (req, res) => {
  if (process.env.ALLOW_DEV_OTP_BYPASS !== 'true' || !supabase) {
    return res.status(404).json({ error: 'not found' });
  }
  const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
  if (!token) {
    return res.status(400).json({ error: 'Missing Authorization' });
  }
  const uid = await getUserIdFromAccessToken(token);
  if (!uid) {
    return res.status(401).json({ error: 'Invalid token' });
  }
  const { data: profile, error: pErr } = await supabase
    .from('profiles')
    .select('*')
    .eq('id', uid)
    .maybeSingle();
  if (pErr) {
    return res.status(500).json({ error: pErr.message });
  }
  if (!profile) {
    return res.json({ profile: null, vehicle: null });
  }
  let vehicle = null;
  if (profile.role === 'captain') {
    const { data: v } = await supabase.from('vehicles').select('*').eq('driver_id', uid).maybeSingle();
    vehicle = v ?? null;
  }
  return res.json({ profile, vehicle });
});

/**
 * Dev: set role from the server (same reason as GET /dev/profile).
 */
devRouter.post('/set-role', async (req, res) => {
  if (process.env.ALLOW_DEV_OTP_BYPASS !== 'true' || !supabase) {
    return res.status(404).json({ error: 'not found' });
  }
  const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
  const role = req.body?.role;
  const allowed = ['user', 'captain', 'admin'];
  if (!token || typeof role !== 'string' || !allowed.includes(role)) {
    return res.status(400).json({ error: 'Invalid request' });
  }
  const uid = await getUserIdFromAccessToken(token);
  if (!uid) {
    return res.status(401).json({ error: 'Invalid token' });
  }
  const { error: upErr } = await supabase.from('profiles').update({ role }).eq('id', uid);
  if (upErr) {
    return res.status(500).json({ error: upErr.message });
  }
  const { data: profile, error: pErr } = await supabase
    .from('profiles')
    .select('*')
    .eq('id', uid)
    .maybeSingle();
  if (pErr || !profile) {
    return res.status(500).json({ error: pErr?.message ?? 'profile missing' });
  }
  let vehicle = null;
  if (role === 'captain') {
    const { data: v } = await supabase.from('vehicles').select('*').eq('driver_id', uid).maybeSingle();
    vehicle = v ?? null;
  }
  return res.json({ profile, vehicle });
});

/**
 * Dev: one-time customer personalization (same as Supabase profiles update; RN dev proxy).
 */
devRouter.post('/customer-personalization', async (req, res) => {
  if (process.env.ALLOW_DEV_OTP_BYPASS !== 'true' || !supabase) {
    return res.status(404).json({ error: 'not found' });
  }
  const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
  if (!token) {
    return res.status(400).json({ error: 'Missing Authorization' });
  }
  const uid = await getUserIdFromAccessToken(token);
  if (!uid) {
    return res.status(401).json({ error: 'Invalid token' });
  }
  const b = req.body ?? {};
  async function uploadToAvatars(path, b64, contentType = 'image/jpeg') {
    const buf = Buffer.from(b64, 'base64');
    const { error } = await supabase.storage.from('avatars').upload(path, buf, {
      contentType,
      upsert: true,
    });
    if (error) {
      throw new Error(error.message);
    }
    const { data } = supabase.storage.from('avatars').getPublicUrl(path);
    return data.publicUrl;
  }
  const userType = b.customer_user_type;
  if (userType !== 'individual' && userType !== 'seller') {
    return res.status(400).json({ error: 'customer_user_type must be individual or seller' });
  }
  const completedAt = new Date().toISOString();
  const payload = {
    customer_user_type: userType,
    customer_personalization_completed_at: completedAt,
  };
  if (userType === 'seller') {
    const range = b.customer_monthly_order_range;
    const allowed = ['0-5', '6-10', '20+'];
    if (typeof range !== 'string' || !allowed.includes(range)) {
      return res.status(400).json({ error: 'seller requires customer_monthly_order_range' });
    }
    const name = typeof b.full_name === 'string' ? b.full_name.trim() : '';
    if (!name) {
      return res.status(400).json({ error: 'full_name required for seller' });
    }
    payload.customer_monthly_order_range = range;
    payload.full_name = name;
    if (typeof b.customer_trade_photo_base64 !== 'string' || b.customer_trade_photo_base64.trim().length < 16) {
      return res.status(400).json({ error: 'seller requires customer_trade_photo_base64' });
    }
    try {
      payload.customer_trade_photo_url = await uploadToAvatars(
        `${uid}/customer/trade-unit.jpg`,
        b.customer_trade_photo_base64.trim(),
      );
    } catch (e) {
      return res.status(500).json({ error: e?.message ?? 'trade image upload failed' });
    }
  } else {
    payload.customer_monthly_order_range = null;
    payload.customer_trade_photo_url = null;
  }
  const { error: upErr } = await supabase.from('profiles').update(payload).eq('id', uid);
  if (upErr) {
    return res.status(500).json({ error: upErr.message });
  }
  const { data: profile, error: pErr } = await supabase
    .from('profiles')
    .select('*')
    .eq('id', uid)
    .maybeSingle();
  if (pErr || !profile) {
    return res.status(500).json({ error: pErr?.message ?? 'profile missing' });
  }
  return res.json({ profile });
});

/**
 * Dev: captain vehicle step — uploads + DB on server (RN emulator cannot reach Supabase Storage/REST).
 */
devRouter.post('/captain-vehicle-step', async (req, res) => {
  if (process.env.ALLOW_DEV_OTP_BYPASS !== 'true' || !supabase) {
    return res.status(404).json({ error: 'not found' });
  }
  const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
  if (!token) {
    return res.status(400).json({ error: 'Missing Authorization' });
  }
  const uid = await getUserIdFromAccessToken(token);
  if (!uid) {
    return res.status(401).json({ error: 'Invalid token' });
  }

  const { fullName, vehicle, avatarBase64, photos } = req.body ?? {};
  if (typeof fullName !== 'string' || !fullName.trim()) {
    return res.status(400).json({ error: 'fullName required' });
  }
  if (!vehicle || typeof vehicle !== 'object') {
    return res.status(400).json({ error: 'vehicle required' });
  }
  const maxWeightKg = Number(vehicle.max_weight_capacity_kg);
  if (!Number.isFinite(maxWeightKg) || maxWeightKg <= 0 || maxWeightKg > 200_000) {
    return res.status(400).json({
      error: 'max_weight_capacity_kg required: positive number in kg (max 200000)',
    });
  }
  const slots = ['front', 'back', 'left', 'right'];
  const photosIn = photos && typeof photos === 'object' ? photos : {};

  async function uploadToAvatars(path, b64, contentType = 'image/jpeg') {
    const buf = Buffer.from(b64, 'base64');
    const { error } = await supabase.storage.from('avatars').upload(path, buf, {
      contentType,
      upsert: true,
    });
    if (error) {
      throw new Error(error.message);
    }
    const { data } = supabase.storage.from('avatars').getPublicUrl(path);
    return data.publicUrl;
  }

  try {
    let avatarUrl = null;
    if (typeof avatarBase64 === 'string' && avatarBase64.length > 0) {
      avatarUrl = await uploadToAvatars(`${uid}/avatar.jpg`, avatarBase64);
    }

    const { data: existingV } = await supabase
      .from('vehicles')
      .select('photos')
      .eq('driver_id', uid)
      .maybeSingle();
    const photoUrls = { ...(existingV?.photos ?? {}) };
    for (const s of slots) {
      if (typeof photosIn[s] === 'string' && photosIn[s].length > 0) {
        photoUrls[s] = await uploadToAvatars(`${uid}/vehicle/${s}.jpg`, photosIn[s]);
      }
    }

    const profileUpdate = {
      full_name: fullName.trim(),
      captain_vehicle_submitted_at: new Date().toISOString(),
    };
    if (avatarUrl) {
      profileUpdate.avatar_url = avatarUrl;
    }

    const { error: pErr } = await supabase.from('profiles').update(profileUpdate).eq('id', uid);
    if (pErr) {
      return res.status(500).json({ error: pErr.message });
    }

    const { error: vErr } = await supabase.from('vehicles').upsert(
      {
        driver_id: uid,
        type: vehicle.type,
        registration_number: vehicle.registration_number,
        model: vehicle.model ?? null,
        max_weight_capacity_kg: maxWeightKg,
        photos: photoUrls,
      },
      { onConflict: 'driver_id' },
    );
    if (vErr) {
      return res.status(500).json({ error: vErr.message });
    }

    const { data: profile, error: p2 } = await supabase
      .from('profiles')
      .select('*')
      .eq('id', uid)
      .maybeSingle();
    if (p2 || !profile) {
      return res.status(500).json({ error: p2?.message ?? 'profile missing' });
    }
    const { data: vrow } = await supabase.from('vehicles').select('*').eq('driver_id', uid).maybeSingle();
    return res.json({ profile, vehicle: vrow ?? null });
  } catch (e) {
    return res.status(500).json({ error: e?.message ?? 'upload failed' });
  }
});

/**
 * Dev: captain KYC documents — uploads + profile update on server.
 */
devRouter.post('/captain-documents', async (req, res) => {
  if (process.env.ALLOW_DEV_OTP_BYPASS !== 'true' || !supabase) {
    return res.status(404).json({ error: 'not found' });
  }
  const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
  if (!token) {
    return res.status(400).json({ error: 'Missing Authorization' });
  }
  const uid = await getUserIdFromAccessToken(token);
  if (!uid) {
    return res.status(401).json({ error: 'Invalid token' });
  }

  const { documents } = req.body ?? {};
  const keys = ['rc', 'insurance', 'pollution', 'license', 'aadhar_front', 'aadhar_back'];
  if (!documents || typeof documents !== 'object') {
    return res.status(400).json({ error: 'documents required' });
  }

  async function uploadToAvatars(path, b64, contentType = 'image/jpeg') {
    const buf = Buffer.from(b64, 'base64');
    const { error } = await supabase.storage.from('avatars').upload(path, buf, {
      contentType,
      upsert: true,
    });
    if (error) {
      throw new Error(error.message);
    }
    const { data } = supabase.storage.from('avatars').getPublicUrl(path);
    return data.publicUrl;
  }

  try {
    const { data: profRow } = await supabase
      .from('profiles')
      .select('captain_documents')
      .eq('id', uid)
      .maybeSingle();
    const captain_documents = { ...(profRow?.captain_documents ?? {}) };
    for (const k of keys) {
      if (typeof documents[k] === 'string' && documents[k].length > 0) {
        captain_documents[k] = await uploadToAvatars(`${uid}/documents/${k}.jpg`, documents[k]);
      }
    }

    const { error: upErr } = await supabase
      .from('profiles')
      .update({
        captain_documents,
        profile_completed_at: new Date().toISOString(),
        captain_kyc_status: 'submitted',
        captain_kyc_submitted_at: new Date().toISOString(),
        captain_kyc_rejection_reason: null,
      })
      .eq('id', uid);
    if (upErr) {
      return res.status(500).json({ error: upErr.message });
    }

    await supabase
      .from('vehicles')
      .update({
        kyc_status: 'pending',
        kyc_reviewed_at: null,
        kyc_rejection_reason: null,
      })
      .eq('driver_id', uid);

    const { data: profile, error: pErr } = await supabase
      .from('profiles')
      .select('*')
      .eq('id', uid)
      .maybeSingle();
    if (pErr || !profile) {
      return res.status(500).json({ error: pErr?.message ?? 'profile missing' });
    }
    return res.json({ profile });
  } catch (e) {
    return res.status(500).json({ error: e?.message ?? 'upload failed' });
  }
});

app.use('/dev', devRouter);
app.use('/api/dev', devRouter);

const httpServer = createServer(app);
const io = new Server(httpServer, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
});
attachTripSocket(io, { supabase, getUserIdFromAccessToken });
registerRideRoutes(app, { supabase, getUserIdFromAccessToken, io });

httpServer.listen(port, '0.0.0.0', () => {
  console.log(`Oshu backend listening on http://0.0.0.0:${port} (emulator: http://10.0.2.2:${port})`);
  console.log('[oshu-backend] Socket.IO ready at same port (/socket.io)');
});
