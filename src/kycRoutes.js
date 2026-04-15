/**
 * KYC HTTP routes.
 *
 * Aadhaar OTP: UIDAI does not offer a public REST API for arbitrary apps. Production must use a
 * licensed Authentication Service Provider (ASP) / e-KYC aggregator. AADHAAR_KYC_MODE=mock uses an
 * in-memory challenge and accepts any 6-digit OTP for development.
 */
import crypto from 'node:crypto';

/** txnId -> { userId, expiresAt, last4 } */
const aadhaarChallenges = new Map();

const CHALLENGE_TTL_MS = 10 * 60 * 1000;

function normalizeName(raw) {
  return String(raw ?? '')
    .toUpperCase()
    .replace(/[^A-Z\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function firstJsonObject(text) {
  const s = String(text ?? '');
  const start = s.indexOf('{');
  const end = s.lastIndexOf('}');
  if (start >= 0 && end > start) {
    return s.slice(start, end + 1);
  }
  return null;
}

async function geminiExtract({ apiKey, docType, base64, mimeType = 'image/jpeg' }) {
  const primaryModel = process.env.GEMINI_MODEL || 'gemini-2.5-flash';
  const fallbackModels = String(process.env.GEMINI_MODEL_FALLBACKS || 'gemini-2.5-flash-lite')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);
  const modelsToTry = [primaryModel, ...fallbackModels];
  const prompt =
    `You are an OCR + document parser for Indian KYC documents.\n` +
    `Return ONLY valid JSON. Do not include markdown.\n\n` +
    `doc_type: ${docType}\n` +
    `Extract fields when present:\n` +
    `- name (person name)\n` +
    `- doc_number (DL number / Aadhaar number masked or last4)\n` +
    `- expiry_date (ISO 8601 date or datetime; if only date, return YYYY-MM-DD)\n\n` +
    `If a field is missing, set it to null.\n`;

  const sleep = ms => new Promise(r => setTimeout(r, ms));
  let lastErr = null;

  for (const model of modelsToTry) {
    // 3 attempts per model: handles temporary "high demand" spikes.
    for (let attempt = 0; attempt < 3; attempt++) {
      const controller = new AbortController();
      const t = setTimeout(() => controller.abort(), 12_000);
      try {
        const url =
          `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(model)}:generateContent?key=${encodeURIComponent(apiKey)}`;
        const r = await fetch(url, {
          method: 'POST',
          signal: controller.signal,
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            contents: [
              {
                role: 'user',
                parts: [
                  { text: prompt },
                  { inlineData: { mimeType, data: base64 } },
                ],
              },
            ],
            generationConfig: {
              temperature: 0,
              responseMimeType: 'application/json',
            },
          }),
        });
        const j = await r.json().catch(() => ({}));
        if (!r.ok) {
          const msg = j?.error?.message || j?.error || `Gemini HTTP ${r.status}`;
          const retriable =
            r.status === 429 ||
            r.status === 503 ||
            /high demand|rate limit|quota|overloaded|try again/i.test(String(msg));
          if (retriable && attempt < 2) {
            // Exponential backoff with small jitter.
            const backoffMs = Math.min(2500, 350 * 2 ** attempt) + Math.floor(Math.random() * 200);
            await sleep(backoffMs);
            continue;
          }
          throw new Error(msg);
        }
        const text =
          j?.candidates?.[0]?.content?.parts?.map(p => p.text).filter(Boolean).join('\n') ??
          j?.candidates?.[0]?.content?.parts?.[0]?.text ??
          '';
        const jsonText = firstJsonObject(text) ?? text;
        const parsed = JSON.parse(jsonText);
        return parsed;
      } catch (e) {
        lastErr = e instanceof Error ? e : new Error('Gemini OCR failed');
        // If aborted/timeout, try again quickly (still within attempt limit).
        if (e?.name === 'AbortError' && attempt < 2) {
          await sleep(200 + Math.floor(Math.random() * 150));
          continue;
        }
        // Otherwise, break attempts and try next model.
        break;
      } finally {
        clearTimeout(t);
      }
    }
  }

  throw lastErr ?? new Error('Gemini OCR failed');
}

function sweepChallenges() {
  const now = Date.now();
  for (const [k, v] of aadhaarChallenges.entries()) {
    if (v.expiresAt < now) {
      aadhaarChallenges.delete(k);
    }
  }
}

function normalizeAadhaar(raw) {
  const d = String(raw ?? '').replace(/\D/g, '');
  return d.length === 12 ? d : null;
}

async function uniqueCaptainOshuId(supabase) {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  for (let attempt = 0; attempt < 24; attempt++) {
    let s = 'OSH-CAP-';
    for (let i = 0; i < 6; i++) {
      s += chars[crypto.randomInt(0, chars.length)];
    }
    const { data } = await supabase.from('profiles').select('id').eq('captain_oshu_id', s).maybeSingle();
    if (!data) {
      return s;
    }
  }
  throw new Error('Could not allocate Oshu ID');
}

export function registerKycRoutes(app, { supabase, getUserIdFromAccessToken }) {
  if (!supabase) {
    return;
  }

  async function readUser(req, res) {
    const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
    if (!token) {
      res.status(400).json({ error: 'Missing Authorization' });
      return null;
    }
    const uid = await getUserIdFromAccessToken(token);
    if (!uid) {
      res.status(401).json({ error: 'Invalid token' });
      return null;
    }
    return { token, uid };
  }

  async function readAdmin(req, res) {
    const u = await readUser(req, res);
    if (!u) {
      return null;
    }
    const { data: prof } = await supabase.from('profiles').select('role').eq('id', u.uid).maybeSingle();
    if (prof?.role !== 'admin') {
      res.status(403).json({ error: 'Admin only' });
      return null;
    }
    return u;
  }

  /**
   * POST /api/kyc/ocr
   * Body: { doc_type: "license"|"aadhar_front"|"insurance"|"pollution", image_base64: "..." }
   *
   * Calls Gemini OCR (API key must be set on server: GEMINI_API_KEY).
   */
  app.post('/api/kyc/ocr', async (req, res) => {
    const u = await readUser(req, res);
    if (!u) {
      return;
    }
    const { data: prof } = await supabase.from('profiles').select('role').eq('id', u.uid).maybeSingle();
    if (prof?.role !== 'captain') {
      return res.status(403).json({ error: 'Captains only' });
    }
    const doc_type = typeof req.body?.doc_type === 'string' ? req.body.doc_type : '';
    const image_base64 = typeof req.body?.image_base64 === 'string' ? req.body.image_base64 : '';
    if (!doc_type || !['license', 'aadhar_front', 'insurance', 'pollution'].includes(doc_type)) {
      return res.status(400).json({ error: 'Invalid doc_type' });
    }
    if (!image_base64 || image_base64.length < 100) {
      return res.status(400).json({ error: 'image_base64 required' });
    }
    const apiKey = process.env.GEMINI_API_KEY;
    if (!apiKey) {
      return res.status(503).json({ error: 'Gemini OCR not configured (missing GEMINI_API_KEY)' });
    }
    try {
      const out = await geminiExtract({ apiKey, docType: doc_type, base64: image_base64 });
      const result = {
        name: out?.name != null ? normalizeName(out.name) : null,
        doc_number: typeof out?.doc_number === 'string' ? out.doc_number.trim() : null,
        expiry_date: typeof out?.expiry_date === 'string' ? out.expiry_date.trim() : null,
      };
      return res.json({ ok: true, result });
    } catch (e) {
      return res.status(400).json({ ok: false, error: e instanceof Error ? e.message : 'OCR failed' });
    }
  });

  /**
   * POST /api/kyc/aadhaar/request-otp
   * Body: { aadhaarNumber: "12 digits" }
   */
  app.post('/api/kyc/aadhaar/request-otp', async (req, res) => {
    const u = await readUser(req, res);
    if (!u) {
      return;
    }
    const aadhaar = normalizeAadhaar(req.body?.aadhaarNumber);
    if (!aadhaar) {
      return res.status(400).json({ error: 'Enter a valid 12-digit Aadhaar number' });
    }
    sweepChallenges();
    const mode = process.env.AADHAAR_KYC_MODE || 'mock';
    const txnId = crypto.randomBytes(16).toString('hex');
    const expiresAt = Date.now() + CHALLENGE_TTL_MS;
    aadhaarChallenges.set(txnId, { userId: u.uid, expiresAt, last4: aadhaar.slice(-4) });

    if (mode === 'mock') {
      console.info('[kyc] Aadhaar OTP (mock): txn=%s last4=%s — any 6-digit OTP works', txnId, aadhaar.slice(-4));
      return res.json({
        txnId,
        expiresInSec: Math.floor(CHALLENGE_TTL_MS / 1000),
        mock: true,
        message: 'OTP sent (mock). Enter any 6-digit code to verify.',
      });
    }

    return res.status(501).json({
      error:
        'AADHAAR_KYC_MODE is not mock. Configure a licensed ASP integration (UIDAI has no public app OTP API).',
    });
  });

  /**
   * POST /api/kyc/aadhaar/verify-otp
   * Body: { txnId, otp }
   */
  app.post('/api/kyc/aadhaar/verify-otp', async (req, res) => {
    const u = await readUser(req, res);
    if (!u) {
      return;
    }
    const txnId = typeof req.body?.txnId === 'string' ? req.body.txnId : '';
    const otp = String(req.body?.otp ?? '').replace(/\D/g, '');
    if (!txnId || otp.length !== 6) {
      return res.status(400).json({ error: 'txnId and 6-digit OTP required' });
    }
    sweepChallenges();
    const ch = aadhaarChallenges.get(txnId);
    if (!ch || ch.userId !== u.uid || ch.expiresAt < Date.now()) {
      return res.status(400).json({ error: 'Invalid or expired session. Request OTP again.' });
    }
    const mode = process.env.AADHAAR_KYC_MODE || 'mock';
    if (mode !== 'mock') {
      return res.status(501).json({ error: 'ASP verification not implemented' });
    }

    aadhaarChallenges.delete(txnId);
    const masked = `********${ch.last4}`;
    const verifiedAt = new Date().toISOString();
    const { error: upErr } = await supabase
      .from('profiles')
      .update({
        customer_aadhaar_masked: masked,
        customer_aadhaar_verified_at: verifiedAt,
      })
      .eq('id', u.uid);
    if (upErr) {
      return res.status(500).json({ error: upErr.message });
    }
    return res.json({ ok: true, customer_aadhaar_masked: masked, customer_aadhaar_verified_at: verifiedAt });
  });

  /**
   * GET /api/admin/kyc/pending-captains
   */
  app.get('/api/admin/kyc/pending-captains', async (req, res) => {
    const a = await readAdmin(req, res);
    if (!a) {
      return;
    }
    const { data: rows, error } = await supabase
      .from('profiles')
      .select(
        'id, full_name, phone, captain_kyc_status, captain_documents, captain_kyc_submitted_at, captain_kyc_rejection_reason, avatar_url',
      )
      .eq('role', 'captain')
      .in('captain_kyc_status', ['submitted', 'under_review']);
    if (error) {
      return res.status(500).json({ error: error.message });
    }
    const ids = (rows ?? []).map(r => r.id);
    let vehiclesByDriver = {};
    if (ids.length) {
      const { data: veh } = await supabase.from('vehicles').select('*').in('driver_id', ids);
      for (const v of veh ?? []) {
        vehiclesByDriver[v.driver_id] = v;
      }
    }
    const list = (rows ?? []).map(p => ({
      profile: p,
      vehicle: vehiclesByDriver[p.id] ?? null,
    }));
    return res.json({ captains: list });
  });

  /**
   * POST /api/admin/kyc/approve-captain
   * Body: { captainId: uuid }
   */
  app.post('/api/admin/kyc/approve-captain', async (req, res) => {
    const a = await readAdmin(req, res);
    if (!a) {
      return;
    }
    const captainId = req.body?.captainId;
    if (!captainId || typeof captainId !== 'string') {
      return res.status(400).json({ error: 'captainId required' });
    }
    const { data: cap, error: cErr } = await supabase
      .from('profiles')
      .select('id, role, captain_kyc_status')
      .eq('id', captainId)
      .maybeSingle();
    if (cErr || !cap || cap.role !== 'captain') {
      return res.status(404).json({ error: 'Captain not found' });
    }
    let oshuId;
    try {
      oshuId = await uniqueCaptainOshuId(supabase);
    } catch (e) {
      return res.status(500).json({ error: e?.message ?? 'ID allocation failed' });
    }
    const now = new Date().toISOString();
    const { error: pErr } = await supabase
      .from('profiles')
      .update({
        captain_kyc_status: 'approved',
        captain_kyc_reviewed_at: now,
        captain_kyc_rejection_reason: null,
        captain_oshu_id: oshuId,
        captain_id_card_issued_at: now,
      })
      .eq('id', captainId);
    if (pErr) {
      return res.status(500).json({ error: pErr.message });
    }
    const { error: vErr } = await supabase
      .from('vehicles')
      .update({
        kyc_status: 'approved',
        kyc_reviewed_at: now,
        kyc_rejection_reason: null,
      })
      .eq('driver_id', captainId);
    if (vErr) {
      return res.status(500).json({ error: vErr.message });
    }
    const { data: profile } = await supabase.from('profiles').select('*').eq('id', captainId).maybeSingle();
    const { data: vehicle } = await supabase.from('vehicles').select('*').eq('driver_id', captainId).maybeSingle();
    return res.json({ ok: true, profile, vehicle, captain_oshu_id: oshuId });
  });

  /**
   * POST /api/admin/kyc/reject-captain
   * Body: { captainId, reason }
   */
  app.post('/api/admin/kyc/reject-captain', async (req, res) => {
    const a = await readAdmin(req, res);
    if (!a) {
      return;
    }
    const captainId = req.body?.captainId;
    const reason = typeof req.body?.reason === 'string' ? req.body.reason.trim() : '';
    if (!captainId || typeof captainId !== 'string') {
      return res.status(400).json({ error: 'captainId required' });
    }
    if (!reason) {
      return res.status(400).json({ error: 'reason required' });
    }
    const { data: cap } = await supabase.from('profiles').select('id, role').eq('id', captainId).maybeSingle();
    if (!cap || cap.role !== 'captain') {
      return res.status(404).json({ error: 'Captain not found' });
    }
    const now = new Date().toISOString();
    const { error: pErr } = await supabase
      .from('profiles')
      .update({
        captain_kyc_status: 'rejected',
        captain_kyc_reviewed_at: now,
        captain_kyc_rejection_reason: reason,
      })
      .eq('id', captainId);
    if (pErr) {
      return res.status(500).json({ error: pErr.message });
    }
    await supabase
      .from('vehicles')
      .update({
        kyc_status: 'rejected',
        kyc_reviewed_at: now,
        kyc_rejection_reason: reason,
      })
      .eq('driver_id', captainId);
    const { data: profile } = await supabase.from('profiles').select('*').eq('id', captainId).maybeSingle();
    return res.json({ ok: true, profile });
  });
}
