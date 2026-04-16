/**
 * KYC HTTP routes.
 *
 * Aadhaar OTP: UIDAI does not offer a public REST API for arbitrary apps. Production must use a
 * licensed Authentication Service Provider (ASP) / e-KYC aggregator. AADHAAR_KYC_MODE=mock uses an
 * in-memory challenge and accepts any 6-digit OTP for development.
 */
import crypto from 'node:crypto';
import { createClient } from '@supabase/supabase-js';

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
    `You are a strict Indian KYC document validator + OCR.\n` +
    `Return ONLY valid JSON (no markdown).\n\n` +
    `doc_type: ${docType}\n\n` +
    `First answer this question with ONE WORD (yes/no):\n` +
    `Q: Is this image a valid photo/scan of the requested doc_type (${docType})?\n\n` +
    `Return fields:\n` +
    `- answer: "yes"|"no" — ALWAYS include this (lowercase)\n` +
    `- issue: string|null — short reason when answer="no"\n` +
    `- doc_kind: string — one of: "rc", "license", "aadhar_front", "aadhar_back", "insurance", "pollution", "other", "unknown"\n` +
    `- name: string|null\n` +
    `- doc_number: string|null\n` +
    `- vehicle_number: string|null\n` +
    `- expiry_date: string|null (ISO 8601; if only date, YYYY-MM-DD)\n\n` +
    `Rules:\n` +
    `- Be conservative: answer "yes" ONLY if you are very confident this is the requested document.\n` +
    `- If the image is a selfie, scenery, random photo, app screenshot, chat screenshot, or anything not clearly a document, answer "no".\n` +
    `- Prefer "no" when the image is blurry, cropped, or key text is unreadable.\n` +
    `- doc_type cues:\n` +
    `  • license: should clearly show "DRIVING LICENCE"/"DRIVING LICENSE" or typical Indian DL layout.\n` +
    `  • rc: should clearly look like an RC/Registration Certificate with a vehicle registration number.\n` +
    `  • insurance: should clearly look like a motor insurance policy/certificate and include an expiry/valid till date.\n` +
    `  • pollution: should clearly look like a PUC certificate and include a validity/expiry date.\n` +
    `  • aadhar_front/aadhar_back: should clearly look like Aadhaar card (front/back).\n` +
    `- If answer="no", still attempt to set doc_kind and issue; other fields may be null.\n` +
    `- Never hallucinate numbers/dates; if unclear, return null.\n`;

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

async function geminiYesNoValidate({ apiKey, docType, base64, mimeType = 'image/jpeg' }) {
  const primaryModel = process.env.GEMINI_MODEL || 'gemini-2.5-flash';
  const fallbackModels = String(process.env.GEMINI_MODEL_FALLBACKS || 'gemini-2.5-flash-lite')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);
  const modelsToTry = [primaryModel, ...fallbackModels];
  const prompt =
    `Return ONLY valid JSON.\n` +
    `Answer with ONE WORD only (yes/no) in the "answer" field.\n\n` +
    `Q: Is this image a valid photo/scan of the requested document type "${docType}"?\n` +
    `Rules: Be conservative. If unsure, answer "no".\n\n` +
    `JSON shape:\n` +
    `{\n` +
    `  "answer": "yes" | "no",\n` +
    `  "issue": string | null\n` +
    `}\n`;

  const sleep = ms => new Promise(r => setTimeout(r, ms));
  let lastErr = null;
  for (const model of modelsToTry) {
    for (let attempt = 0; attempt < 3; attempt++) {
      const controller = new AbortController();
      const t = setTimeout(() => controller.abort(), 10_000);
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
            generationConfig: { temperature: 0, responseMimeType: 'application/json' },
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
            const backoffMs = Math.min(2000, 300 * 2 ** attempt) + Math.floor(Math.random() * 200);
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
        return JSON.parse(jsonText);
      } catch (e) {
        lastErr = e instanceof Error ? e : new Error('Gemini validate failed');
        if (e?.name === 'AbortError' && attempt < 2) {
          await sleep(200 + Math.floor(Math.random() * 150));
          continue;
        }
        break;
      } finally {
        clearTimeout(t);
      }
    }
  }
  throw lastErr ?? new Error('Gemini validate failed');
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

  const supabaseUrl = process.env.SUPABASE_URL;
  const anonKey = process.env.SUPABASE_ANON_KEY;
  const anonAuthClient =
    supabaseUrl && anonKey
      ? createClient(supabaseUrl, anonKey, { auth: { persistSession: false, autoRefreshToken: false } })
      : null;

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
      // Allow-listed admin phones: promote role server-side.
      const phone = (() => {
        try {
          const mid = u.token.split('.')[1];
          const pad = mid.length % 4 === 0 ? '' : '='.repeat(4 - (mid.length % 4));
          const b64 = mid.replace(/-/g, '+').replace(/_/g, '/') + pad;
          const payload = JSON.parse(Buffer.from(b64, 'base64').toString('utf8'));
          const p = payload?.phone ?? payload?.user_metadata?.phone ?? payload?.user_metadata?.phone_number;
          return typeof p === 'string' ? p : null;
        } catch {
          return null;
        }
      })();
      let authPhone = null;
      if (anonAuthClient) {
        try {
          const { data } = await anonAuthClient.auth.getUser(u.token);
          authPhone =
            data?.user?.phone ?? data?.user?.user_metadata?.phone ?? data?.user?.user_metadata?.phone_number ?? null;
        } catch {
          authPhone = null;
        }
      }
      let adminPhone = null;
      try {
        const out = await supabase.auth.admin.getUserById(u.uid);
        adminPhone =
          out?.data?.user?.phone ??
          out?.data?.user?.user_metadata?.phone ??
          out?.data?.user?.user_metadata?.phone_number ??
          null;
      } catch {
        adminPhone = null;
      }
      const { data: pRow } = await supabase.from('profiles').select('phone').eq('id', u.uid).maybeSingle();
      const chosenPhone = [phone, authPhone, adminPhone, pRow?.phone].find(
        v => typeof v === 'string' && v.replace(/\D/g, '').length >= 10,
      );
      const digits = String(chosenPhone ?? '').replace(/\D/g, '');
      const last10 = digits.length >= 10 ? digits.slice(-10) : digits;
      const env = String(process.env.ADMIN_PHONES_LAST10 ?? '').trim();
      const set = new Set((env ? env.split(',') : ['7985935125']).map(x => String(x).trim()).filter(Boolean));
      if (last10 && set.has(last10)) {
        // Best-effort persist role; never block whitelisted admin.
        try {
          await supabase.from('profiles').update({ role: 'admin' }).eq('id', u.uid);
        } catch {
          // ignore
        }
        return u;
      }
      res.status(403).json({ error: 'Admin only' });
      return null;
    }
    return u;
  }

  /**
   * POST /api/kyc/validate
   * Body: { doc_type: "rc"|"license"|"aadhar_front"|"aadhar_back"|"insurance"|"pollution", image_base64: "..." }
   *
   * Calls Gemini with a simple yes/no question.
   */
  app.post('/api/kyc/validate', async (req, res) => {
    const u = await readUser(req, res);
    if (!u) return;
    const { data: prof } = await supabase.from('profiles').select('role').eq('id', u.uid).maybeSingle();
    if (prof?.role !== 'captain') {
      return res.status(403).json({ error: 'Captains only' });
    }
    const doc_type = typeof req.body?.doc_type === 'string' ? req.body.doc_type : '';
    const image_base64 = typeof req.body?.image_base64 === 'string' ? req.body.image_base64 : '';
    if (!doc_type || !['rc', 'license', 'aadhar_front', 'aadhar_back', 'insurance', 'pollution'].includes(doc_type)) {
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
      const out = await geminiYesNoValidate({ apiKey, docType: doc_type, base64: image_base64 });
      const answerRaw = typeof out?.answer === 'string' ? out.answer.trim().toLowerCase() : '';
      const isYes = answerRaw === 'yes';
      const issue = typeof out?.issue === 'string' ? out.issue.trim() : null;
      console.info('[kyc][validate] doc_type=%s answer=%s issue=%s', doc_type, isYes ? 'yes' : 'no', issue ?? '');
      return res.json({ ok: true, result: { answer: isYes ? 'yes' : 'no', is_expected_doc: isYes, issue } });
    } catch (e) {
      return res.status(400).json({ ok: false, error: e instanceof Error ? e.message : 'Validate failed' });
    }
  });

  /**
   * POST /api/kyc/ocr
   * Body: { doc_type: "rc"|"license"|"aadhar_front"|"aadhar_back"|"insurance"|"pollution", image_base64: "..." }
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
    if (!doc_type || !['rc', 'license', 'aadhar_front', 'aadhar_back', 'insurance', 'pollution'].includes(doc_type)) {
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
      const answerRaw = typeof out?.answer === 'string' ? out.answer.trim().toLowerCase() : '';
      let isYes = answerRaw === 'yes';
      const kind = typeof out?.doc_kind === 'string' ? out.doc_kind.trim() : '';
      // If kind mismatches requested doc_type, force NO.
      if (isYes && kind && kind !== doc_type) {
        isYes = false;
      }
      // Require key fields per doc_type; otherwise force NO.
      const docNumber = typeof out?.doc_number === 'string' ? out.doc_number.trim() : '';
      const vehicleNumber = typeof out?.vehicle_number === 'string' ? out.vehicle_number.trim() : '';
      const expiry = typeof out?.expiry_date === 'string' ? out.expiry_date.trim() : '';
      const name = out?.name != null ? normalizeName(out.name) : '';
      if (isYes) {
        if (doc_type === 'rc' && !vehicleNumber) {
          isYes = false;
        }
        if ((doc_type === 'insurance' || doc_type === 'pollution') && !expiry) {
          isYes = false;
        }
        if (doc_type === 'license' && !docNumber) {
          isYes = false;
        }
        if (doc_type === 'aadhar_front' && !name && !docNumber) {
          isYes = false;
        }
        if (doc_type === 'aadhar_back' && !docNumber) {
          // Back often doesn't show name; require some Aadhaar number hint.
          isYes = false;
        }
      }
      const result = {
        // Strict yes/no: anything else is treated as "no".
        answer: isYes ? 'yes' : 'no',
        is_expected_doc: isYes,
        doc_kind: kind || null,
        issue:
          typeof out?.issue === 'string' && out.issue.trim()
            ? out.issue.trim()
            : isYes
              ? null
              : 'Not a valid document image or required fields could not be verified.',
        name: name || null,
        doc_number: docNumber || null,
        vehicle_number: vehicleNumber || null,
        expiry_date: expiry || null,
      };
      console.info(
        '[kyc][ocr] doc_type=%s answer=%s kind=%s issue=%s doc=%s veh=%s exp=%s name=%s',
        doc_type,
        result.answer,
        result.doc_kind ?? '',
        result.issue ?? '',
        result.doc_number ?? '',
        result.vehicle_number ?? '',
        result.expiry_date ?? '',
        result.name ?? '',
      );
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
