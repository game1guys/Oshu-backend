/**
 * Referrals: each customer gets a code; referee applies once; both earn 200 coins (₹200).
 *
 *   GET  /api/referrals/me   — code, reward copy, whether already referred
 *   POST /api/referrals/apply — body { code } — one-time for customers
 */

const REFERRAL_REWARD_COINS = 200;
const REFERRAL_REWARD_INR = 200;

async function requireUser(supabase, getUserId, req, res) {
  const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
  if (!token || !supabase) {
    res.status(400).json({ error: 'Missing Authorization' });
    return null;
  }
  const uid = await getUserId(token);
  if (!uid) {
    res.status(401).json({ error: 'Invalid token' });
    return null;
  }
  return uid;
}

export function registerReferralRoutes(app, { supabase, getUserIdFromAccessToken }) {
  /** GET /api/referrals/me */
  app.get('/api/referrals/me', async (req, res) => {
    const uid = await requireUser(supabase, getUserIdFromAccessToken, req, res);
    if (!uid) return;

    const { data: profile, error: pErr } = await supabase
      .from('profiles')
      .select('id, role, referral_code, referred_by, referral_applied_at')
      .eq('id', uid)
      .maybeSingle();
    if (pErr) return res.status(500).json({ error: pErr.message });
    if (!profile) return res.status(404).json({ error: 'Profile not found' });
    if (profile.role !== 'user') {
      return res.status(403).json({ error: 'Referrals are for customers only' });
    }

    const { data: codeRaw, error: cErr } = await supabase.rpc('ensure_referral_code', {
      p_user: uid,
    });
    if (cErr) {
      return res.status(500).json({ error: cErr.message });
    }
    const referral_code = typeof codeRaw === 'string' ? codeRaw : profile.referral_code;

    const share_message =
      `Join Oshu with my code ${referral_code} — we both get ₹${REFERRAL_REWARD_INR} in Oshu coins!`;

    return res.json({
      referral_code,
      reward_coins: REFERRAL_REWARD_COINS,
      reward_inr: REFERRAL_REWARD_INR,
      share_message,
      already_referred: profile.referred_by != null,
      referral_applied_at: profile.referral_applied_at,
    });
  });

  /** POST /api/referrals/apply  body: { code: string } */
  app.post('/api/referrals/apply', async (req, res) => {
    const uid = await requireUser(supabase, getUserIdFromAccessToken, req, res);
    if (!uid) return;

    const raw = req.body?.code;
    if (typeof raw !== 'string' || !raw.trim()) {
      return res.status(400).json({ error: 'code is required' });
    }

    const { data: result, error: rErr } = await supabase.rpc('apply_referral_reward', {
      p_referee: uid,
      p_code: raw,
    });
    if (rErr) {
      return res.status(500).json({ error: rErr.message });
    }

    const row =
      result && typeof result === 'object' && !Array.isArray(result)
        ? /** @type {{ ok?: boolean; error?: string; reward_coins?: number }} */ (result)
        : {};
    if (row.ok !== true) {
      const err = row.error;
      if (err === 'invalid_code') {
        return res.status(400).json({ error: 'Invalid referral code' });
      }
      if (err === 'self_referral') {
        return res.status(400).json({ error: 'You cannot use your own code' });
      }
      if (err === 'customers_only') {
        return res.status(403).json({ error: 'Only customer accounts can apply a referral code' });
      }
      if (err === 'already_applied') {
        return res.status(409).json({ error: 'You have already applied a referral code' });
      }
      return res.status(400).json({ error: err ?? 'Could not apply code' });
    }

    return res.json({
      ok: true,
      reward_coins: row.reward_coins ?? REFERRAL_REWARD_COINS,
      reward_inr: REFERRAL_REWARD_INR,
    });
  });
}
