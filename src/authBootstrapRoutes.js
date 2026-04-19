/**
 * POST /api/auth/bootstrap — ensure `public.profiles` exists for the JWT user (service role).
 * Used by the mobile app after OTP/session so new users get a row before RoleSelect / setRole.
 */

function bearerToken(req) {
  const h = req.headers.authorization;
  if (!h || typeof h !== 'string') {
    return null;
  }
  const m = h.match(/^Bearer\s+(.+)$/i);
  return m ? m[1].trim() : null;
}

function phoneFromAuthUserRecord(user) {
  if (!user) {
    return null;
  }
  if (typeof user.phone === 'string' && user.phone.trim()) {
    return user.phone.trim();
  }
  const um = user.user_metadata;
  if (um && typeof um === 'object') {
    const p = um.phone ?? um.phone_number;
    if (typeof p === 'string' && p.trim()) {
      return p.trim();
    }
  }
  return null;
}

export function registerAuthBootstrapRoutes(app, { supabase, getUserIdFromAccessToken }) {
  app.post('/api/auth/bootstrap', async (req, res) => {
    if (!supabase) {
      return res.status(503).json({ error: 'Supabase not configured' });
    }
    const token = bearerToken(req);
    if (!token) {
      return res.status(400).json({ error: 'Missing Authorization' });
    }
    const uid = await getUserIdFromAccessToken(token);
    if (!uid) {
      return res.status(401).json({ error: 'Invalid token' });
    }

    let authPhone = null;
    try {
      const { data: authData, error: authErr } = await supabase.auth.admin.getUserById(uid);
      if (!authErr && authData?.user) {
        authPhone = phoneFromAuthUserRecord(authData.user);
      }
    } catch (e) {
      console.warn('[oshu-backend] auth/bootstrap: getUserById failed', e?.message ?? e);
    }

    const fetchProfile = async () => {
      const { data: profile, error: pErr } = await supabase
        .from('profiles')
        .select('*')
        .eq('id', uid)
        .maybeSingle();
      return { profile: profile ?? null, error: pErr };
    };

    let { profile, error: pErr } = await fetchProfile();
    if (pErr) {
      return res.status(500).json({ error: pErr.message });
    }

    if (!profile) {
      const payload = { id: uid, phone: authPhone ?? null };
      const { data: inserted, error: insErr } = await supabase
        .from('profiles')
        .insert(payload)
        .select('*')
        .maybeSingle();

      if (insErr) {
        if (insErr.code === '23505') {
          const again = await fetchProfile();
          profile = again.profile;
          if (again.error || !profile) {
            return res.status(500).json({ error: again.error?.message ?? 'profile missing after race' });
          }
        } else {
          return res.status(500).json({ error: insErr.message });
        }
      } else if (inserted) {
        profile = inserted;
      } else {
        const again = await fetchProfile();
        profile = again.profile;
        if (again.error || !profile) {
          return res.status(500).json({ error: again.error?.message ?? 'profile missing after insert' });
        }
      }
    }

    if (!profile) {
      return res.status(500).json({ error: 'profile missing' });
    }

    if (authPhone && (!profile.phone || String(profile.phone).replace(/\D/g, '').length < 10)) {
      const { error: upErr } = await supabase.from('profiles').update({ phone: authPhone }).eq('id', uid);
      if (!upErr) {
        const { data: pr2, error: e2 } = await supabase
          .from('profiles')
          .select('*')
          .eq('id', uid)
          .maybeSingle();
        if (!e2 && pr2) {
          profile = pr2;
        }
      }
    }

    let vehicle = null;
    if (profile.role === 'captain') {
      const { data: v } = await supabase.from('vehicles').select('*').eq('driver_id', uid).maybeSingle();
      vehicle = v ?? null;
    }

    const needsRoleSelect = !profile.role;
    return res.json({
      needs_role_select: needsRoleSelect,
      role: profile.role ?? null,
      profile,
      vehicle,
    });
  });
}
