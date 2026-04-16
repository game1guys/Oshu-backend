/**
 * Captain wallet helpers: platform fee on online payments (customer → Oshu → captain wallet).
 */

export function platformFeePct() {
  const n = Number(process.env.OSHU_PLATFORM_FEE_PCT ?? 0);
  if (!Number.isFinite(n)) {
    return 0;
  }
  return Math.min(100, Math.max(0, n));
}

export function captainNetFromCustomerPayment(finalPayableInr) {
  const fee = platformFeePct();
  const x = Number(finalPayableInr);
  if (!Number.isFinite(x) || x <= 0) {
    return 0;
  }
  const net = (x * (100 - fee)) / 100;
  return Math.round(net * 100) / 100;
}

export function minWithdrawInr() {
  const n = Number(process.env.CAPTAIN_MIN_WITHDRAW_INR ?? 100);
  return Number.isFinite(n) && n > 0 ? n : 100;
}

export function normalizeUpiVpa(v) {
  return String(v ?? '')
    .trim()
    .toLowerCase();
}

/** Basic UPI VPA shape — not exhaustive. */
export function isPlausibleUpiVpa(v) {
  const s = normalizeUpiVpa(v);
  if (s.length < 5 || s.length > 100) {
    return false;
  }
  const at = s.indexOf('@');
  if (at <= 0 || at === s.length - 1) {
    return false;
  }
  return /^[a-z0-9._-]+@[a-z0-9.-]+$/.test(s);
}
