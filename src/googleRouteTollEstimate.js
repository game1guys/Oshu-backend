/**
 * Google Routes API v2 — estimated toll (INR) for pickup→drop.
 * Requires GOOGLE_MAPS_API_KEY with Routes API enabled. On failure / no coverage, returns 0.
 * @see https://developers.google.com/maps/documentation/routes/calculate_toll_fees
 */

const COMPUTE_ROUTES_URL = 'https://routes.googleapis.com/directions/v2:computeRoutes';
const REQUEST_TIMEOUT_MS = 12_000;

function roundMoney(n) {
  return Math.round(Number(n) * 100) / 100;
}

/** Parse Google Money (units may be string; nanos optional). */
function moneyToNumber(m) {
  if (!m) {
    return 0;
  }
  const u = typeof m.units === 'string' ? Number(m.units) : Number(m.units ?? 0);
  const n = typeof m.nanos === 'string' ? Number(m.nanos) : Number(m.nanos ?? 0);
  const units = Number.isFinite(u) ? u : 0;
  const nanos = Number.isFinite(n) ? n / 1e9 : 0;
  return units + nanos;
}

/** Sum INR entries only (robust units/nanos parsing). */
function sumInrFromTollInfo(tollInfo) {
  if (!tollInfo?.estimatedPrice?.length) {
    return 0;
  }
  let sum = 0;
  for (const m of tollInfo.estimatedPrice) {
    if (String(m?.currencyCode ?? '').toUpperCase() !== 'INR') {
      continue;
    }
    sum += moneyToNumber(m);
  }
  return roundMoney(sum);
}

function tollFromRoute(route) {
  if (!route) {
    return 0;
  }
  let toll = sumInrFromTollInfo(route.travelAdvisory?.tollInfo);
  if (toll > 0) {
    return toll;
  }
  const legs = route.legs;
  if (!Array.isArray(legs)) {
    return 0;
  }
  for (const leg of legs) {
    toll += sumInrFromTollInfo(leg?.travelAdvisory?.tollInfo);
  }
  return roundMoney(toll);
}

/**
 * @param {string} apiKey
 * @param {{ pickup_lat: number, pickup_lng: number, drop_lat: number, drop_lng: number }} coords
 * @returns {Promise<{ toll_inr: number, road_distance_km: number | null, ok: boolean, had_route: boolean }>}
 */
export async function fetchRouteTollEstimateInr(apiKey, coords) {
  if (!apiKey || typeof apiKey !== 'string') {
    return { toll_inr: 0, road_distance_km: null, ok: false, had_route: false };
  }
  const { pickup_lat, pickup_lng, drop_lat, drop_lng } = coords;
  if ([pickup_lat, pickup_lng, drop_lat, drop_lng].some(x => typeof x !== 'number' || Number.isNaN(x))) {
    return { toll_inr: 0, road_distance_km: null, ok: false, had_route: false };
  }

  /** India: FASTag is required for meaningful numeric toll on many NH routes in Routes API. */
  const body = {
    origin: { location: { latLng: { latitude: pickup_lat, longitude: pickup_lng } } },
    destination: { location: { latLng: { latitude: drop_lat, longitude: drop_lng } } },
    travelMode: 'DRIVE',
    routingPreference: 'TRAFFIC_AWARE',
    extraComputations: ['TOLLS'],
    routeModifiers: {
      vehicleInfo: { emissionType: 'DIESEL' },
      tollPasses: ['IN_FASTAG'],
    },
  };

  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  try {
    const res = await fetch(COMPUTE_ROUTES_URL, {
      method: 'POST',
      signal: controller.signal,
      headers: {
        'Content-Type': 'application/json',
        'X-Goog-Api-Key': apiKey.trim(),
        'X-Goog-FieldMask': 'routes.distanceMeters,routes.travelAdvisory.tollInfo,routes.legs.travelAdvisory.tollInfo',
      },
      body: JSON.stringify(body),
    });
    const text = await res.text();
    let data;
    try {
      data = JSON.parse(text);
    } catch {
      return { toll_inr: 0, road_distance_km: null, ok: false, had_route: false };
    }
    if (!res.ok) {
      console.warn('[oshu] Google Routes toll HTTP', res.status, data?.error?.message ?? text.slice(0, 200));
      return { toll_inr: 0, road_distance_km: null, ok: false, had_route: false };
    }
    const route = data?.routes?.[0];
    const had_route = Boolean(route);
    const toll_inr = tollFromRoute(route);
    const dm = route?.distanceMeters != null ? Number(route.distanceMeters) : null;
    const road_distance_km =
      dm != null && Number.isFinite(dm) ? roundMoney(dm / 1000) : null;
    if (had_route && toll_inr === 0) {
      const dbg = route?.travelAdvisory?.tollInfo ?? route?.legs?.[0]?.travelAdvisory?.tollInfo;
      if (dbg && JSON.stringify(dbg).length > 2) {
        console.warn('[oshu] Google Routes toll: route ok but ₹0 — tollInfo:', JSON.stringify(dbg).slice(0, 500));
      }
    }
    return { toll_inr, road_distance_km, ok: true, had_route };
  } catch (e) {
    console.warn('[oshu] Google Routes toll fetch failed', e?.message ?? e);
    return { toll_inr: 0, road_distance_km: null, ok: false, had_route: false };
  } finally {
    clearTimeout(t);
  }
}
