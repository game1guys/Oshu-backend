/**
 * Google Routes API v2 — driving polyline between two points (for live trip navigation on map).
 */

const COMPUTE_ROUTES_URL = 'https://routes.googleapis.com/directions/v2:computeRoutes';
const REQUEST_TIMEOUT_MS = 12_000;

/** Decode Google encoded polyline (precision 5). */
export function decodePolyline(encoded) {
  if (!encoded || typeof encoded !== 'string') {
    return [];
  }
  const coords = [];
  let index = 0;
  let lat = 0;
  let lng = 0;
  while (index < encoded.length) {
    let b;
    let shift = 0;
    let result = 0;
    do {
      b = encoded.charCodeAt(index++) - 63;
      result |= (b & 0x1f) << shift;
      shift += 5;
    } while (b >= 0x20);
    const dlat = (result & 1) !== 0 ? ~(result >> 1) : result >> 1;
    lat += dlat;
    shift = 0;
    result = 0;
    do {
      b = encoded.charCodeAt(index++) - 63;
      result |= (b & 0x1f) << shift;
      shift += 5;
    } while (b >= 0x20);
    const dlng = (result & 1) !== 0 ? ~(result >> 1) : result >> 1;
    lng += dlng;
    coords.push({ latitude: lat * 1e-5, longitude: lng * 1e-5 });
  }
  return coords;
}

function decimatePath(points, maxPts) {
  if (!points?.length || points.length <= maxPts) {
    return points ?? [];
  }
  const step = Math.ceil(points.length / maxPts);
  const out = [];
  for (let i = 0; i < points.length; i += step) {
    out.push(points[i]);
  }
  const last = points[points.length - 1];
  const olast = out[out.length - 1];
  if (olast.latitude !== last.latitude || olast.longitude !== last.longitude) {
    out.push(last);
  }
  return out;
}

/**
 * @param {string} apiKey
 * @param {number} originLat
 * @param {number} originLng
 * @param {number} destLat
 * @param {number} destLng
 * @returns {Promise<{ path: { latitude: number; longitude: number }[]; duration_seconds: number | null; distance_meters: number | null; ok: boolean }>}
 */
export async function fetchDrivingRoutePolyline(apiKey, originLat, originLng, destLat, destLng) {
  if (!apiKey || typeof apiKey !== 'string') {
    return { path: [], duration_seconds: null, distance_meters: null, ok: false };
  }
  if (
    [originLat, originLng, destLat, destLng].some(
      x => typeof x !== 'number' || Number.isNaN(x) || !Number.isFinite(x),
    )
  ) {
    return { path: [], duration_seconds: null, distance_meters: null, ok: false };
  }

  const body = {
    origin: { location: { latLng: { latitude: originLat, longitude: originLng } } },
    destination: { location: { latLng: { latitude: destLat, longitude: destLng } } },
    travelMode: 'DRIVE',
    routingPreference: 'TRAFFIC_AWARE',
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
        'X-Goog-FieldMask': 'routes.polyline,routes.duration,routes.distanceMeters',
      },
      body: JSON.stringify(body),
    });
    const text = await res.text();
    let data;
    try {
      data = JSON.parse(text);
    } catch {
      return { path: [], duration_seconds: null, distance_meters: null, ok: false };
    }
    if (!res.ok) {
      return { path: [], duration_seconds: null, distance_meters: null, ok: false };
    }
    const r0 = data?.routes?.[0];
    const enc = r0?.polyline?.encodedPolyline;
    if (!enc) {
      return { path: [], duration_seconds: null, distance_meters: null, ok: false };
    }
    const raw = decodePolyline(enc);
    const path = decimatePath(raw, 450);
    let duration_seconds = null;
    if (typeof r0?.duration === 'string') {
      const m = r0.duration.match(/^(\d+)(?:\.\d+)?s$/);
      if (m) {
        duration_seconds = Number(m[1]);
      }
    } else if (typeof r0?.duration === 'number') {
      duration_seconds = r0.duration;
    }
    const distance_meters =
      typeof r0?.distanceMeters === 'number' && Number.isFinite(r0.distanceMeters) ? r0.distanceMeters : null;
    return { path, duration_seconds, distance_meters, ok: path.length > 0 };
  } catch {
    return { path: [], duration_seconds: null, distance_meters: null, ok: false };
  } finally {
    clearTimeout(t);
  }
}
