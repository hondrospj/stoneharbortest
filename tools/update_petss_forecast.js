#!/usr/bin/env node
/**
 * Crest-anchored NAVD88 "high tide events" builder for USGS 01412150 (param 72279)
 * - Uses NOAA CO-OPS predicted HIGH tide crest times (interval=hilo, type=H) as the "tide clock"
 * - For each predicted HIGH tide crest:
 *    - Search observed USGS IV points within ±2 hours and take the MAX
 *    - BUT: if there are ZERO observed points within ±1 hour of the crest, SKIP that crest entirely
 *
 * Writes to: data/peaks_navd88.json
 *
 * Modes:
 *   node tools/update_peaks_navd88.js
 *     -> incremental update from lastProcessedISO (with buffer) to now
 *
 *   node tools/update_peaks_navd88.js --backfill-year=2000
 *     -> backfill exactly that calendar year (UTC)
 *
 *   node tools/update_peaks_navd88.js --backfill-from=2000 --backfill-to=2026
 *     -> backfill inclusive year range (UTC)
 */

const fs = require("fs");
const path = require("path");

// -------------------------
// Config (matches your dashboard)
// -------------------------
const CACHE_PATH = path.join(__dirname, "..", "data", "peaks_navd88.json");

const SITE = "01407600";
const PARAM = "72279";

// NOAA tide-clock (predicted highs/lows) — used ONLY for crest times
const NOAA_STATION = "8531804"; // , Maurice River, NJ (as in your dashboard)

// Keep this in cache for transparency; we still keep your 5-hour constant in JSON,
// but we are no longer using declustering for cache building under this method.
const PEAK_MIN_SEP_MINUTES = 300;

// Incremental overlap so boundary crests don't get missed
const BUFFER_HOURS = 12;

// Crest anchoring rules (your request)
const CREST_WINDOW_HOURS = 2;      // search max within ±2h of predicted crest
const REQUIRE_WITHIN_HOURS = 1;    // if NO obs points within ±1h, skip that crest entirely

// Method/version tag so you can cleanly rebuild without mixing old scheme
const METHOD = "crest_anchored_highs_v1";

// -------------------------
// Helpers
// -------------------------
function die(msg) {
  console.error(msg);
  process.exit(1);
}

function loadJSON(p) {
  if (!fs.existsSync(p)) die(`Missing cache file: ${p}`);
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function saveJSON(p, obj) {
  fs.writeFileSync(p, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

function isoNow() {
  return new Date().toISOString();
}

function addHoursISO(iso, hours) {
  const t = new Date(iso).getTime();
  if (!Number.isFinite(t)) return null;
  return new Date(t + hours * 3600 * 1000).toISOString();
}

function clampISO(iso) {
  const t = new Date(iso).getTime();
  return Number.isFinite(t) ? new Date(t).toISOString() : null;
}

function parseArg(name) {
  const a = process.argv.find(x => x.startsWith(name + "="));
  return a ? a.split("=").slice(1).join("=") : null;
}

function roundFt(x) {
  return Math.round(x * 1000) / 1000;
}

function yyyymmddUTC(d) {
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return `${y}${m}${day}`;
}

function addDaysUTC(d, days) {
  return new Date(d.getTime() + days * 86400 * 1000);
}

function startOfUTCDate(d) {
  return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate(), 0, 0, 0));
}

function parseNOAATimeToISO_UTC(t) {
  // NOAA predictions return "YYYY-MM-DD HH:MM" (no timezone)
  // We request time_zone=gmt so interpret as UTC and append Z.
  // Example: "2026-01-28 14:12" -> "2026-01-28T14:12:00Z"
  return t.replace(" ", "T") + ":00Z";
}

function classifyNAVD(ft, T) {
  let type = "Below";
  if (ft >= T.majorLow) type = "Major";
  else if (ft >= T.moderateLow) type = "Moderate";
  else if (ft >= T.minorLow) type = "Minor";
  return type;
}

// -------------------------
// USGS IV fetch (15-min-ish)
// -------------------------
async function fetchUSGSIV({ startISO, endISO }) {
  const url =
    "https://waterservices.usgs.gov/nwis/iv/?" +
    new URLSearchParams({
      format: "json",
      sites: SITE,
      parameterCd: PARAM,
      startDT: startISO,
      endDT: endISO,
      siteStatus: "all",
      agencyCd: "USGS"
    }).toString();

  const res = await fetch(url, { headers: { "User-Agent": "peaks-cache/2.0" } });
  if (!res.ok) throw new Error(`USGS IV fetch failed: ${res.status} ${res.statusText}`);
  const j = await res.json();

  const ts = j?.value?.timeSeries?.[0];
  const vals = ts?.values?.[0]?.value || [];

  const series = vals
    .map(v => ({ t: v.dateTime, ft: Number(v.value) }))
    .filter(p => p.t && Number.isFinite(p.ft));

  series.sort((a, b) => new Date(a.t) - new Date(b.t));
  return series;
}

// -------------------------
// NOAA "hilo" predictions fetch (chunked)
// -------------------------
async function fetchNOAAHiloPredictionsHighs({ startISO, endISO }) {
  const start = new Date(startISO);
  const end = new Date(endISO);
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) {
    throw new Error("Invalid startISO/endISO for NOAA predictions.");
  }

  // NOAA is generally happier with ~31-day windows. We'll chunk 30 days.
  const highs = [];
  let cur = startOfUTCDate(start);
  const endDay = startOfUTCDate(end);

  while (cur <= endDay) {
    const chunkEnd = addDaysUTC(cur, 30);
    const actualEnd = chunkEnd < endDay ? chunkEnd : endDay;

    const url =
      "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?" +
      new URLSearchParams({
        product: "predictions",
        application: "peaks-cache",
        format: "json",
        station: NOAA_STATION,
        time_zone: "gmt",
        units: "english",
        interval: "hilo",
        datum: "MLLW", // required by NOAA; crest TIMES are what we care about
        begin_date: yyyymmddUTC(cur),
        end_date: yyyymmddUTC(actualEnd)
      }).toString();

    const res = await fetch(url, { headers: { "User-Agent": "peaks-cache/2.0" } });
    if (!res.ok) throw new Error(`NOAA predictions fetch failed: ${res.status} ${res.statusText}`);

    const j = await res.json();
    const arr = Array.isArray(j?.predictions) ? j.predictions : [];

    for (const p of arr) {
      if (p?.type !== "H") continue; // highs only
      const iso = parseNOAATimeToISO_UTC(p.t);
      const ms = new Date(iso).getTime();
      if (!Number.isFinite(ms)) continue;
      highs.push({ t: new Date(ms).toISOString() });
    }

    cur = addDaysUTC(actualEnd, 1);
  }

  highs.sort((a, b) => new Date(a.t) - new Date(b.t));
  return highs;
}

// -------------------------
// Crest-anchored event builder
// -------------------------
function buildCrestAnchoredHighEvents({ series, predictedHighs, thresholdsNAVD88 }) {
  if (!Array.isArray(series) || !series.length) return [];
  if (!Array.isArray(predictedHighs) || !predictedHighs.length) return [];

  const w2 = CREST_WINDOW_HOURS * 3600 * 1000;
  const w1 = REQUIRE_WITHIN_HOURS * 3600 * 1000;

  const pts = [...series].sort((a, b) => new Date(a.t) - new Date(b.t));

  const out = [];
  let left = 0;

  for (const h of predictedHighs) {
    const crestISO = h.t;
    const crestMs = new Date(crestISO).getTime();
    if (!Number.isFinite(crestMs)) continue;

    // Advance left pointer to first point >= crest - 2h
    while (left < pts.length) {
      const tMs = new Date(pts[left].t).getTime();
      if (!Number.isFinite(tMs) || tMs < crestMs - w2) left++;
      else break;
    }

    let i = left;
    let hasWithin1h = false;
    let best = null;

    while (i < pts.length) {
      const tMs = new Date(pts[i].t).getTime();
      if (!Number.isFinite(tMs)) { i++; continue; }
      if (tMs > crestMs + w2) break;

      const dt = Math.abs(tMs - crestMs);
      if (dt <= w1) hasWithin1h = true;

      if (!best || pts[i].ft > best.ft) best = pts[i];
      i++;
    }

    // Your rule: if we do not have ANY observed values within ±1h, do not report anything
    if (!hasWithin1h) continue;
    if (!best) continue;

    const ft = Number(best.ft);
    out.push({
      t: new Date(best.t).toISOString(),     // observed time of window max
      ft: roundFt(ft),
      type: classifyNAVD(ft, thresholdsNAVD88),
      crest: new Date(crestISO).toISOString(), // predicted crest time (key)
      kind: "CrestHigh"
    });
  }

  return out;
}

// -------------------------
// Main update logic
// -------------------------
async function main() {
  const cache = loadJSON(CACHE_PATH);

  // Ensure required metadata exists (you already store these)
  cache.site = cache.site || SITE;
  cache.parameterCd = cache.parameterCd || PARAM;
  cache.datum = cache.datum || "NAVD88";
  cache.peakMinSepMinutes = cache.peakMinSepMinutes || PEAK_MIN_SEP_MINUTES;

  const THRESH_NAVD88 = cache?.thresholdsNAVD88 || null;
  if (!THRESH_NAVD88) {
    die(
      "Missing NAVD88 thresholds. Add thresholdsNAVD88 to data/peaks_navd88.json, e.g.\n" +
      '  "thresholdsNAVD88": {"minorLow": 4.19, "moderateLow": 5.19, "majorLow": 6.19}\n'
    );
  }

  // If method changed, clear events to avoid mixing old peak scheme with crest-anchored scheme
  if (cache.method !== METHOD) {
    console.log(`Method changed (${cache.method || "none"} -> ${METHOD}). Clearing events for clean rebuild.`);
    cache.method = METHOD;
    cache.events = [];
    // Leave lastProcessedISO as-is; you can run a backfill range to rebuild.
  }

  const backfillYear = parseArg("--backfill-year");
  const backfillFrom = parseArg("--backfill-from");
  const backfillTo = parseArg("--backfill-to");

  let startISO, endISO;

  if (backfillYear) {
    const y = Number(backfillYear);
    if (!Number.isFinite(y) || y < 1900 || y > 3000) die("Invalid --backfill-year=YYYY");
    startISO = new Date(Date.UTC(y, 0, 1, 0, 0, 0)).toISOString();
    endISO = new Date(Date.UTC(y + 1, 0, 1, 0, 0, 0)).toISOString();
    console.log(`Backfill year ${y}: ${startISO} → ${endISO}`);
  } else if (backfillFrom && backfillTo) {
    const y1 = Number(backfillFrom);
    const y2 = Number(backfillTo);
    if (!Number.isFinite(y1) || !Number.isFinite(y2)) die("Invalid --backfill-from / --backfill-to (must be years)");
    const lo = Math.min(y1, y2);
    const hi = Math.max(y1, y2);
    if (lo < 1900 || hi > 3000) die("Backfill range out of bounds.");
    startISO = new Date(Date.UTC(lo, 0, 1, 0, 0, 0)).toISOString();
    endISO = new Date(Date.UTC(hi + 1, 0, 1, 0, 0, 0)).toISOString();
    console.log(`Backfill years ${lo}–${hi}: ${startISO} → ${endISO}`);
  } else {
    const last = clampISO(cache.lastProcessedISO || "2000-01-01T00:00:00Z");
    if (!last) die("Cache lastProcessedISO is invalid ISO.");
    startISO = addHoursISO(last, -BUFFER_HOURS);
    endISO = isoNow();
    console.log(`Incremental: ${startISO} → ${endISO}`);
  }

  // 1) Fetch observed series from USGS
  const series = await fetchUSGSIV({ startISO, endISO });
  if (!series.length) {
    console.log("No series points returned; nothing to do.");
    return;
  }

  // 2) Fetch predicted high tide crest times from NOAA (pad window slightly)
  const predStartISO = addHoursISO(startISO, -3);
  const predEndISO = addHoursISO(endISO, +3);

  const predictedHighs = await fetchNOAAHiloPredictionsHighs({ startISO: predStartISO, endISO: predEndISO });
  if (!predictedHighs.length) {
    console.log("No NOAA predicted highs returned; nothing to do.");
    return;
  }

  // 3) Build crest-anchored events
  const crestHighs = buildCrestAnchoredHighEvents({
    series,
    predictedHighs,
    thresholdsNAVD88: THRESH_NAVD88
  });

  // 4) Merge/dedupe by crest time (stable key)
  const existing = Array.isArray(cache.events) ? cache.events : [];
  const byCrest = new Map();

  for (const e of existing) {
    if (e?.crest) byCrest.set(String(e.crest), e);
  }

  let added = 0;
  let updated = 0;

  for (const e of crestHighs) {
    const key = String(e.crest);
    const prev = byCrest.get(key);

    if (!prev) {
      existing.push(e);
      byCrest.set(key, e);
      added++;
      continue;
    }

    // Update if we now have a better observed max (or previous was missing/NaN)
    const prevFt = Number(prev.ft);
    const newFt = Number(e.ft);

    // If the old one exists but was based on sparse data and later we capture a higher max,
    // prefer the higher max.
    if (!Number.isFinite(prevFt) || (Number.isFinite(newFt) && newFt > prevFt)) {
      prev.t = e.t;
      prev.ft = e.ft;
      prev.type = e.type;
      prev.kind = e.kind;
      prev.crest = e.crest;
      updated++;
    }
  }

  // Keep chronological order
  existing.sort((a, b) => new Date(a.t) - new Date(b.t));
  cache.events = existing;

  // Advance lastProcessedISO to newest timestamp in the fetched USGS series
  const newestT = series[series.length - 1]?.t;
  if (newestT) cache.lastProcessedISO = new Date(newestT).toISOString();

  saveJSON(CACHE_PATH, cache);

  console.log(`Fetched USGS points:         ${series.length}`);
  console.log(`NOAA predicted HIGH crests:  ${predictedHighs.length}`);
  console.log(`Crest-anchored events built: ${crestHighs.length}`);
  console.log(`Events added:               ${added}`);
  console.log(`Events updated:             ${updated}`);
  console.log(`New lastProcessedISO:       ${cache.lastProcessedISO}`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
