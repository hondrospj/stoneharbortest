#!/usr/bin/env node
/**
 * Update PETSS forecast (ensemble mean) from NOMADS PETSS production tarballs.
 *
 * Outputs:
 *  - data/petss_forecast.csv   (time_utc_iso, twl_ft_mllw, tide_ft_mllw, surge_ft, src_time)
 *  - data/petss_forecast.json  ([{ t: "...Z", twl, tide, surge }...])
 *  - data/petss_meta.json      ({ stid, datum, run_dir, cycle, source_url, updated_utc, n_points })
 *
 * Env:
 *  - PETSS_STID  (required) e.g. "8531804"
 *  - PETSS_DATUM (optional; metadata only) e.g. "MLLW"
 */

"use strict";

const fs = require("fs");
const path = require("path");
const os = require("os");
const https = require("https");
const { execSync } = require("child_process");

const BASE = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/petss/prod/";

function log(...a) { console.log(...a); }
function die(msg, err) {
  console.error(msg);
  if (err) console.error(err.stack || err);
  process.exit(1);
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function fetchText(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { "User-Agent": "petss-forecast-updater" } }, (res) => {
      // handle redirects
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return resolve(fetchText(res.headers.location));
      }
      if (res.statusCode !== 200) {
        return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
      }
      let data = "";
      res.setEncoding("utf8");
      res.on("data", (chunk) => (data += chunk));
      res.on("end", () => resolve(data));
    }).on("error", reject);
  });
}

function downloadFile(url, outPath) {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(outPath);
    https.get(url, { headers: { "User-Agent": "petss-forecast-updater" } }, (res) => {
      // redirects
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        file.close(() => fs.unlinkSync(outPath));
        return resolve(downloadFile(res.headers.location, outPath));
      }
      if (res.statusCode !== 200) {
        file.close(() => fs.unlinkSync(outPath));
        return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
      }
      res.pipe(file);
      file.on("finish", () => file.close(resolve));
    }).on("error", (err) => {
      try { file.close(() => fs.unlinkSync(outPath)); } catch (_) {}
      reject(err);
    });
  });
}

function listLatestProdDir(html) {
  // Expect directory names like petss.20260131/
  const re = /petss\.(\d{8})\/?/g;
  const dates = [];
  let m;
  while ((m = re.exec(html)) !== null) dates.push(m[1]);
  if (!dates.length) throw new Error("Could not find petss.YYYYMMDD directories in NOMADS listing.");
  dates.sort(); // ascending
  const latest = dates[dates.length - 1];
  return `petss.${latest}/`;
}

function chooseCycleTarball(html) {
  // Prefer t18z, then t12z, t06z, t00z. We want the station CSV tarball.
  const preferred = ["t18z", "t12z", "t06z", "t00z"];
  for (const cyc of preferred) {
    const name = `petss.${cyc}.csv.tar.gz`;
    if (html.includes(name)) return name;
  }
  // Fallback: pick ANY petss.t??z.csv.tar.gz
  const m = html.match(/petss\.t\d{2}z\.csv\.tar\.gz/g);
  if (m && m.length) return m.sort().pop();
  throw new Error("Could not find any petss.t??z.csv.tar.gz tarball in run dir listing.");
}

function findFileRecursive(rootDir, filename) {
  const stack = [rootDir];
  while (stack.length) {
    const d = stack.pop();
    const ents = fs.readdirSync(d, { withFileTypes: true });
    for (const e of ents) {
      const p = path.join(d, e.name);
      if (e.isDirectory()) stack.push(p);
      else if (e.isFile() && e.name === filename) return p;
    }
  }
  return null;
}

function parseNomadsStationCsv(text, stid) {
  const lines = text.split(/\r?\n/).filter((l) => l.trim().length > 0);

  // Find header line containing TIME and TWL
  let headerIdx = -1;
  for (let i = 0; i < lines.length; i++) {
    const h = lines[i].trim();
    if (h.toUpperCase().includes("TIME") && h.toUpperCase().includes("TWL")) {
      headerIdx = i;
      break;
    }
  }
  if (headerIdx === -1) {
    throw new Error(`Could not find NOMADS header row with TIME/TWL for STID=${stid}`);
  }

  const header = lines[headerIdx].split(",").map((s) => s.trim().toUpperCase());
  const idxTIME = header.indexOf("TIME");
  const idxTWL = header.indexOf("TWL");
  const idxTIDE = header.indexOf("TIDE");
  const idxSURGE = header.indexOf("SURGE");

  if (idxTIME === -1 || idxTWL === -1) {
    throw new Error(`Header missing TIME or TWL for STID=${stid}. Header=${header.join("|")}`);
  }

  function parseNum(s) {
    const v = Number(String(s).trim());
    if (!Number.isFinite(v)) return null;
    // NOMADS uses 9999.000 as missing
    if (Math.abs(v - 9999) < 1e-6) return null;
    return v;
  }

  function parseTimeYYYYMMDDHHMM(s) {
    const t = String(s).trim();
    // Expect 12 digits: YYYYMMDDHHMM
    if (!/^\d{12}$/.test(t)) return null;
    const Y = Number(t.slice(0, 4));
    const M = Number(t.slice(4, 6));
    const D = Number(t.slice(6, 8));
    const h = Number(t.slice(8, 10));
    const m = Number(t.slice(10, 12));
    // UTC Date
    const dt = new Date(Date.UTC(Y, M - 1, D, h, m, 0));
    if (Number.isNaN(dt.getTime())) return null;
    return dt;
  }

  const rows = [];
  for (let i = headerIdx + 1; i < lines.length; i++) {
    const line = lines[i].trim();
    // skip separators or junk
    if (!/^\d{12}\s*,/.test(line)) continue;

    const parts = line.split(",").map((s) => s.trim());
    const dt = parseTimeYYYYMMDDHHMM(parts[idxTIME]);
    if (!dt) continue;

    const tide = idxTIDE >= 0 ? parseNum(parts[idxTIDE]) : null;
    const surge = idxSURGE >= 0 ? parseNum(parts[idxSURGE]) : null;
    const twl = parseNum(parts[idxTWL]);

    // Ensemble mean TWL is TWL when present; fallback to tide+surge if TWL missing but both exist
    const twlBest =
      twl != null ? twl :
      (tide != null && surge != null ? (tide + surge) : null);

    // For plotting: keep only points with a usable ensemble mean
    if (twlBest == null) continue;

    rows.push({
      t: dt.toISOString(),
      twl: Number(twlBest.toFixed(3)),
      tide: tide != null ? Number(tide.toFixed(3)) : null,
      surge: surge != null ? Number(surge.toFixed(3)) : null,
      src_time: String(parts[idxTIME]).trim()
    });
  }

  if (!rows.length) {
    throw new Error(
      `Parsed 0 usable rows (no valid TWL or TIDE+SURGE). ` +
      `This can happen if the file is mostly 9999 missing values.`
    );
  }

  // Sort time ascending
  rows.sort((a, b) => (a.t < b.t ? -1 : a.t > b.t ? 1 : 0));
  return rows;
}

async function main() {
  const stid = process.env.PETSS_STID?.trim();
  const datum = (process.env.PETSS_DATUM || "MLLW").trim();

  if (!stid) die("PETSS_STID is required (e.g., 8531804).");

  log("Running PETSS forecast updater via NOMADS…");
  log("STID:", stid);
  log("DATUM (for metadata only):", datum);
  log("Base:", BASE);

  // 1) Find latest run dir
  const baseHtml = await fetchText(BASE);
  const runDir = listLatestProdDir(baseHtml);
  log("Latest PETSS prod dir:", runDir);

  // 2) Choose cycle tarball
  const runHtml = await fetchText(BASE + runDir);
  const tarball = chooseCycleTarball(runHtml);
  log("Chosen cycle tarball:", tarball);

  const cycleMatch = tarball.match(/petss\.(t\d{2}z)\.csv\.tar\.gz/);
  const cycle = cycleMatch ? cycleMatch[1] : "unknown";

  const url = BASE + runDir + tarball;
  log("Downloading:", url);

  // 3) Download + extract
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "petss-"));
  const tgzPath = path.join(tmp, tarball);
  await downloadFile(url, tgzPath);

  const extractDir = path.join(tmp, "extract");
  ensureDir(extractDir);

  // Use system tar (available on ubuntu-latest)
  execSync(`tar -xzf "${tgzPath}" -C "${extractDir}"`, { stdio: "inherit" });

  // 4) Locate station file
  const stationFile = findFileRecursive(extractDir, `${stid}.csv`);
  if (!stationFile) {
    // Dump a quick directory tree depth 3 to help if this ever changes
    const listing = execSync(`find "${extractDir}" -maxdepth 4 -type f | head -n 200`, { encoding: "utf8" });
    throw new Error(`Could not find ${stid}.csv under extract dir.\nSample files:\n${listing}`);
  }
  log("Station CSV file:", stationFile);

  const stationText = fs.readFileSync(stationFile, "utf8");

  // Always write a debug snapshot of the station file (small and helpful)
  ensureDir("data");
  fs.writeFileSync("data/petss_station_debug.txt", stationText.split(/\r?\n/).slice(0, 250).join("\n") + "\n", "utf8");

  // 5) Parse NOMADS station CSV and keep ensemble mean TWL
  const rows = parseNomadsStationCsv(stationText, stid);

  // 6) Write outputs
  const outCsv = [
    "time_utc_iso,twl_ft_mllw,tide_ft_mllw,surge_ft,src_time",
    ...rows.map(r => {
      const tide = (r.tide == null ? "" : r.tide);
      const surge = (r.surge == null ? "" : r.surge);
      return `${r.t},${r.twl},${tide},${surge},${r.src_time}`;
    })
  ].join("\n") + "\n";

  fs.writeFileSync("data/petss_forecast.csv", outCsv, "utf8");
  fs.writeFileSync("data/petss_forecast.json", JSON.stringify(rows, null, 2) + "\n", "utf8");

  const meta = {
    stid,
    datum,
    run_dir: runDir.replace(/\/$/, ""),
    cycle,
    source_url: url,
    updated_utc: new Date().toISOString(),
    n_points: rows.length,
    notes: "Ensemble mean plotted as TWL (fallback to TIDE+SURGE when TWL missing)."
  };
  fs.writeFileSync("data/petss_meta.json", JSON.stringify(meta, null, 2) + "\n", "utf8");

  log(`Wrote ${rows.length} points → data/petss_forecast.csv + .json + meta`);
}

main().catch((e) => {
  try {
    ensureDir("data");
    fs.writeFileSync("data/petss_error.txt", String(e && (e.stack || e.message || e)) + "\n", "utf8");
  } catch (_) {}
  die("PETSS update failed:", e);
});
