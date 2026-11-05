#!/usr/bin/env python3
"""
ERA5 Pakistan bulk downloader / scheduler.

Pulls the complete ERA5 atmospheric record over Pakistan from the Copernicus
Climate Data Store (CDS):

  * reanalysis-era5-pressure-levels : all 16 variables x 37 pressure levels, hourly
  * reanalysis-era5-complete (MARS)  : all 16 model-level analysis params x 137 levels, hourly

Design (one invocation = one short "tick"; run it every ~30 min from GitHub
Actions, Windows Task Scheduler, cron, ...):

  1. Accept missing dataset licences through the API.
  2. Scan the CDS job list: previously accepted/successful requests that already
     cover a chunk are adopted or subtracted, so nothing is requested twice.
  3. Poll in-flight requests -- but only once they are older than
     ``min_age_before_poll_minutes`` (CDS needs 0.5-2 h); it never blocks
     waiting on CDS.
  4. Stream finished results straight from CDS to Google Drive (resumable
     upload, no local disk needed, MD5-verified) -- or to a local folder, or
     just list them for manual download.
  5. Submit new chunks while staying under the per-request CDS cost limits
     (checked with ``estimate_costs``; oversize chunks are split automatically)
     and under conservative per-user in-flight limits.

State is kept in ``state/`` (small JSON shards) so every chunk is requested
exactly once and the job is fully resumable.
"""
from __future__ import annotations

import argparse
import calendar
import csv
import datetime as dt
import hashlib
import json
import logging
import logging.handlers
import os
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Iterator

import requests

ROOT = Path(__file__).resolve().parent
STATE_DIR = ROOT / "state"
LOG_DIR = ROOT / "logs"
LOCK_FILE = ROOT / ".scheduler.lock"

PL = "reanalysis-era5-pressure-levels"
ML = "reanalysis-era5-complete"
HOURS = [f"{h:02d}:00" for h in range(24)]
IN_PROGRESS = ("accepted", "running")
GOOD_JOB_STATES = ("accepted", "running", "successful")

log = logging.getLogger("era5")


class StopSubmitting(Exception):
    """CDS told us to slow down (rate/queue limit) -- stop submitting this run."""


class DatasetQueueFull(Exception):
    """CDS queue limit reached for this specific dataset -- stop submitting to this dataset."""


class TooLarge(Exception):
    """Request exceeds the CDS cost limit -- needs splitting."""


# --------------------------------------------------------------------------- utils
def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso(t: dt.datetime | None = None) -> str:
    return (t or utcnow()).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(s: str | None) -> dt.datetime | None:
    if not s:
        return None
    s = s.rstrip("Z")
    try:
        t = dt.datetime.fromisoformat(s)
    except ValueError:
        return None
    return t if t.tzinfo else t.replace(tzinfo=dt.timezone.utc)


def minutes_since(s: str | None) -> float:
    t = parse_iso(s)
    return 1e9 if t is None else (utcnow() - t).total_seconds() / 60.0


def atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def human(n: float | int | None) -> str:
    if not n:
        return "0 B"
    n = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if n < 1024 or unit == "PB":
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def setup_logging(verbose: bool) -> None:
    LOG_DIR.mkdir(exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(message)s", "%Y-%m-%d %H:%M:%S")
    root = logging.getLogger()
    root.setLevel(logging.DEBUG if verbose else logging.INFO)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    fh = logging.handlers.RotatingFileHandler(LOG_DIR / "scheduler.log", maxBytes=5_000_000,
                                              backupCount=5, encoding="utf-8")
    fh.setFormatter(fmt)
    root.handlers[:] = [sh, fh]
    for noisy in ("urllib3", "multiurl", "ecmwf", "datapi"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def load_config() -> dict:
    return json.loads((ROOT / "config.json").read_text(encoding="utf-8"))


def load_secrets() -> dict:
    s: dict[str, str] = {}
    f = ROOT / "secrets.local.json"
    if f.exists():
        s.update({k: v for k, v in json.loads(f.read_text(encoding="utf-8")).items() if v})
    rc = Path.home() / ".cdsapirc"
    if rc.exists() and "CDSAPI_KEY" not in s:
        for line in rc.read_text().splitlines():
            if line.strip().startswith("key:"):
                s.setdefault("CDSAPI_KEY", line.split(":", 1)[1].strip())
            if line.strip().startswith("url:"):
                s.setdefault("CDSAPI_URL", line.split(":", 1)[1].strip())
    for k in ("CDSAPI_KEY", "CDSAPI_URL", "GDRIVE_CLIENT_ID", "GDRIVE_CLIENT_SECRET", "GDRIVE_REFRESH_TOKEN"):
        if os.environ.get(k):
            s[k] = os.environ[k].strip()
    return s


# --------------------------------------------------------------------------- MARS-style list helpers
def mars_list(v: Any) -> list[str]:
    """Expand MARS lists like '1/to/137', '00/to/23/by/1', '2020-01-01/to/2020-01-31', 'a/b/c'."""
    if v is None:
        return []
    if isinstance(v, (list, tuple)):
        out: list[str] = []
        for x in v:
            out.extend(mars_list(x))
        return out
    parts = [p.strip() for p in str(v).split("/") if p.strip()]
    low = [p.lower() for p in parts]
    if len(parts) >= 3 and low[1] == "to":
        a, b = parts[0], parts[2]
        step = parts[4] if len(parts) >= 5 and low[3] == "by" else "1"
        if "-" in a and len(a) >= 8:  # dates
            d0, d1 = dt.date.fromisoformat(a[:10]), dt.date.fromisoformat(b[:10])
            n, out = int(step), []
            while d0 <= d1:
                out.append(d0.isoformat())
                d0 += dt.timedelta(days=n)
            return out
        ia, ib, st = int(float(a.split(":")[0])), int(float(b.split(":")[0])), int(float(step))
        return [str(i) for i in range(ia, ib + 1, st)]
    return parts


def norm_param(p: str) -> str:
    p = str(p).strip()
    return p.split(".")[0] if p.replace(".", "").isdigit() else p.lower()


def norm_time(t: str) -> str:
    t = str(t).strip()
    return f"{int(t.split(':')[0]):02d}" if t.split(":")[0].isdigit() else t


def area_of(v: Any) -> list[float] | None:
    if v is None or v == "" or v == []:
        return None
    if isinstance(v, str):
        v = [x for x in v.replace(",", "/").split("/") if x.strip()]
    return [float(x) for x in v]


def area_contains(outer: list[float] | None, inner: list[float]) -> bool:
    if outer is None:  # no area => global
        return True
    n, w, s, e = outer
    return n >= inner[0] and w <= inner[1] and s <= inner[2] and e >= inner[3]


# --------------------------------------------------------------------------- chunk planning
class Planner:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.area = [float(x) for x in cfg["area_nwse"]]
        self.start = cfg["start"]
        self.end = self._resolve_end(cfg.get("end", "auto"))
        self.order = cfg.get("order", "newest_year_first")
        self.newest_first = self.order == "newest_first"
        self.pl = cfg["datasets"]["pressure_levels"]
        self.ml = cfg["datasets"]["model_levels"]

    def _resolve_end(self, end: str) -> str:
        if end and end != "auto":
            return end
        ref = dt.date.today() - dt.timedelta(days=int(self.cfg.get("auto_end_lag_days", 5)))
        y, m = ref.year, ref.month
        if ref.day < calendar.monthrange(y, m)[1]:
            y, m = (y - 1, 12) if m == 1 else (y, m - 1)
        return f"{y:04d}-{m:02d}"

    def months(self) -> list[tuple[int, int]]:
        y, m = map(int, self.start.split("-"))
        ye, me = map(int, self.end.split("-"))
        out = []
        while (y, m) <= (ye, me):
            out.append((y, m))
            y, m = (y + 1, 1) if m == 12 else (y, m + 1)
        if self.order == "newest_year_first":
            by_year: dict[int, list[tuple[int, int]]] = {}
            for ym in out:
                by_year.setdefault(ym[0], []).append(ym)
            return [ym for yr in sorted(by_year, reverse=True) for ym in by_year[yr]]
        return out[::-1] if self.newest_first else out

    # ---- requests
    def pl_request(self, y: int, m: int, variables: list[str], days: list[str] | None = None) -> dict:
        nd = calendar.monthrange(y, m)[1]
        return {
            "product_type": ["reanalysis"],
            "variable": list(variables),
            "year": [f"{y:04d}"],
            "month": [f"{m:02d}"],
            "day": days if days is not None else [f"{d:02d}" for d in range(1, nd + 1)],
            "time": list(HOURS),
            "pressure_level": list(self.pl["pressure_levels"]),
            "data_format": "grib",
            "download_format": "unarchived",
            "area": list(self.area),
        }

    def ml_request(self, date: str, time_: str | None = None, param: str | None = None) -> dict:
        return {
            "class": "ea", "expver": "1", "stream": "oper", "type": "an", "levtype": "ml",
            "levelist": self.ml["levelist"],
            "param": param or self.ml["param"],
            "date": date,
            "time": time_ or self.ml["time"],
            "grid": self.cfg.get("grid", "0.25/0.25"),
            "area": "/".join(f"{x:g}" for x in self.area),
            "format": "grib",
        }

    # ---- chunk descriptors
    def chunk(self, key: str) -> dict:
        """Rebuild the default descriptor for a parent key."""
        kind, rest = key.split(":", 1)
        if kind == "pl":
            ym, g = rest.split(":")
            y, m = map(int, ym.split("-"))
            grp = self.pl["variable_groups"][int(g[1:])]
            return {"key": key, "ds": PL, "year": y, "request": self.pl_request(y, m, grp["variables"]),
                    "name": f"era5_pl_{y:04d}{m:02d}_{grp['tag']}.grib",
                    "folder": ["pressure_levels", f"{y:04d}"]}
        if kind == "ml":
            y = int(rest[:4])
            return {"key": key, "ds": ML, "year": y, "request": self.ml_request(rest),
                    "name": f"era5_ml_{rest.replace('-', '')}.grib",
                    "folder": ["model_levels", f"{y:04d}"]}
        raise ValueError(key)

    def iter_keys(self, ds: str) -> Iterator[str]:
        for (y, m) in self.months():
            if ds == PL:
                for gi in range(len(self.pl["variable_groups"])):
                    yield f"pl:{y:04d}-{m:02d}:g{gi:02d}"
            else:
                days = range(1, calendar.monthrange(y, m)[1] + 1)
                for d in (reversed(days) if self.newest_first else days):
                    yield f"ml:{y:04d}-{m:02d}-{d:02d}"

    def total(self, ds: str) -> int:
        if ds == PL:
            return len(self.months()) * len(self.pl["variable_groups"])
        return sum(calendar.monthrange(y, m)[1] for y, m in self.months())

    @staticmethod
    def split(ds: str, req: dict) -> list[dict] | None:
        """Split an oversize request into two halves (variables > days > times / times > params)."""
        if ds == PL:
            for dim in ("variable", "day", "time", "pressure_level"):
                vals = req[dim]
                if len(vals) > 1:
                    h = len(vals) // 2
                    return [dict(req, **{dim: vals[:h]}), dict(req, **{dim: vals[h:]})]
            return None
        times = [f"{int(t):02d}" for t in mars_list(req["time"])]
        if len(times) > 1:
            h = len(times) // 2
            return [dict(req, time="/".join(times[:h])), dict(req, time="/".join(times[h:]))]
        params = mars_list(req["param"])
        if len(params) > 1:
            h = len(params) // 2
            return [dict(req, param="/".join(params[:h])), dict(req, param="/".join(params[h:]))]
        return None


# --------------------------------------------------------------------------- coverage by pre-existing jobs
def coverage_record(ds: str, req: dict) -> dict:
    """Compact description of what a (foreign) CDS request contains."""
    if ds == PL:
        return {
            "product": [str(x) for x in mars_list(req.get("product_type", ["reanalysis"]))],
            "vars": sorted(set(mars_list(req.get("variable")))),
            "years": sorted({f"{int(x):04d}" for x in mars_list(req.get("year"))}),
            "months": sorted({f"{int(x):02d}" for x in mars_list(req.get("month"))}),
            "days": sorted({f"{int(x):02d}" for x in mars_list(req.get("day"))}),
            "times": sorted({norm_time(x) for x in mars_list(req.get("time"))}),
            "levels": sorted({str(int(float(x))) for x in mars_list(req.get("pressure_level"))}),
            "area": area_of(req.get("area")),
        }
    return {
        "type": str(req.get("type", "an")).lower(), "levtype": str(req.get("levtype", "")).lower(),
        "dates": sorted(set(d[:10] for d in mars_list(req.get("date")))),
        "params": sorted({norm_param(p) for p in mars_list(req.get("param"))}),
        "levels": sorted({str(int(float(x))) for x in mars_list(req.get("levelist"))}),
        "times": sorted({norm_time(x) for x in mars_list(req.get("time"))}),
        "area": area_of(req.get("area")),
    }


def pl_covered_days(cov: dict, req: dict, area: list[float]) -> set[str]:
    """Days of a PL chunk request that a foreign request fully covers."""
    if "reanalysis" not in cov.get("product", ["reanalysis"]):
        return set()
    if not set(req["variable"]) <= set(cov["vars"]):
        return set()
    if req["year"][0] not in cov["years"] or req["month"][0] not in cov["months"]:
        return set()
    if not {norm_time(t) for t in req["time"]} <= set(cov["times"]):
        return set()
    if not set(req["pressure_level"]) <= set(cov["levels"]):
        return set()
    if not area_contains(cov["area"], area):
        return set()
    return set(req["day"]) & set(cov["days"])


def ml_covered(cov: dict, req: dict, area: list[float]) -> bool:
    if cov.get("type") != "an" or cov.get("levtype") != "ml":
        return False
    if not {d[:10] for d in mars_list(req["date"])} <= set(cov["dates"]):
        return False
    return ({norm_param(p) for p in mars_list(req["param"])} <= set(cov["params"])
            and {str(int(x)) for x in mars_list(req["levelist"])} <= set(cov["levels"])
            and {norm_time(t) for t in mars_list(req["time"])} <= set(cov["times"])
            and area_contains(cov["area"], area))


def same_request(ours: dict, theirs: dict) -> bool:
    """True if a CDS job's stored request equals ours on every key we send."""
    def n(v: Any) -> list[str]:
        out = []
        for x in mars_list(v):
            try:
                out.append(repr(float(x)))
            except ValueError:
                out.append(str(x).lower())
        return sorted(out)
    for k, v in ours.items():
        if k in ("data_format", "download_format", "format"):
            continue
        if k not in theirs or n(v) != n(theirs[k]):
            return False
    return True


# --------------------------------------------------------------------------- state
class State:
    """Chunk state, sharded per dataset+year so commits stay small."""

    def __init__(self, root: Path):
        self.root = root
        self.cdir = root / "chunks"
        self.meta: dict = {}
        self.entries: dict[str, dict] = {}
        self.dirty: set[str] = set()
        self.meta_dirty = False
        mf = root / "meta.json"
        if mf.exists():
            self.meta = json.loads(mf.read_text(encoding="utf-8"))
        if self.cdir.exists():
            for f in sorted(self.cdir.glob("*.json")):
                self.entries.update(json.loads(f.read_text(encoding="utf-8")))
        self.by_rid = {e["rid"]: k for k, e in self.entries.items() if e.get("rid")}

    @staticmethod
    def shard(key: str) -> str:
        kind, rest = key.split(":", 1)
        return f"{kind}_{rest[:4]}"

    def get(self, key: str) -> dict | None:
        return self.entries.get(key)

    def put(self, key: str, entry: dict) -> None:
        self.entries[key] = entry
        if entry.get("rid"):
            self.by_rid[entry["rid"]] = key
        self.dirty.add(self.shard(key))

    def touch_meta(self) -> None:
        self.meta_dirty = True

    def save(self) -> None:
        groups: dict[str, dict] = {}
        for k, e in self.entries.items():
            sh = self.shard(k)
            if sh in self.dirty:
                groups.setdefault(sh, {})[k] = e
        for sh, data in groups.items():
            atomic_write(self.cdir / f"{sh}.json",
                         json.dumps(dict(sorted(data.items())), indent=0, separators=(",", ":")))
        self.dirty.clear()
        if self.meta_dirty:
            atomic_write(self.root / "meta.json", json.dumps(self.meta, indent=1, sort_keys=True))
            self.meta_dirty = False


# --------------------------------------------------------------------------- Google Drive (OAuth refresh token, resumable streaming upload)
class Drive:
    API = "https://www.googleapis.com/drive/v3"
    UPLOAD = "https://www.googleapis.com/upload/drive/v3/files"
    CHUNK = 64 * 1024 * 1024  # multiple of 256 KiB

    def __init__(self, client_id: str, client_secret: str, refresh_token: str):
        self.cid, self.csec, self.rtok = client_id, client_secret, refresh_token
        self._tok: str | None = None
        self._exp = 0.0
        self.s = requests.Session()
        self.folder_cache: dict[str, str] = {}

    def token(self) -> str:
        if not self._tok or time.time() > self._exp - 300:
            r = requests.post("https://oauth2.googleapis.com/token", timeout=60, data={
                "client_id": self.cid, "client_secret": self.csec,
                "refresh_token": self.rtok, "grant_type": "refresh_token"})
            if r.status_code != 200:
                raise RuntimeError(f"Google token refresh failed: {r.status_code} {r.text[:300]}")
            j = r.json()
            self._tok, self._exp = j["access_token"], time.time() + int(j.get("expires_in", 3600))
        return self._tok

    def call(self, method: str, url: str, ok=(200, 201, 204), **kw) -> requests.Response:
        base_headers = dict(kw.pop("headers", None) or {})
        timeout = kw.pop("timeout", 120)
        for attempt in range(8):
            headers = dict(base_headers, Authorization=f"Bearer {self.token()}")
            try:
                r = self.s.request(method, url, headers=headers, timeout=timeout, **kw)
            except requests.RequestException as e:
                log.warning("Drive %s %s network error: %s", method, url[:80], e)
                time.sleep(min(60, 2 ** attempt))
                continue
            if r.status_code in ok:
                return r
            retryable = (r.status_code == 401 or r.status_code == 429 or r.status_code >= 500
                         or (r.status_code == 403 and "ratelimit" in r.text.lower()))
            if not retryable:
                raise RuntimeError(f"Drive {method} {url[:80]} -> {r.status_code}: {r.text[:400]}")
            if r.status_code == 401:
                self._tok = None
            time.sleep(min(60, 2 ** attempt))
        raise RuntimeError(f"Drive {method} {url[:80]} failed after retries")

    def about(self) -> dict:
        return self.call("GET", f"{self.API}/about", params={"fields": "user(emailAddress),storageQuota"}).json()

    def free_bytes(self) -> float:
        q = self.about().get("storageQuota", {})
        if "limit" not in q:
            return float("inf")
        return float(q["limit"]) - float(q.get("usage", 0))

    def find(self, parent: str, name: str, folder: bool) -> dict | None:
        q = f"name = '{name}' and '{parent}' in parents and trashed = false"
        q += " and mimeType " + ("=" if folder else "!=") + " 'application/vnd.google-apps.folder'"
        r = self.call("GET", f"{self.API}/files", params={"q": q, "fields": "files(id,name,size,md5Checksum)",
                                                          "pageSize": 10, "spaces": "drive"})
        files = r.json().get("files", [])
        return files[0] if files else None

    def ensure_folder(self, parts: list[str]) -> str:
        path, parent = "", "root"
        for p in parts:
            path += "/" + p
            if path not in self.folder_cache:
                f = self.find(parent, p, folder=True)
                if f is None:
                    f = self.call("POST", f"{self.API}/files", params={"fields": "id"}, json={
                        "name": p, "mimeType": "application/vnd.google-apps.folder", "parents": [parent]}).json()
                    log.info("Drive: created folder %s", path)
                self.folder_cache[path] = f["id"]
            parent = self.folder_cache[path]
        return parent

    def delete(self, file_id: str) -> None:
        self.call("DELETE", f"{self.API}/files/{file_id}")

    # ---- streaming upload
    @staticmethod
    def _source(url: str, start: int) -> Iterator[bytes]:
        """Yield bytes of url from offset `start`, transparently reconnecting with HTTP Range."""
        pos, fails = start, 0
        while True:
            try:
                with requests.get(url, stream=True, timeout=(30, 300),
                                  headers={"Range": f"bytes={pos}-"} if pos else {}) as r:
                    if r.status_code in (403, 404, 410):
                        raise FileNotFoundError(f"CDS result no longer available ({r.status_code})")
                    r.raise_for_status()
                    if pos and r.status_code != 206:
                        raise RuntimeError("server ignored Range header")
                    for blk in r.iter_content(1024 * 1024):
                        if blk:
                            pos += len(blk)
                            fails = 0
                            yield blk
                    return
            except FileNotFoundError:
                raise
            except (requests.RequestException, RuntimeError) as e:
                fails += 1
                if fails > 10:
                    raise
                log.warning("download interrupted at %s (%s); resuming", human(pos), e)
                time.sleep(min(60, 3 * fails))

    def upload_from_url(self, parent: str, name: str, url: str, size: int) -> dict:
        init = self.call("POST", self.UPLOAD, params={"uploadType": "resumable"},
                         headers={"X-Upload-Content-Type": "application/octet-stream",
                                  "X-Upload-Content-Length": str(size),
                                  "Content-Type": "application/json; charset=UTF-8"},
                         json={"name": name, "parents": [parent]})
        session = init.headers["Location"]
        if size <= 0:
            raise RuntimeError("unknown/zero result size")
        src = self._source(url, 0)
        buf, buf_start, t0 = bytearray(), 0, time.time()
        last_log = 0.0
        while True:
            target = min(self.CHUNK, size - buf_start)
            while len(buf) < target:
                try:
                    buf.extend(next(src))
                except StopIteration:
                    raise RuntimeError(f"source ended early at {buf_start + len(buf)} of {size} bytes")
            data = bytes(buf[:target])
            end = buf_start + target - 1
            r = None
            for attempt in range(10):
                try:
                    r = self.s.put(session, data=data, timeout=(30, 600), headers={
                        "Authorization": f"Bearer {self.token()}",
                        "Content-Length": str(target),
                        "Content-Range": f"bytes {buf_start}-{end}/{size}"})
                except requests.RequestException as e:
                    log.warning("Drive chunk upload error (%s); querying session", e)
                    r = None
                if r is not None and r.status_code in (200, 201, 308):
                    break
                if r is not None and r.status_code in (404, 410):
                    raise RuntimeError("Drive upload session expired")
                time.sleep(min(60, 2 ** attempt))
                # ask Drive how much it has
                try:
                    r = self.s.put(session, timeout=60, headers={
                        "Authorization": f"Bearer {self.token()}", "Content-Length": "0",
                        "Content-Range": f"bytes */{size}"})
                    if r.status_code in (200, 201, 308):
                        break
                except requests.RequestException:
                    pass
            else:
                raise RuntimeError("Drive chunk upload failed repeatedly")
            if r.status_code in (200, 201):
                j = r.json()
                fid = j["id"]
                meta = self.call("GET", f"{self.API}/files/{fid}", params={"fields": "id,size,md5Checksum"}).json()
                el = time.time() - t0
                log.info("  uploaded %s (%s) in %.0fs, %.1f MB/s", name, human(size), el, size / 1e6 / max(el, 1))
                return meta
            rng = r.headers.get("Range")  # e.g. 'bytes=0-1048575'
            committed = int(rng.split("-")[1]) + 1 if rng else 0
            if committed < buf_start:
                raise RuntimeError("Drive lost already-committed data; restarting upload")
            drop = committed - buf_start
            del buf[:drop]
            buf_start = committed
            if time.time() - last_log > 30:
                last_log = time.time()
                log.info("  %s: %s / %s", name, human(buf_start), human(size))


# --------------------------------------------------------------------------- scheduler
class Scheduler:
    def __init__(self, cfg: dict, secrets: dict, dry_run: bool = False, max_minutes: float | None = None):
        from ecmwf.datastores import Client

        self.cfg, self.dry = cfg, dry_run
        if not secrets.get("CDSAPI_KEY"):
            raise SystemExit("No CDS API key: set CDSAPI_KEY (env/GitHub secret) or put it in secrets.local.json")
        self.client = Client(url=secrets.get("CDSAPI_URL") or cfg["cds_url"], key=secrets["CDSAPI_KEY"],
                             progress=False, maximum_tries=3, retry_after=20, sleep_max=60)
        self.planner = Planner(cfg)
        self.state = State(STATE_DIR)
        self.deadline = time.time() + 60 * (max_minutes or cfg.get("max_run_minutes", 25))
        storage = cfg.get("storage", "auto")
        have_drive = all(secrets.get(k) for k in ("GDRIVE_CLIENT_ID", "GDRIVE_CLIENT_SECRET", "GDRIVE_REFRESH_TOKEN"))
        if storage == "auto":
            storage = "drive" if have_drive else "none"
        if storage == "drive" and not have_drive:
            raise SystemExit("storage=drive but GDRIVE_CLIENT_ID / GDRIVE_CLIENT_SECRET / GDRIVE_REFRESH_TOKEN missing")
        self.storage = storage
        self.drive = Drive(secrets["GDRIVE_CLIENT_ID"], secrets["GDRIVE_CLIENT_SECRET"],
                           secrets["GDRIVE_REFRESH_TOKEN"]) if storage == "drive" else None
        self.datasets = {}
        for name, dcfg in cfg["datasets"].items():
            if dcfg.get("enabled", True):
                self.datasets[dcfg["collection"]] = dcfg
        self.halt_submissions: str | None = None

    # ---------------------------------------------------------------- helpers
    def time_left(self) -> float:
        return self.deadline - time.time()

    def descriptor(self, key: str) -> dict:
        """Descriptor for any key (parents rebuilt from planner, children stored in state)."""
        e = self.state.get(key) or {}
        if "parent" in e:
            parent = self.descriptor(e["parent"])
            return {"key": key, "ds": parent["ds"], "year": parent["year"], "request": e["req"],
                    "name": e["name"], "folder": parent["folder"]}
        d = self.planner.chunk(key)
        if e.get("req"):
            d["request"] = e["req"]
        return d

    def counts(self) -> dict[str, dict[str, int]]:
        out: dict[str, dict[str, int]] = {}
        for k, e in self.state.entries.items():
            ds = PL if k.startswith("pl:") else ML
            out.setdefault(ds, {}).setdefault(e["s"], 0)
            out[ds][e["s"]] += 1
        return out

    # ---------------------------------------------------------------- 1. licences
    def ensure_licences(self) -> None:
        if minutes_since(self.state.meta.get("licences_checked")) < 60 * self.cfg.get("licence_check_hours", 24):
            return
        accepted = {(l["id"], int(l["revision"])) for l in self.client.get_accepted_licences()}
        all_ok = True
        for ds in self.datasets:
            col = self.client.get_collection(ds).json
            for link in col.get("links", []):
                if link.get("rel") != "license":
                    continue
                lid, rev = link["id"], int(link.get("rev") or link.get("revision") or 1)
                if (lid, rev) in accepted:
                    continue
                try:
                    if not self.dry:
                        self.client.accept_licence(lid, rev)
                    log.info("Accepted licence %s (rev %s) for %s via API", lid, rev, ds)
                except Exception as e:  # noqa: BLE001
                    all_ok = False
                    log.error("Could not accept licence %s for %s automatically (%s). Accept it manually at "
                              "https://cds.climate.copernicus.eu/datasets/%s?tab=download#manage-licences", lid, ds, e, ds)
        if all_ok:
            self.state.meta["licences_checked"] = iso()
            self.state.touch_meta()

    # ---------------------------------------------------------------- 2. scan job list (foreign jobs / adoption)
    def scan_jobs(self) -> None:
        meta = self.state.meta
        foreign: dict = meta.setdefault("foreign_jobs", {})
        last = meta.get("jobs_scanned_until")
        newest = last
        page = self.client.get_jobs(limit=100, sortby="-created")
        n_new = 0
        while page is not None:
            stop = False
            for jb in page.json.get("jobs", []):
                created = jb.get("created", "")
                rid = jb["jobID"]
                if rid in self.state.by_rid:
                    k_own = self.state.by_rid[rid]
                    e_own = self.state.entries.get(k_own, {})
                    if e_own.get("s") == "submitted" and jb.get("status") in ("rejected", "failed"):
                        log.warning("%s: CDS job %s is %s (detected during job scan); resetting for retry",
                                    k_own, rid, jb.get("status"))
                        e_own.pop("rid", None)
                        e_own["s"] = "pending"
                        e_own["att"] = max(0, e_own.get("att", 1) - 1)
                        self.state.put(k_own, e_own)
                    continue
                if last and created <= last:
                    stop = True
                    break
                newest = max(newest or "", created)
                if rid in foreign or jb.get("processID") not in self.datasets:
                    continue
                if jb.get("status") not in GOOD_JOB_STATES:
                    continue
                try:
                    req = self.client.get_remote(rid).request
                except Exception as e:  # noqa: BLE001
                    log.warning("Could not read request of job %s: %s", rid, e)
                    continue
                asset = jb.get("metadata", {}).get("results", {}).get("asset", {}).get("value", {})
                foreign[rid] = {"ds": jb["processID"], "status": jb["status"], "created": created,
                                "fin": jb.get("finished"), "url": asset.get("href"),
                                "size": asset.get("file:size"), "md5": asset.get("file:checksum"),
                                "cov": coverage_record(jb["processID"], req), "req": req}
                n_new += 1
            if stop:
                break
            page = page.next
        if newest != last:
            meta["jobs_scanned_until"] = newest
        # refresh foreign jobs still in progress; forget failed ones
        for rid, fj in list(foreign.items()):
            if fj["status"] in IN_PROGRESS:
                try:
                    st = self.client.get_remote(rid).status
                except Exception:  # noqa: BLE001
                    st = "deleted"
                if st not in GOOD_JOB_STATES:
                    log.info("Pre-existing job %s is now %s; its coverage is dropped", rid, st)
                    del foreign[rid]
                else:
                    fj["status"] = st
        if n_new:
            log.info("Found %d pre-existing CDS job(s) relevant to these datasets", n_new)
        self.state.touch_meta()

    def apply_prior_jobs(self, d: dict) -> tuple[str, dict]:
        """Decide how a chunk relates to pre-existing CDS jobs.

        Returns (action, info):
          ('adopt', {rid,status}) | ('covered', {by}) | ('split_cov', {}) | ('trim', {days, excluded, by}) | ('new', {}).
        """
        ds, req = d["ds"], d["request"]
        for rid, fj in self.state.meta.get("foreign_jobs", {}).items():
            if fj["ds"] == ds and fj.get("req") and same_request(req, fj["req"]) and not fj.get("adopted"):
                return "adopt", {"rid": rid, "status": fj["status"]}
        if ds == PL:
            per_var_days: dict[str, set[str]] = {v: set() for v in req["variable"]}
            per_var_by: dict[str, list[str]] = {v: [] for v in req["variable"]}
            for rid, fj in self.state.meta.get("foreign_jobs", {}).items():
                if fj["ds"] != ds:
                    continue
                for v in req["variable"]:
                    days = pl_covered_days(fj["cov"], dict(req, variable=[v]), self.planner.area)
                    if days:
                        per_var_days[v] |= days
                        if rid not in per_var_by[v]:
                            per_var_by[v].append(rid)
            cov_sets = [frozenset(s) for s in per_var_days.values()]
            if len(set(cov_sets)) > 1:
                return "split_cov", {}
            trimmed = set(cov_sets[0]) if cov_sets else set()
            by = sorted({r for rs in per_var_by.values() for r in rs})
            if trimmed:
                remaining = [x for x in req["day"] if x not in trimmed]
                if not remaining:
                    return "covered", {"by": by}
                return "trim", {"days": remaining, "excluded": sorted(trimmed), "by": by}
        else:
            for rid, fj in self.state.meta.get("foreign_jobs", {}).items():
                if fj["ds"] == ds and ml_covered(fj["cov"], req, self.planner.area):
                    return "covered", {"by": [rid]}
        return "new", {}

    # ---------------------------------------------------------------- 3. poll
    def poll(self) -> None:
        min_age = self.cfg.get("min_age_before_poll_minutes", 90)
        every = self.cfg.get("poll_interval_minutes", 25)
        for key, e in sorted(self.state.entries.items()):
            if e["s"] != "submitted":
                continue
            if minutes_since(e.get("sub")) < min_age or minutes_since(e.get("chk")) < every:
                continue
            try:
                remote = self.client.get_remote(e["rid"])
                st = remote.status
            except requests.HTTPError as ex:
                if ex.response is not None and ex.response.status_code == 404:
                    log.warning("%s: CDS request %s vanished; will resubmit", key, e["rid"])
                    self._fail(key, e, "request vanished from CDS")
                    continue
                log.warning("%s: poll error %s", key, ex)
                continue
            except Exception as ex:  # noqa: BLE001
                log.warning("%s: poll error %s", key, ex)
                continue
            e["chk"] = iso()
            if st in IN_PROGRESS:
                log.info("%s: still %s at CDS (age %.0f min)", key, st, minutes_since(e.get("sub")))
                self.state.put(key, e)
            elif st == "successful":
                try:
                    res = remote.get_results()
                    asset = res.json.get("asset", {}).get("value", {})
                    e.update(s="ready", url=res.location, size=int(res.content_length or asset.get("file:size") or 0),
                             md5=asset.get("file:checksum"), fin=iso())
                    log.info("%s: READY at CDS (%s)", key, human(e["size"]))
                    self.state.put(key, e)
                except Exception as ex:  # noqa: BLE001
                    log.warning("%s: could not get results: %s", key, ex)
            else:
                msg = st
                try:
                    remote.get_results()
                except Exception as ex:  # noqa: BLE001
                    msg = f"{st}: {str(ex)[:500]}"
                if any(s in msg.lower() for s in ("cost limit", "too large", "exceed", "request is too")):
                    log.warning("%s: rejected as too large -> splitting", key)
                    self._split(key, e)
                else:
                    log.warning("%s: CDS request %s %s", key, e["rid"], msg)
                    self._fail(key, e, msg)
        self.state.save()

    def _fail(self, key: str, e: dict, msg: str) -> None:
        e["err"] = msg[:600]
        e.pop("rid", None)
        if "queued requests" in msg.lower():
            e["s"] = "pending"
            e["att"] = max(0, e.get("att", 1) - 1)
            self.state.put(key, e)
            return
        e["att"] = e.get("att", 0)
        if e["att"] >= self.cfg.get("max_attempts", 4):
            e["s"] = "dead"
            log.error("%s: giving up after %d attempts (use `reset %s` to retry)", key, e["att"], key)
        else:
            e["s"] = "failed"
            e["retry_at"] = iso(utcnow() + dt.timedelta(minutes=self.cfg.get("retry_backoff_minutes", 120) * e["att"]))
        self.state.put(key, e)

    def _split(self, key: str, e: dict) -> list[str]:
        d = self.descriptor(key)
        halves = Planner.split(d["ds"], d["request"])
        if not halves:
            self._fail(key, e, "too large and cannot be split further")
            return []
        kids = []
        for i, req in enumerate(halves, 1):
            ck = f"{key}:p{i}"
            stem, ext = os.path.splitext(d["name"])
            self.state.put(ck, {"s": "pending", "parent": key, "req": req, "name": f"{stem}_p{i}{ext}", "att": 0})
            kids.append(ck)
        e.update(s="split", children=kids)
        e.pop("rid", None)
        self.state.put(key, e)
        log.info("%s split into %s", key, ", ".join(kids))
        return kids

    # ---------------------------------------------------------------- 4. transfer
    def transfer(self) -> None:
        ready = sorted((k for k, e in self.state.entries.items() if e["s"] == "ready"),
                       key=lambda k: self.state.entries[k].get("fin", ""))
        if self.storage == "none":
            self.write_manual_list()
            return
        prior_ready = [(rid, fj) for rid, fj in sorted(self.state.meta.get("foreign_jobs", {}).items())
                       if fj.get("status") == "successful" and fj.get("url") and not fj.get("pulled")]
        if not ready and not prior_ready:
            return
        if self.drive:
            free = self.drive.free_bytes()
            log.info("Google Drive free space: %s", "unlimited" if free == float("inf") else human(free))
        margin = self.cfg.get("drive_free_space_margin_gb", 2) * 1024 ** 3
        if self.drive and prior_ready:
            for rid, fj in prior_ready:
                if self.time_left() < 120 or self.dry:
                    break
                sz = int(fj.get("size") or 0)
                if sz and free >= sz + margin:
                    try:
                        folder = self.drive.ensure_folder([self.cfg.get("drive_root_folder", "ERA5_Pakistan"), "prior_cds_jobs"])
                        fname = f"prior_{fj['ds']}_{rid[:8]}.grib"
                        log.info("prior_job:%s: streaming CDS -> Drive prior_cds_jobs/%s (%s)", rid[:8], fname, human(sz))
                        meta = self.drive.upload_from_url(folder, fname, fj["url"], sz)
                        fj["pulled"] = True
                        fj["fid"] = meta["id"]
                        self.state.touch_meta()
                        self.state.save()
                        free -= sz
                    except Exception as ex:  # noqa: BLE001
                        log.warning("prior_job:%s transfer skipped: %s", rid[:8], ex)
        for key in ready:
            if self.time_left() < 120:
                log.info("Run time budget reached; %d ready chunk(s) left for next run", len(ready))
                break
            e = self.state.entries[key]
            if self.drive and free < e.get("size", 0) + margin:
                self.halt_submissions = "Google Drive is full"
                log.error("Google Drive is nearly full (%s free, need %s): transfers and new submissions paused. "
                          "Free space / upgrade storage.", human(free), human(e.get("size", 0)))
                break
            if self.dry:
                log.info("[dry-run] would transfer %s (%s)", key, human(e.get("size")))
                continue
            try:
                self._transfer_one(key, e)
                if self.drive:
                    free -= e.get("size", 0)
            except FileNotFoundError:
                log.warning("%s: CDS result expired before transfer; refreshing link", key)
                self._refresh_or_resubmit(key, e)
            except Exception as ex:  # noqa: BLE001
                e["terr"] = f"{iso()} {str(ex)[:400]}"
                e["tatt"] = e.get("tatt", 0) + 1
                log.error("%s: transfer failed (%s)\n%s", key, ex, traceback.format_exc(limit=3))
                if e["tatt"] >= 5:
                    log.warning("%s: 5 failed transfers -> resubmitting request", key)
                    e.pop("tatt", None)
                    self._fail(key, e, f"transfer failed repeatedly: {ex}")
                self.state.put(key, e)
            self.state.save()

    def _transfer_one(self, key: str, e: dict) -> None:
        d = self.descriptor(key)
        size = int(e.get("size") or 0)
        if not size:
            size = int(requests.head(e["url"], timeout=60, allow_redirects=True).headers.get("Content-Length", 0))
        if self.drive:
            folder = self.drive.ensure_folder([self.cfg.get("drive_root_folder", "ERA5_Pakistan")] + d["folder"])
            existing = self.drive.find(folder, d["name"], folder=False)
            if existing and int(existing.get("size", -1)) == size:
                log.info("%s: already on Drive as %s", key, d["name"])
                meta = existing
            else:
                if existing:
                    self.drive.delete(existing["id"])
                log.info("%s: streaming CDS -> Drive %s/%s (%s)", key, "/".join(d["folder"]), d["name"], human(size))
                meta = self.drive.upload_from_url(folder, d["name"], e["url"], size)
            if int(meta.get("size", -1)) != size:
                raise RuntimeError(f"size mismatch on Drive: {meta.get('size')} != {size}")
            if e.get("md5") and meta.get("md5Checksum") and e["md5"] != meta["md5Checksum"]:
                self.drive.delete(meta["id"])
                raise RuntimeError(f"MD5 mismatch ({meta['md5Checksum']} != {e['md5']}); deleted, will retry")
            e.update(s="done", fid=meta["id"], done=iso())
        else:  # local
            target = ROOT / self.cfg.get("local_dir", "downloads") / Path(*d["folder"]) / d["name"]
            target.parent.mkdir(parents=True, exist_ok=True)
            part = target.with_suffix(target.suffix + ".part")
            have = part.stat().st_size if part.exists() else 0
            log.info("%s: downloading to %s (%s)", key, target, human(size))
            h = hashlib.md5()
            if have:
                with open(part, "rb") as f:
                    for blk in iter(lambda: f.read(8 << 20), b""):
                        h.update(blk)
            with open(part, "ab") as f:
                for blk in Drive._source(e["url"], have):
                    f.write(blk)
                    h.update(blk)
            if part.stat().st_size != size:
                raise RuntimeError(f"size mismatch {part.stat().st_size} != {size}")
            if e.get("md5") and h.hexdigest() != e["md5"]:
                part.unlink()
                raise RuntimeError("MD5 mismatch; deleted, will retry")
            os.replace(part, target)
            e.update(s="done", path=str(target.relative_to(ROOT)), done=iso())
        for k in ("url", "terr", "tatt", "exp"):
            e.pop(k, None)
        self.state.put(key, e)
        log.info("%s: DONE", key)

    def _refresh_or_resubmit(self, key: str, e: dict) -> None:
        try:
            e["exp"] = e.get("exp", 0) + 1
            if e["exp"] > 2:
                raise RuntimeError("link refresh did not help")
            res = self.client.get_remote(e["rid"]).get_results()
            e["url"] = res.location
            self.state.put(key, e)
        except Exception:  # noqa: BLE001
            e.pop("exp", None)
            e["att"] = max(0, e.get("att", 1) - 1)  # expiry is not the request's fault
            self._fail(key, e, "CDS result expired before it was transferred")

    def write_manual_list(self) -> None:
        rows = [(k, e) for k, e in sorted(self.state.entries.items()) if e["s"] == "ready"]
        prior = [(rid, fj) for rid, fj in sorted(self.state.meta.get("foreign_jobs", {}).items())
                 if fj.get("status") == "successful" and fj.get("url") and not fj.get("pulled")]
        path = STATE_DIR / "ready_for_manual_download.csv"
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["chunk", "suggested_filename", "size_bytes", "md5", "cds_request_id", "finished_utc", "download_url"])
            for k, e in rows:
                w.writerow([k, self.descriptor(k)["name"], e.get("size"), e.get("md5"), e.get("rid"), e.get("fin"), e.get("url")])
            for rid, fj in prior:
                w.writerow([f"prior_job:{rid[:8]}", f"prior_{fj['ds']}_{rid[:8]}.grib",
                            fj.get("size"), fj.get("md5"), rid, fj.get("fin") or fj.get("created"), fj.get("url")])
        if rows or prior:
            log.info("%d chunk(s) (+ %d prior job(s)) ready for manual download -> %s (also at https://cds.climate.copernicus.eu/requests)",
                     len(rows), len(prior), path.relative_to(ROOT))

    # ---------------------------------------------------------------- 5. submit
    def submit_new(self) -> None:
        if self.halt_submissions:
            log.warning("Not submitting new requests: %s", self.halt_submissions)
            return
        budget = self.cfg.get("max_submissions_per_run", 4)
        if self.storage == "none":
            backlog = sum(1 for e in self.state.entries.values() if e["s"] == "ready")
            cap = self.cfg.get("max_unpulled_ready", 150)
            if backlog >= cap:
                log.warning("%d finished results not yet pulled (cap %d). Download them, then run "
                            "`mark-pulled --all` to continue submitting.", backlog, cap)
                return
        for ds, dcfg in self.datasets.items():
            in_flight = sum(1 for k, e in self.state.entries.items()
                            if e["s"] == "submitted" and (k.startswith("pl:") == (ds == PL)))
            slots = max(0, dcfg.get("max_in_flight", 2) - in_flight)
            log.info("%s: %d in flight, %d free slot(s)", ds, in_flight, slots)
            for key in self._candidates(ds):
                if slots <= 0 or budget <= 0 or self.time_left() < 30:
                    break
                try:
                    if self._submit_one(key, dcfg):
                        slots -= 1
                        budget -= 1
                except DatasetQueueFull as ex:
                    log.warning("%s: queue limit reached (%s); moving to next dataset", ds, ex)
                    self.state.save()
                    break
                except StopSubmitting as ex:
                    log.warning("CDS asked us to back off (%s); no more submissions this run", ex)
                    self.state.save()
                    return
                self.state.save()

    def _candidates(self, ds: str) -> Iterator[str]:
        """Yield submittable keys in planned order (expanding split chunks into their children)."""
        def expand(key: str) -> Iterator[str]:
            e = self.state.get(key)
            if e is None:
                yield key
                return
            s = e["s"]
            if s == "split":
                for c in e.get("children", []):
                    yield from expand(c)
            elif s == "pending":
                yield key
            elif s == "failed" and (parse_iso(e.get("retry_at")) or utcnow()) <= utcnow():
                yield key
        for parent in self.planner.iter_keys(ds):
            yield from expand(parent)

    def _submit_one(self, key: str, dcfg: dict) -> bool:
        e = self.state.get(key) or {"s": "pending", "att": 0}
        d = self.descriptor(key)
        ds, req = d["ds"], d["request"]
        if not e.get("cov_checked"):  # first time we evaluate this chunk -> check pre-existing jobs
            action, info = self.apply_prior_jobs(d)
            if action == "split_cov":
                log.info("%s: partial variable coverage from prior CDS jobs -> splitting by variable", key)
                self.state.put(key, e)
                kids = self._split(key, e)
                for ck in kids:
                    if self._submit_one(ck, dcfg):
                        return True
                return False
            e["cov_checked"] = True
            if action == "adopt":
                self.state.meta["foreign_jobs"][info["rid"]]["adopted"] = key
                self.state.touch_meta()
                e.update(s="submitted", rid=info["rid"], sub=iso(utcnow() - dt.timedelta(days=1)), att=1, note="adopted existing CDS request")
                log.info("%s: adopted existing CDS request %s (%s)", key, info["rid"], info["status"])
                self.state.put(key, e)
                return False
            if action == "covered":
                e.update(s="covered", by=info["by"])
                log.info("%s: already provided by earlier CDS request(s) %s -> skipped", key, info["by"])
                self.state.put(key, e)
                return False
            if action == "trim":
                req = dict(req, day=info["days"])
                e["req"] = req
                e["note"] = f"days {','.join(info['excluded'])} already provided by {','.join(info['by'])}"
                log.info("%s: %s -> requesting remaining %d day(s) only", key, e["note"], len(info["days"]))
        # cost check against the CDS per-request limit
        try:
            est = self.client.estimate_costs(ds, req)
            ests = est if isinstance(est, list) else [est]
            for c in ests:
                if c.get("limit") and float(c["cost"]) > float(c["limit"]) * dcfg.get("cost_safety", 0.97):
                    raise TooLarge(f"cost {c['cost']} > limit {c['limit']} ({c.get('id')})")
        except TooLarge as ex:
            log.info("%s: %s -> splitting", key, ex)
            self.state.put(key, e)
            self._split(key, e)
            return False
        except Exception as ex:  # noqa: BLE001
            self._check_backoff(ex)
            log.warning("%s: cost estimate failed (%s); submitting anyway", key, ex)
        if self.dry:
            log.info("[dry-run] would submit %s -> %s", key, d["name"])
            return True
        try:
            remote = self.client.submit(ds, req)
        except Exception as ex:  # noqa: BLE001
            self._check_backoff(ex)
            txt = str(ex).lower()
            if any(s in txt for s in ("cost limit", "too large", "request is too", "exceed")):
                self.state.put(key, e)
                self._split(key, e)
                return False
            e["att"] = e.get("att", 0) + 1
            log.error("%s: submit failed: %s", key, ex)
            self._fail(key, e, f"submit failed: {ex}")
            return False
        # Wait 7s for CDS async queue admission check before recording as accepted
        time.sleep(7)
        try:
            st = remote.status
        except Exception:  # noqa: BLE001
            st = "accepted"
        if st in ("rejected", "failed"):
            msg = st
            try:
                remote.get_results()
            except Exception as ex:  # noqa: BLE001
                msg = f"{st}: {str(ex)[:400]}"
            log.warning("%s: CDS rejected submission %s (%s)", key, remote.request_id, msg.splitlines()[-1])
            if "queued requests" in msg.lower():
                e["s"] = "pending"
                self.state.put(key, e)
                raise DatasetQueueFull(msg.splitlines()[-1])
            e["att"] = e.get("att", 0) + 1
            self._fail(key, e, msg)
            return False
        e.update(s="submitted", rid=remote.request_id, sub=iso(), att=e.get("att", 0) + 1)
        for k in ("err", "retry_at", "chk"):
            e.pop(k, None)
        self.state.put(key, e)
        log.info("%s: SUBMITTED & ACCEPTED as %s -> %s", key, remote.request_id, d["name"])
        return True

    @staticmethod
    def _check_backoff(ex: Exception) -> None:
        resp = getattr(ex, "response", None)
        code = getattr(resp, "status_code", None)
        txt = str(ex).lower()
        if code == 429 or "too many" in txt or "rate limit" in txt or "queue" in txt and "limit" in txt:
            raise StopSubmitting(str(ex)[:200])

    # ---------------------------------------------------------------- report
    def write_progress(self) -> None:
        c = self.counts()
        lines = [f"# ERA5 Pakistan download progress", "", f"_Updated {iso()} -- storage: **{self.storage}**, "
                 f"period {self.planner.start} .. {self.planner.end} ({self.cfg.get('order')})_", "",
                 "| dataset | planned chunks | done | covered by earlier requests | in flight | ready | failed (retrying) | dead | split | volume done |",
                 "|---|---|---|---|---|---|---|---|---|---|"]
        for ds in self.datasets:
            cc = c.get(ds, {})
            vol = sum(e.get("size", 0) for k, e in self.state.entries.items()
                      if e["s"] in ("done", "pulled") and k.startswith("pl:") == (ds == PL))
            lines.append(f"| {ds} | {self.planner.total(ds)} | {cc.get('done', 0) + cc.get('pulled', 0)} | "
                         f"{cc.get('covered', 0)} | {cc.get('submitted', 0)} | {cc.get('ready', 0)} | "
                         f"{cc.get('failed', 0)} | {cc.get('dead', 0)} | {cc.get('split', 0)} | {human(vol)} |")
        if self.halt_submissions:
            lines += ["", f"**Submissions paused:** {self.halt_submissions}"]
        atomic_write(STATE_DIR / "progress.md", "\n".join(lines) + "\n")
        print("\n".join(lines))

    # ---------------------------------------------------------------- main tick
    def run(self) -> None:
        log.info("=== tick start (storage=%s, dry_run=%s, period %s..%s) ===",
                 self.storage, self.dry, self.planner.start, self.planner.end)
        self.client.check_authentication()
        steps = [("licences", self.ensure_licences), ("scan jobs", self.scan_jobs), ("poll", self.poll),
                 ("transfer", self.transfer), ("submit", self.submit_new)]
        for name, fn in steps:
            try:
                fn()
            except Exception as ex:  # noqa: BLE001
                log.error("step '%s' failed: %s\n%s", name, ex, traceback.format_exc())
            finally:
                self.state.save()
        if self.storage == "none":
            self.write_manual_list()
        self.write_progress()
        self.state.save()
        log.info("=== tick end ===")


# --------------------------------------------------------------------------- lock
class Lock:
    def __init__(self, stale_minutes: float):
        self.stale = stale_minutes

    def __enter__(self):
        if LOCK_FILE.exists():
            age = (time.time() - LOCK_FILE.stat().st_mtime) / 60
            if age < self.stale:
                raise SystemExit(f"Another run holds {LOCK_FILE.name} ({age:.0f} min old); exiting.")
            log.warning("Removing stale lock (%.0f min old)", age)
            LOCK_FILE.unlink()
        fd = os.open(LOCK_FILE, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, f"{os.getpid()} {iso()}".encode())
        os.close(fd)
        return self

    def __exit__(self, *a):
        try:
            LOCK_FILE.unlink()
        except FileNotFoundError:
            pass


# --------------------------------------------------------------------------- CLI
def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-v", "--verbose", action="store_true")
    sub = ap.add_subparsers(dest="cmd")
    r = sub.add_parser("run", help="one scheduler tick (default)")
    r.add_argument("--dry-run", action="store_true", help="estimate & plan only, no submit/transfer")
    r.add_argument("--max-run-minutes", type=float)
    sub.add_parser("status", help="print progress table")
    p = sub.add_parser("plan", help="show the next N chunks that would be submitted")
    p.add_argument("-n", type=int, default=10)
    m = sub.add_parser("mark-pulled", help="(manual mode) mark ready chunks as downloaded by you")
    m.add_argument("keys", nargs="*")
    m.add_argument("--all", action="store_true")
    rs = sub.add_parser("reset", help="put dead/failed chunks back to pending")
    rs.add_argument("keys", nargs="*")
    rs.add_argument("--all-dead", action="store_true")
    a = ap.parse_args()
    setup_logging(a.verbose)
    cfg, secrets = load_config(), load_secrets()
    cmd = a.cmd or "run"

    if cmd in ("run", None):
        mx = getattr(a, "max_run_minutes", None)
        with Lock(stale_minutes=(mx or cfg.get("max_run_minutes", 25)) + 60):
            Scheduler(cfg, secrets, dry_run=getattr(a, "dry_run", False), max_minutes=mx).run()
    elif cmd == "status":
        Scheduler(cfg, secrets, dry_run=True).write_progress()
    elif cmd == "plan":
        s = Scheduler(cfg, secrets, dry_run=True)
        for ds in s.datasets:
            print(f"--- {ds}")
            for i, key in zip(range(a.n), s._candidates(ds)):
                d = s.descriptor(key)
                print(f"{key:28s} {d['name']}")
    elif cmd == "mark-pulled":
        st = State(STATE_DIR)
        keys = [k for k, e in st.entries.items() if e["s"] == "ready"] if a.all else a.keys
        for k in keys:
            e = st.get(k)
            if e and e["s"] == "ready":
                e.update(s="pulled", done=iso())
                e.pop("url", None)
                st.put(k, e)
        st.save()
        print(f"marked {len(keys)} chunk(s) as pulled")
    elif cmd == "reset":
        st = State(STATE_DIR)
        keys = [k for k, e in st.entries.items() if e["s"] == "dead"] if a.all_dead else a.keys
        for k in keys:
            e = st.get(k)
            if e:
                e.update(s="pending", att=0)
                for f in ("err", "retry_at", "rid"):
                    e.pop(f, None)
                st.put(k, e)
        st.save()
        print(f"reset {len(keys)} chunk(s)")


if __name__ == "__main__":
    main()
