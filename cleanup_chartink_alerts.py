"""
cleanup_chartink_alerts.py — trim chartink_alerts.json to a rolling window.

The Chartink webhook appends every alert and never removes anything, so the
file grows without bound. live_market.html only ever reads TODAY's entries
(_ckToday filters to the latest date), so older entries are pure payload
weight on every page load.

Run once daily, after market close. Ideally the Worker would trim on write
instead — that caps the file at the source and needs no cron — but this works
without touching the Worker.
"""

import os
import sys
import json
import requests
from datetime import datetime, timedelta, timezone

WORKER_URL   = "https://r2-uploader.tradewithtechnical2025.workers.dev"
WORKER_TOKEN = os.environ.get("R2_TOKEN", "TWT2025xSecure")

FILE      = "chartink_alerts.json"
KEEP_DAYS        = 1   # drop entries older than this entirely
KEEP_DETAIL_DAYS = 1   # older than this: keep count/time, strip heavy arrays
IST       = timezone(timedelta(hours=5, minutes=30))

# Never leave the file empty. If the filter would wipe everything the input
# format has probably changed, and silently publishing [] would break the
# live dashboard far worse than an oversized file.
MIN_KEEP = 1


def parse_time(entry):
    """Best-effort timestamp extraction; returns None if unparseable."""
    raw = entry.get("time") or entry.get("triggered_at") or entry.get("ts")
    if not raw:
        return None
    s = str(raw).strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(s[:26] if "." in s else s, fmt)
            return dt.replace(tzinfo=timezone.utc) if s.endswith("Z") else dt.replace(tzinfo=IST)
        except ValueError:
            continue
    return None


def main():
    print(f"Fetching {FILE}…")
    r = requests.get(f"{WORKER_URL}/{FILE}",
                     headers={"X-Secret-Token": WORKER_TOKEN}, timeout=60)
    if r.status_code != 200:
        sys.exit(f"GET failed: HTTP {r.status_code} — aborting, file untouched.")

    raw = r.content
    try:
        entries = json.loads(raw)
    except json.JSONDecodeError as e:
        sys.exit(f"Not valid JSON ({e}) — aborting, file untouched.")

    if not isinstance(entries, list):
        sys.exit("Expected a JSON array — aborting, file untouched.")

    before_n, before_kb = len(entries), len(raw) / 1024
    print(f"  {before_n} entries, {before_kb:.1f} KB")

    # Same two-tier policy the Worker applies on every webhook, so running
    # this by hand and letting the Worker trim produce identical output.
    now = datetime.now(IST)
    drop_before   = now - timedelta(days=KEEP_DAYS)
    strip_before  = now - timedelta(days=KEEP_DETAIL_DAYS)
    kept, undated, stripped = [], 0, 0
    for e in entries:
        t = parse_time(e)
        if t is None:
            # Keep unparseable entries rather than dropping data we don't
            # understand — if this count is large, the format has changed.
            kept.append(e)
            undated += 1
        elif t >= strip_before:
            kept.append(e)
        elif t >= drop_before:
            if e.get("columns") or e.get("stocks"):
                e = {**e, "columns": [], "stocks": [], "trigger_prices": []}
                stripped += 1
            kept.append(e)

    if stripped:
        print(f"  {stripped} entries stripped of per-stock detail (kept count/time)")

    if undated:
        print(f"  ⚠ {undated} entries had no parseable timestamp (kept)")

    removed = before_n - len(kept)
    if removed <= 0 and not stripped:
        print("  Nothing to trim — file untouched.")
        return
    if len(kept) < MIN_KEEP:
        sys.exit(f"Filter would leave {len(kept)} entries — aborting as unsafe.")

    payload = json.dumps(kept, separators=(",", ":"))
    after_kb = len(payload.encode()) / 1024
    print(f"  Trimming {removed} entries older than {KEEP_DAYS}d "
          f"-> {len(kept)} entries, {after_kb:.1f} KB "
          f"({(1 - after_kb / before_kb) * 100:.0f}% smaller)")

    p = requests.post(f"{WORKER_URL}?file={FILE}",
                      headers={"X-Secret-Token": WORKER_TOKEN,
                               "Content-Type": "application/json"},
                      data=payload, timeout=120)
    if p.status_code != 200:
        sys.exit(f"PUT failed: HTTP {p.status_code} {p.text[:200]}")
    print("  ✅ pushed")


if __name__ == "__main__":
    main()
