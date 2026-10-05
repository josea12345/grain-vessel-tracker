"""
Grain Vessel Tracker — AIS collector (v1).

Connects to AISStream.io's free WebSocket feed, subscribes to bounding boxes
around key grain export ports, keeps the latest position report per vessel,
writes a live snapshot (data/vessels.json) for the 3D globe, and archives
every snapshot into data/vessels.db so voyage history builds from day one.

Usage:
    export AISSTREAM_API_KEY=your_key_here   # free at https://aisstream.io
    python collector/collector.py --seconds 120

Notes / honest limitations:
- AISStream is real-time only; there is no free historical endpoint. History
  in vessels.db starts the day you first run this.
- AISStream's PositionReport metadata does NOT include ship type (it arrives
  in separate static-data messages), so v1 tracks ALL vessel traffic inside
  the port zones — bulkers, tankers, tugs — and labels it honestly as such.
  "Grain vessel" here means a vessel inside a grain-port bounding box: a
  heuristic, not a cargo manifest. Ship-type filtering via static-data
  messages is a v2 improvement. The README states this plainly.
- One WebSocket connection per API key. Send the subscription within 3 seconds
  of connecting or the server drops you.
- Honors HTTPS_PROXY/https_proxy env vars for sandboxed networks.
"""
import argparse
import asyncio
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone

try:
    import websockets
except ImportError:
    sys.exit("Missing dependency: pip install -r collector/requirements.txt")

# ---------------------------------------------------------------------------
# Grain ports, v1: Mississippi River system (New Orleans / S. Louisiana) and
# Santos, Brazil — the two largest Western-hemisphere grain export gateways.
# Bounding boxes are [[lat_min, lon_min], [lat_max, lon_max]].
# ---------------------------------------------------------------------------
PORTS = {
    "new-orleans": {
        "name": "New Orleans / S. Louisiana",
        "country": "USA",
        "bbox": [[28.6, -92.2], [30.6, -88.6]],
        "marker": [-90.06, 29.95],
    },
    "santos": {
        "name": "Santos",
        "country": "Brazil",
        "bbox": [[-25.2, -47.2], [-22.8, -45.4]],
        "marker": [-46.31, -23.96],
    },
}

# AIS ship-type codes 70-79 cover cargo vessels, but AISStream's PositionReport
# MetaData does NOT include ShipType (it arrives in separate static-data
# messages). v1 therefore tracks ALL vessel traffic inside the port zones and
# labels it honestly as such — see README "Honest limitations". Ship-type
# filtering via static-data integration is a v2 improvement.

WS_URL = "wss://stream.aisstream.io/v0/stream"
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")


def build_subscription(api_key):
    return {
        "APIKey": api_key,
        "BoundingBoxes": [p["bbox"] for p in PORTS.values()],
        "FilterMessageTypes": ["PositionReport"],
    }


def extract_vessel(msg):
    """Pull the fields the globe needs, defensively — AIS payloads vary."""
    meta = msg.get("MetaData", {}) or {}
    body = (msg.get("Message", {}) or {}).get("PositionReport", {}) or {}
    try:
        lat = float(meta.get("latitude"))
        lon = float(meta.get("longitude"))
    except (TypeError, ValueError):
        return None
    mmsi = str(meta.get("MMSI", ""))
    if not mmsi:
        return None
    # NOTE: no ship-type filtering — see comment at top of file.
    try:
        sog = float(body.get("Sog", 0) or 0)
    except (TypeError, ValueError):
        sog = 0.0
    try:
        cog = float(body.get("Cog", 0) or 0)
    except (TypeError, ValueError):
        cog = 0.0
    return {
        "mmsi": mmsi,
        "name": (meta.get("ShipName") or "UNKNOWN").strip(),
        "lat": lat,
        "lon": lon,
        "sog_knots": round(sog, 1),          # speed over ground
        "cog_deg": round(cog, 1),            # course over ground
        "destination": (meta.get("Destination") or "").strip(),
        "timestamp": meta.get("time_utc") or datetime.now(timezone.utc).isoformat(),
    }


def archive_snapshot(vessels):
    """Append this snapshot to SQLite so trails/history accumulate."""
    os.makedirs(DATA_DIR, exist_ok=True)
    db = os.path.join(DATA_DIR, "vessels.db")
    conn = sqlite3.connect(db)
    conn.execute(
        """CREATE TABLE IF NOT EXISTS positions (
               mmsi TEXT, name TEXT, lat REAL, lon REAL,
               sog_knots REAL, cog_deg REAL, destination TEXT,
               seen_at TEXT,
               PRIMARY KEY (mmsi, seen_at))"""
    )
    now = datetime.now(timezone.utc).isoformat()
    rows = [
        (v["mmsi"], v["name"], v["lat"], v["lon"],
         v["sog_knots"], v["cog_deg"], v["destination"], now)
        for v in vessels.values()
    ]
    conn.executemany("INSERT OR IGNORE INTO positions VALUES (?,?,?,?,?,?,?,?)", rows)
    conn.commit()
    total = conn.execute("SELECT COUNT(*) FROM positions").fetchone()[0]
    conn.close()
    return total


async def collect(api_key, seconds):
    vessels = {}
    subscription = json.dumps(build_subscription(api_key))
    # Honor standard proxy env vars (some sandboxes/VPNs require egress via proxy).
    proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")
    connect_kwargs = {"max_size": 10 * 1024 * 1024}
    if proxy:
        connect_kwargs["proxy"] = proxy
    async with websockets.connect(WS_URL, **connect_kwargs) as ws:
        # The 3-second rule: subscribe immediately or the server drops you.
        await ws.send(subscription)
        deadline = time.time() + seconds
        n_messages = 0
        while time.time() < deadline:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=deadline - time.time())
            except asyncio.TimeoutError:
                break
            n_messages += 1
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if msg.get("MessageType") != "PositionReport":
                continue
            v = extract_vessel(msg)
            if v:
                vessels[v["mmsi"]] = v
    return vessels, n_messages


def main():
    ap = argparse.ArgumentParser(description="Collect grain-port vessel positions from AISStream.")
    ap.add_argument("--seconds", type=int, default=120,
                    help="How long to listen to the AIS stream (default: 120).")
    ap.add_argument("--key", default=os.environ.get("AISSTREAM_API_KEY"),
                    help="AISStream API key (or set AISSTREAM_API_KEY).")
    args = ap.parse_args()
    if not args.key:
        sys.exit("No API key. Get a free one at https://aisstream.io and set AISSTREAM_API_KEY.")

    print(f"Listening to AISStream for {args.seconds}s "
          f"({len(PORTS)} port boxes, all vessel traffic)...")
    vessels, n_messages = asyncio.run(collect(args.key, args.seconds))
    print(f"Received {n_messages} messages -> {len(vessels)} vessels in grain-port boxes.")

    os.makedirs(DATA_DIR, exist_ok=True)
    snapshot = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "ports": {k: {"name": p["name"], "country": p["country"], "marker": p["marker"]}
                 for k, p in PORTS.items()},
        "vessel_count": len(vessels),
        "vessels": sorted(vessels.values(), key=lambda v: v["name"]),
        "sample_data": False,
    }
    with open(os.path.join(DATA_DIR, "vessels.json"), "w") as f:
        json.dump(snapshot, f, indent=1)
    total_rows = archive_snapshot(vessels)
    print(f"Wrote data/vessels.json ({len(vessels)} vessels). "
          f"Archive now holds {total_rows} position rows.")


if __name__ == "__main__":
    main()
