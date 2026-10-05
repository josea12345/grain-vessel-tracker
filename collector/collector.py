"""
Grain Vessel Tracker — AIS collector (v1).

Connects to AISStream.io's free WebSocket feed, subscribes to bounding boxes
around 20 major grain export ports worldwide (US Gulf, US Pacific Northwest,
Brazil, Argentina, Canada, Black Sea, France, Australia), keeps the latest
position report per vessel,
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
# Grain ports: the world's major grain export gateways — US Gulf & Pacific
# Northwest, Brazil, Argentina, Canada, Black Sea, France, Australia.
# Bounding boxes are [[lat_min, lon_min], [lat_max, lon_max]].
# ---------------------------------------------------------------------------
PORTS = {
    # --- US Gulf ---
    "new-orleans": {
        "name": "New Orleans / S. Louisiana",
        "country": "USA",
        "bbox": [[28.6, -92.2], [30.6, -88.6]],
        "marker": [-90.06, 29.95],
    },
    "houston": {
        "name": "Houston / Galveston",
        "country": "USA",
        "bbox": [[28.9, -95.5], [30.1, -94.1]],
        "marker": [-94.77, 29.60],
    },
    "corpus-christi": {
        "name": "Corpus Christi",
        "country": "USA",
        "bbox": [[27.2, -98.0], [28.4, -96.8]],
        "marker": [-97.40, 27.81],
    },
    # --- US Pacific Northwest ---
    "pnw-columbia": {
        "name": "Columbia River (Portland / Kalama)",
        "country": "USA",
        "bbox": [[45.4, -123.6], [46.5, -122.2]],
        "marker": [-122.83, 46.01],
    },
    "seattle": {
        "name": "Seattle / Tacoma",
        "country": "USA",
        "bbox": [[47.1, -123.0], [47.9, -122.0]],
        "marker": [-122.47, 47.60],
    },
    # --- Brazil ---
    "santos": {
        "name": "Santos",
        "country": "Brazil",
        "bbox": [[-25.2, -47.2], [-22.8, -45.4]],
        "marker": [-46.31, -23.96],
    },
    "paranagua": {
        "name": "Paranaguá",
        "country": "Brazil",
        "bbox": [[-26.3, -49.3], [-24.7, -47.7]],
        "marker": [-48.52, -25.50],
    },
    "rio-grande": {
        "name": "Rio Grande",
        "country": "Brazil",
        "bbox": [[-32.8, -52.9], [-31.3, -51.3]],
        "marker": [-52.10, -32.03],
    },
    "itaqui": {
        "name": "Itaqui / São Luís",
        "country": "Brazil",
        "bbox": [[-3.3, -45.1], [-1.8, -43.6]],
        "marker": [-44.38, -2.58],
    },
    "barcarena": {
        "name": "Barcarena / Vila do Conde",
        "country": "Brazil",
        "bbox": [[-2.2, -49.4], [-0.9, -48.1]],
        "marker": [-48.75, -1.53],
    },
    # --- Argentina ---
    "rosario": {
        "name": "Rosario / San Lorenzo (Up-River)",
        "country": "Argentina",
        "bbox": [[-33.3, -61.3], [-32.2, -60.2]],
        "marker": [-60.73, -32.75],
    },
    "bahia-blanca": {
        "name": "Bahía Blanca",
        "country": "Argentina",
        "bbox": [[-39.5, -62.9], [-38.1, -61.3]],
        "marker": [-62.10, -38.78],
    },
    # --- Canada ---
    "vancouver": {
        "name": "Vancouver",
        "country": "Canada",
        "bbox": [[49.0, -123.7], [49.5, -122.7]],
        "marker": [-123.12, 49.29],
    },
    "prince-rupert": {
        "name": "Prince Rupert",
        "country": "Canada",
        "bbox": [[53.9, -131.0], [54.7, -129.6]],
        "marker": [-130.32, 54.32],
    },
    # --- Black Sea ---
    "odesa": {
        "name": "Odesa / Chornomorsk",
        "country": "Ukraine",
        "bbox": [[46.1, 30.2], [46.9, 31.2]],
        "marker": [30.73, 46.48],
    },
    "novorossiysk": {
        "name": "Novorossiysk",
        "country": "Russia",
        "bbox": [[44.3, 37.2], [45.1, 38.3]],
        "marker": [37.77, 44.72],
    },
    "constanta": {
        "name": "Constanta",
        "country": "Romania",
        "bbox": [[43.8, 28.1], [44.6, 29.2]],
        "marker": [28.66, 44.17],
    },
    # --- Europe ---
    "rouen": {
        "name": "Rouen",
        "country": "France",
        "bbox": [[49.1, 0.4], [49.8, 1.7]],
        "marker": [1.08, 49.44],
    },
    # --- Australia ---
    "kwinana": {
        "name": "Kwinana / Perth",
        "country": "Australia",
        "bbox": [[-32.9, 115.1], [-31.6, 116.4]],
        "marker": [115.75, -32.23],
    },
    "newcastle-au": {
        "name": "Newcastle",
        "country": "Australia",
        "bbox": [[-33.5, 151.2], [-32.3, 152.4]],
        "marker": [151.78, -32.92],
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
