"""
Grain Vessel Tracker — AIS collector (v1).

Connects to AISStream.io's free WebSocket feed, subscribes to bounding boxes
around 32 major grain ports and chokepoints across the Americas, Black Sea,
Europe, and Australia (US Gulf, US Pacific Northwest, Brazil, Argentina,
Uruguay, Central America & Caribbean, Canada, Black Sea, France, Australia), keeps the latest
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
        "name": "Mississippi River (New Orleans / S. Louisiana)",
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
    "sao-francisco": {
        "name": "São Francisco do Sul",
        "country": "Brazil",
        "bbox": [[-27.0, -49.4], [-25.5, -47.9]],
        "marker": [-48.63, -26.23],
    },
    "salvador": {
        "name": "Salvador",
        "country": "Brazil",
        "bbox": [[-13.7, -39.2], [-12.3, -37.8]],
        "marker": [-38.51, -12.97],
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
    "necochea": {
        "name": "Necochea / Quequén",
        "country": "Argentina",
        "bbox": [[-39.3, -59.7], [-37.9, -58.2]],
        "marker": [-58.95, -38.57],
    },
    "zarate": {
        "name": "Zárate / Campana",
        "country": "Argentina",
        "bbox": [[-34.8, -59.7], [-33.4, -58.3]],
        "marker": [-59.03, -34.10],
    },
    # --- Uruguay ---
    "nueva-palmira": {
        "name": "Nueva Palmira",
        "country": "Uruguay",
        "bbox": [[-34.6, -59.1], [-33.2, -57.7]],
        "marker": [-58.42, -33.88],
    },
    "montevideo": {
        "name": "Montevideo",
        "country": "Uruguay",
        "bbox": [[-35.6, -56.9], [-34.2, -55.5]],
        "marker": [-56.21, -34.90],
    },
    # --- Central America & Caribbean ---
    "cristobal": {
        "name": "Panama Canal – Cristóbal",
        "country": "Panama",
        "bbox": [[8.7, -80.6], [10.0, -79.2]],
        "marker": [-79.90, 9.35],
    },
    "balboa": {
        "name": "Panama Canal – Balboa",
        "country": "Panama",
        "bbox": [[8.3, -80.2], [9.6, -78.9]],
        "marker": [-79.57, 8.95],
    },
    "veracruz": {
        "name": "Veracruz",
        "country": "Mexico",
        "bbox": [[18.6, -96.8], [19.8, -95.5]],
        "marker": [-96.13, 19.20],
    },
    "corinto": {
        "name": "Corinto",
        "country": "Nicaragua",
        "bbox": [[11.9, -87.8], [13.1, -86.6]],
        "marker": [-87.18, 12.48],
    },
    "puerto-cortes": {
        "name": "Puerto Cortés",
        "country": "Honduras",
        "bbox": [[15.2, -88.6], [16.4, -87.3]],
        "marker": [-87.95, 15.83],
    },
    "puerto-quetzal": {
        "name": "Puerto Quetzal",
        "country": "Guatemala",
        "bbox": [[13.3, -91.4], [14.5, -90.1]],
        "marker": [-90.78, 13.92],
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

# --- Ocean "lanes": the highways grain travels. Watching these puts vessels
# on the map mid-journey instead of only inside port circles. They are NOT
# ports: no markers, no product mapping — just coverage.
LANES = {
    "lane-trans-atlantic": {
        "name": "North Atlantic lane",
        "bbox": [[28, -65], [52, -10]],
    },
    "lane-trans-pacific": {
        "name": "Trans-Pacific lane",
        "bbox": [[5, -175], [40, -120]],
    },
    "lane-south-atlantic": {
        "name": "South Atlantic lane",
        "bbox": [[-38, -45], [5, -5]],
    },
    "lane-indian": {
        "name": "Indian Ocean lane",
        "bbox": [[-12, 50], [20, 90]],
    },
    "lane-med": {
        "name": "Mediterranean lane",
        "bbox": [[30, 5], [46, 38]],
    },
}

# AIS ship-type codes 70-79 cover cargo vessels, but AISStream's PositionReport
# MetaData does NOT include ShipType (it arrives in separate static-data
# messages). v1 therefore tracks ALL vessel traffic inside the port zones and
# labels it honestly as such — see README "Honest limitations". Ship-type
# filtering via static-data integration is a v2 improvement.

WS_URL = "wss://stream.aisstream.io/v0/stream"
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")


def load_tracked_mmsis():
    """MMSIs the user asked to track anywhere in the world (not just in zones)."""
    return [str(x.get("mmsi", x)) for x in _tracked_entries()
            if str(x.get("mmsi", x)).isdigit()]


def _tracked_entries():
    path = os.path.join(DATA_DIR, "tracked_mmsis.json")
    try:
        data = json.load(open(path))
        return data if isinstance(data, list) else []
    except (OSError, ValueError, AttributeError):
        return []


def build_subscription(api_key):
    return {
        "APIKey": api_key,
        "BoundingBoxes": [p["bbox"] for p in PORTS.values()] + [l["bbox"] for l in LANES.values()],
        # PositionReport gives live positions (+ navigational status).
        # ShipStaticData (type 5) carries the crew-declared destination + ETA.
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
    }


def build_mmsi_subscription(api_key, mmsis):
    # NOTE: AISStream ANDs subscription filters, so an MMSI filter combined
    # with bounding boxes only matches that vessel INSIDE those boxes.
    # Worldwide per-vessel tracking needs its own box-free subscription.
    return {
        "APIKey": api_key,
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
        "FiltersShipMMSI": [str(m) for m in mmsis],
    }


# AIS navigational-status codes -> plain-English labels (ITU-1371).
NAV_STATUS_LABELS = {
    0: "Underway", 1: "At anchor", 2: "Not under command",
    3: "Restricted", 4: "Restricted", 5: "Moored",
    6: "Aground", 7: "Fishing", 8: "Underway",
}


def format_eta(eta):
    """Turn AISStream's Eta object into a short readable string."""
    if not isinstance(eta, dict):
        return ""
    try:
        mo, d, h, mi = (int(eta.get("Month", 0)), int(eta.get("Day", 0)),
                        int(eta.get("Hour", 0)), int(eta.get("Minute", 0)))
    except (TypeError, ValueError):
        return ""
    if not (1 <= mo <= 12 and 1 <= d <= 31):
        return ""
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    return f"{months[mo - 1]} {d}, {h:02d}:{mi:02d} UTC"


def extract_static(msg):
    """Pull destination + ETA from a ShipStaticData message."""
    meta = msg.get("MetaData", {}) or {}
    body = (msg.get("Message", {}) or {}).get("ShipStaticData", {}) or {}
    mmsi = str(meta.get("MMSI", ""))
    if not mmsi:
        return None, None
    return mmsi, {
        "destination": (body.get("Destination") or "").strip(),
        "eta": format_eta(body.get("Eta")),
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
    # Navigational status is the honest "what is it doing" signal AIS gives:
    # underway / at anchor / moored. AIS cannot show loading vs unloading.
    try:
        nav = int(body.get("NavigationalStatus",
                           body.get("NavStatus", 15)) or 0)
    except (TypeError, ValueError):
        nav = 15
    return {
        "mmsi": mmsi,
        "name": (meta.get("ShipName") or "UNKNOWN").strip(),
        "lat": lat,
        "lon": lon,
        "sog_knots": round(sog, 1),          # speed over ground
        "cog_deg": round(cog, 1),            # course over ground
        "nav_status": nav,                   # raw AIS code; UI maps to label
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
               sog_knots REAL, cog_deg REAL, nav_status INTEGER,
               destination TEXT, eta TEXT,
               seen_at TEXT,
               PRIMARY KEY (mmsi, seen_at))"""
    )
    # Migrate older DBs that lack the new columns.
    cols = {r[1] for r in conn.execute("PRAGMA table_info(positions)")}
    for col, typ in (("nav_status", "INTEGER"), ("eta", "TEXT")):
        if col not in cols:
            conn.execute(f"ALTER TABLE positions ADD COLUMN {col} {typ}")
    now = datetime.now(timezone.utc).isoformat()
    rows = [
        (v["mmsi"], v["name"], v["lat"], v["lon"],
         v["sog_knots"], v["cog_deg"], v.get("nav_status", 15),
         v["destination"], v.get("eta", ""), now)
        for v in vessels.values()
    ]
    conn.executemany("INSERT OR IGNORE INTO positions VALUES (?,?,?,?,?,?,?,?,?,?)", rows)
    conn.commit()
    total = conn.execute("SELECT COUNT(*) FROM positions").fetchone()[0]
    conn.close()
    return total


async def collect_one(subscription, seconds):
    """Run a single AISStream subscription, return {mmsi: vessel} + msg count."""
    vessels = {}
    static = {}  # mmsi -> {destination, eta} from ShipStaticData messages
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
            mtype = msg.get("MessageType")
            if mtype == "ShipStaticData":
                mmsi, info = extract_static(msg)
                if mmsi and (info["destination"] or info["eta"]):
                    static[mmsi] = info
                    # Backfill vessels already seen this run.
                    if mmsi in vessels:
                        if info["destination"]:
                            vessels[mmsi]["destination"] = info["destination"]
                        vessels[mmsi]["eta"] = info["eta"]
                continue
            if mtype != "PositionReport":
                continue
            v = extract_vessel(msg)
            if v:
                info = static.get(v["mmsi"])
                if info:
                    if info["destination"]:
                        v["destination"] = info["destination"]
                    v["eta"] = info["eta"]
                else:
                    v["eta"] = ""
                vessels[v["mmsi"]] = v
    return vessels, n_messages


async def collect(api_key, seconds):
    # Phase 1: port zones + ocean lanes.
    vessels, n1 = await collect_one(json.dumps(build_subscription(api_key)), seconds)
    # Phase 2: individually tracked MMSIs, worldwide (separate subscription —
    # see build_mmsi_subscription for why it can't share phase 1's).
    tracked = load_tracked_mmsis()
    n2 = 0
    if tracked:
        v2, n2 = await collect_one(
            json.dumps(build_mmsi_subscription(api_key, tracked)), min(seconds, 120))
        tag_by_mmsi = {str(x.get("mmsi", x)): x.get("tags", [])
                       for x in _tracked_entries()}
        for mmsi, v in v2.items():
            if tag_by_mmsi.get(mmsi):
                v["tags"] = tag_by_mmsi[mmsi]
            vessels.setdefault(mmsi, v)
    return vessels, n1 + n2


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

    # Sanity guard: a degraded run (bad subscription, stream outage) must never
    # wipe the live snapshot. 37 coverage boxes normally yield hundreds of
    # vessels; anything under 50 means the feed failed, not that ports emptied.
    if len(vessels) < 50:
        print(f"REFUSING to write snapshot: only {len(vessels)} vessels "
              f"(minimum 50). Keeping previous data.")
        return

    os.makedirs(DATA_DIR, exist_ok=True)
    snapshot = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "ports": {k: {"name": p["name"], "country": p["country"], "marker": p["marker"]}
                 for k, p in PORTS.items()},
        "lanes": {k: {"name": l["name"], "bbox": l["bbox"]} for k, l in LANES.items()},
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
