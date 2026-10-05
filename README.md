# 🚢 Grain Vessel Tracker — 3D

A 3D globe tracking live cargo vessels inside grain-export port zones, built
with real-time AIS data. The physical flow behind the futures prices.

**Live demo:** *(deploy to GitHub Pages — see below)*
**Companion:** [grain-market-dashboard](https://github.com/josea12345/grain-market-dashboard) — futures prices, forward curves, positioning for the same markets these vessels serve.

## What it does

- **Collector** (`collector/collector.py`) connects to [AISStream.io](https://aisstream.io)'s
  free WebSocket feed, subscribes to bounding boxes around 20 major grain export
  ports (US Gulf, US Pacific Northwest, Brazil, Argentina, Canada, Black Sea,
  France, Australia), keeps the latest position per vessel,
  writes `data/vessels.json` for the globe, and archives every snapshot
  into `data/vessels.db` so voyage history builds from day one.
- **Web app** (`index.html`) — a MagicPort-style vessel directory on a live dark-mode
  3D map ([MapLibre GL](https://maplibre.org) + CARTO basemap, globe projection):
  search vessels by name or MMSI, filter by product focus (corn, soybeans, soybean meal,
  soybean oil, wheat — mapped honestly at the port-zone level), port zone, and
  underway/at-anchor status; expandable vessel cards with AIS details (speed, course,
  destination, position); "Show on map" smoothly flies the map to any vessel; glowing
  markers for vessels and port gateways.

## Honest limitations (read before citing this in an interview)

- AISStream's position reports do **not** include ship type — it arrives in separate
  static-data messages the free feed doesn't bundle. So this tracker shows **all** vessel traffic
  inside the port zones (bulkers, tankers, tugs, everything with AIS on), labeled honestly
  as such. "Grain vessel" here means *a vessel inside a grain-port bounding box* —
  a heuristic, not a cargo manifest. Commercial platforms (e.g. Kpler) combine AIS with port
  lineup data to identify actual cargoes; this project does not.
- The collector also listens to **ShipStaticData** messages, so cards show the crew-declared
  **destination**, the declared **ETA**, and the real AIS **navigational status**
  (underway / at anchor / moored). "Est. transit" is a rough great-circle distance ÷ current
  speed calculation, labeled as such.
- What AIS **cannot** tell you: whether a vessel is loading vs unloading, or what cargo
  it carries. Movement status and destinations are as deep as free AIS goes.
- AISStream is **real-time only** — there is no free historical endpoint. The SQLite
  archive is how history accumulates: it starts the day you first run the collector.
- Mid-ocean AIS coverage can be patchy; port zones are solid.

## Setup

1. Get a free API key at [aisstream.io](https://aisstream.io) (sign in with GitHub).
2. `pip install -r collector/requirements.txt`
3. `export AISSTREAM_API_KEY=your_key_here`
4. `python collector/collector.py --seconds 120` — writes `data/vessels.json`
5. Serve the folder (`python -m http.server`) and open `index.html`.

For continuous updates, the included GitHub Actions workflow
(`.github/workflows/collect.yml`) runs the collector hourly and commits the fresh
snapshot — the globe stays live with zero servers. Add `AISSTREAM_API_KEY` as a repo
secret. (One connection per API key; the collector holds it only while running.)

## Roadmap

- v2: more ports (Rosario, Vancouver, Odesa/Chornomorsk), USDA weekly export-inspection
  volumes under each port marker, Panama/Suez chokepoint counters, ship-type filtering
  via static-data messages
- v3: voyage trails from the SQLite archive, port-congestion view

## Stack

Python (collector) · AISStream.io WebSocket (free tier) · CesiumJS (3D globe) ·
SQLite (position archive) · GitHub Actions + Pages (automation + hosting, $0)
