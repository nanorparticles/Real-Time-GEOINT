# Real-Time-Geospatial-Intelligence-System
Key Features:

* Real-time Data Processing: Using Kafka for streaming data ingestion and processing.
* Geospatial Intelligence: Integration of OSM PBF data and geospatial analysis capabilities.
* Machine Learning: Implementing ML models for anomaly detection and predictive analytics.
* Interactive Dashboard: Developing a user-friendly interface for visualizing geospatial insights.
* Scalability and Deployment: Deploying on cloud platforms with Docker containers for scalability and ease of deployment. 

## Quick Start (S3 + PostGIS)

Prerequisites:
- Docker and Docker Compose
- An S3 bucket/key containing an `.osm.pbf` file (e.g., Geofabrik extract)

Environment variables (create a `.env` next to `docker-compose.yml`):

```
S3_BUCKET=your-bucket
S3_KEY=path/to/your.osm.pbf
AWS_REGION=us-east-1
# Optional for S3-compatible storage:
S3_ENDPOINT_URL=
# If required (do not commit secrets):
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
```

Run the stack:

```
docker compose up --build
```

Services:
- Kafka at `localhost:9092`
- PostGIS at `localhost:5432` (user: `postgres`, pwd: `postgres`, db: `gis`)
- Producer streams OSM ways (highways/waterways/railways) as GeoJSON to Kafka topic `geospatial-data`.
- Consumer upserts features into PostGIS table `features` using PostGIS geometry.
- Dashboard (FastAPI + Leaflet) at `http://localhost:8000`.
- Anomaly detector flags outliers (length/area) and stores them in PostGIS.

Inspect data:

```
psql -h localhost -U postgres -d gis -c "SELECT osm_type, id, ST_AsText(geom) FROM features LIMIT 5;"
```

Open the dashboard:

```
http://localhost:8000
```
Click "Load Current View" to load features within the map bounds.

Toggle "Show anomalies" to overlay outliers (red). Outliers are computed with a robust rolling statistic (MAD):
- Lines: unusual length in meters (EPSG:3857 projection)
- Polygons: unusual area in m² (EPSG:3857 projection)
Tune with env vars: `ANOM_THRESHOLD` (default 4.0), `ANOM_WIN_LENGTH`, `ANOM_WIN_AREA`.

Model-based anomalies (IsolationForest) are also supported and stored as separate metrics with `_iforest` suffix, e.g., `length_m_iforest`, `area_m2_iforest`.

IsolationForest settings (env):
- `ANOM_USE_IFOREST` (default `true`)
- `ANOM_IFOREST_CONTAM` (default `0.01`) — expected anomaly fraction
- `ANOM_IFOREST_MIN_TRAIN` (default `512`) — minimum samples before first fit
- `ANOM_IFOREST_RETRAIN_EVERY` (default `200`) — retrain cadence
- `ANOM_IFOREST_BUFFER` (default `5000`) — rolling buffer size

## Configuration

- Kafka configs: `config/kafka_producer_config.json`, `config/kafka_consumer_config.json` (env overrides supported via `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, etc.)
- PostGIS defaults via env (`PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`)
- S3 via env (`S3_BUCKET`, `S3_KEY`, `AWS_REGION`, optional `S3_ENDPOINT_URL`)

## Notes

- The producer uses `pyosmium` with `locations=True` and currently emits only ways tagged with `highway`, `waterway`, or `railway` to keep volume manageable.
- To widen coverage (e.g., buildings), extend the producer handler to emit polygons and points as needed.
- For production, consider batching, stronger error handling, metrics, and partitioning keys (e.g., by tile or feature type).
