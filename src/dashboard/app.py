import os
import json
from typing import Optional

import psycopg2
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse

app = FastAPI(title="Geo Dashboard API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_pg_conn():
    host = os.getenv('PGHOST', 'localhost')
    port = int(os.getenv('PGPORT', '5432'))
    db = os.getenv('PGDATABASE', 'gis')
    user = os.getenv('PGUSER', 'postgres')
    pwd = os.getenv('PGPASSWORD', 'postgres')
    return psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pwd)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/features/within")
def features_within(
    bbox: str = Query(..., description="minx,miny,maxx,maxy in EPSG:4326"),
    limit: int = Query(500, ge=1, le=5000),
):
    try:
        parts = [float(x) for x in bbox.split(',')]
        if len(parts) != 4:
            raise ValueError
        minx, miny, maxx, maxy = parts
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid bbox; expected minx,miny,maxx,maxy")

    sql = (
        "SELECT osm_type, id, tags, ST_AsGeoJSON(geom) "
        "FROM features "
        "WHERE geom && ST_MakeEnvelope(%s,%s,%s,%s, 4326) "
        "AND ST_Intersects(geom, ST_MakeEnvelope(%s,%s,%s,%s, 4326)) "
        "LIMIT %s"
    )

    features = []
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (minx, miny, maxx, maxy, minx, miny, maxx, maxy, limit))
            for osm_type, oid, tags, geom_json in cur.fetchall():
                geom = json.loads(geom_json)
                features.append({
                    "type": "Feature",
                    "geometry": geom,
                    "properties": {
                        "osm_type": osm_type,
                        "id": int(oid),
                        "tags": tags,
                    }
                })

    return JSONResponse({"type": "FeatureCollection", "features": features})


# Anomalies endpoint (same bbox pattern)
@app.get("/anomalies/within")
def anomalies_within(
    bbox: str = Query(..., description="minx,miny,maxx,maxy in EPSG:4326"),
    limit: int = Query(500, ge=1, le=5000),
):
    try:
        parts = [float(x) for x in bbox.split(',')]
        if len(parts) != 4:
            raise ValueError
        minx, miny, maxx, maxy = parts
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid bbox; expected minx,miny,maxx,maxy")

    sql = (
        "SELECT osm_type, id, tags, metric, value, score, ST_AsGeoJSON(geom) "
        "FROM anomalies "
        "WHERE geom && ST_MakeEnvelope(%s,%s,%s,%s, 4326) "
        "AND ST_Intersects(geom, ST_MakeEnvelope(%s,%s,%s,%s, 4326)) "
        "ORDER BY score DESC "
        "LIMIT %s"
    )

    features = []
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (minx, miny, maxx, maxy, minx, miny, maxx, maxy, limit))
            for osm_type, oid, tags, metric, value, score, geom_json in cur.fetchall():
                geom = json.loads(geom_json)
                features.append({
                    "type": "Feature",
                    "geometry": geom,
                    "properties": {
                        "osm_type": osm_type,
                        "id": int(oid),
                        "tags": tags,
                        "anomaly": {
                            "metric": metric,
                            "value": value,
                            "score": score
                        }
                    }
                })

    return JSONResponse({"type": "FeatureCollection", "features": features})

# Serve the static dashboard
app.mount("/", StaticFiles(directory="src/dashboard/static", html=True), name="static")
