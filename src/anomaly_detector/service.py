import os
import json
import logging
from collections import deque
from typing import Deque, Tuple, Optional

import psycopg2
from kafka import KafkaConsumer
from shapely.geometry import shape
from shapely.ops import transform as shp_transform
from pyproj import Transformer
import numpy as np
from sklearn.ensemble import IsolationForest

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class RollingMAD:
    def __init__(self, window: int = 2000):
        self.window = window
        self.values: Deque[float] = deque(maxlen=window)

    def update(self, x: float) -> Tuple[float, float, float]:
        self.values.append(x)
        vals = list(self.values)
        if not vals:
            return x, 0.0, 0.0
        vals_sorted = sorted(vals)
        n = len(vals_sorted)
        median = vals_sorted[n // 2] if n % 2 == 1 else 0.5 * (vals_sorted[n // 2 - 1] + vals_sorted[n // 2])
        abs_dev = [abs(v - median) for v in vals_sorted]
        abs_sorted = sorted(abs_dev)
        mad = abs_sorted[n // 2] if n % 2 == 1 else 0.5 * (abs_sorted[n // 2 - 1] + abs_sorted[n // 2])
        # Consistent with normal dist: scaled MAD
        scale = 1.4826 * mad if mad > 0 else 0.0
        return median, mad, scale

    def robust_z(self, x: float) -> float:
        median, mad, scale = self.update(x)
        if scale == 0:
            return 0.0
        return (x - median) / scale


class IFModel:
    """IsolationForest with rolling buffer and periodic retraining."""
    def __init__(self, capacity: int = 5000, min_train: int = 512, retrain_every: int = 200, contamination: float = 0.01, random_state: int = 42):
        self.buffer: Deque[float] = deque(maxlen=capacity)
        self.min_train = min_train
        self.retrain_every = retrain_every
        self.contamination = contamination
        self.random_state = random_state
        self.model: Optional[IsolationForest] = None
        self._seen_since_fit = 0

    def update(self, x: float) -> Optional[IsolationForest]:
        self.buffer.append(x)
        self._seen_since_fit += 1
        if len(self.buffer) >= self.min_train and (self.model is None or self._seen_since_fit >= self.retrain_every):
            X = np.array(self.buffer, dtype=np.float64).reshape(-1, 1)
            self.model = IsolationForest(
                n_estimators=200,
                contamination=self.contamination,
                random_state=self.random_state,
                n_jobs=-1,
            ).fit(X)
            self._seen_since_fit = 0
        return self.model

    def score(self, x: float) -> Optional[float]:
        if self.model is None:
            return None
        s = float(self.model.decision_function(np.array([[x]], dtype=np.float64))[0])
        # Lower is more anomalous; return inverted so higher means more anomalous
        return -s


def get_kafka_consumer():
    topic = os.getenv('KAFKA_TOPIC', 'geospatial-data')
    bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    servers = [s.strip() for s in bootstrap.split(',') if s.strip()]
    group_id = os.getenv('KAFKA_GROUP_ID', 'anomaly-detector')
    return KafkaConsumer(
        topic,
        bootstrap_servers=servers,
        group_id=group_id,
        enable_auto_commit=True,
        auto_offset_reset=os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest'),
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )


def get_pg_conn():
    host = os.getenv('PGHOST', 'localhost')
    port = int(os.getenv('PGPORT', '5432'))
    db = os.getenv('PGDATABASE', 'gis')
    user = os.getenv('PGUSER', 'postgres')
    pwd = os.getenv('PGPASSWORD', 'postgres')
    conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pwd)
    conn.autocommit = True
    return conn


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS anomalies (
                osm_type TEXT NOT NULL,
                id BIGINT NOT NULL,
                metric TEXT NOT NULL,
                value DOUBLE PRECISION NOT NULL,
                score DOUBLE PRECISION NOT NULL,
                tags JSONB,
                geom GEOMETRY(Geometry, 4326),
                PRIMARY KEY (osm_type, id, metric)
            );
            """
        )


def upsert_anomaly(conn, feature: dict, metric: str, value: float, score: float):
    props = feature.get('properties', {})
    osm_type = props.get('osm_type', 'way')
    oid = int(props.get('id'))
    tags = json.dumps(props.get('tags', {}))
    geom = json.dumps(feature.get('geometry', {}))
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO anomalies (osm_type, id, metric, value, score, tags, geom)
            VALUES (%s, %s, %s, %s, %s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))
            ON CONFLICT (osm_type, id, metric)
            DO UPDATE SET value = EXCLUDED.value, score = EXCLUDED.score, tags = EXCLUDED.tags, geom = EXCLUDED.geom;
            """,
            (osm_type, oid, metric, value, score, tags, geom)
        )


def build_transformers():
    # Project WGS84 -> WebMercator (meters approx.) for simple length/area
    fwd = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
    return lambda geom: shp_transform(lambda x, y: fwd.transform(x, y), geom)


def main():
    consumer = get_kafka_consumer()
    conn = get_pg_conn()
    ensure_schema(conn)

    to_mercator = build_transformers()
    mad_length = RollingMAD(window=int(os.getenv('ANOM_WIN_LENGTH', '3000')))
    mad_area = RollingMAD(window=int(os.getenv('ANOM_WIN_AREA', '3000')))
    threshold = float(os.getenv('ANOM_THRESHOLD', '4.0'))

    use_if = os.getenv('ANOM_USE_IFOREST', 'true').lower() in ('1', 'true', 'yes')
    if_contam = float(os.getenv('ANOM_IFOREST_CONTAM', '0.01'))
    if_min_train = int(os.getenv('ANOM_IFOREST_MIN_TRAIN', '512'))
    if_retrain_every = int(os.getenv('ANOM_IFOREST_RETRAIN_EVERY', '200'))
    if_cap = int(os.getenv('ANOM_IFOREST_BUFFER', '5000'))

    if_model_len = IFModel(capacity=if_cap, min_train=if_min_train, retrain_every=if_retrain_every, contamination=if_contam)
    if_model_area = IFModel(capacity=if_cap, min_train=if_min_train, retrain_every=if_retrain_every, contamination=if_contam)

    logging.info("Anomaly detector started. Listening for features...")
    for msg in consumer:
        feature = msg.value
        geom = feature.get('geometry')
        if not geom:
            continue
        try:
            g = shape(geom)
        except Exception:
            continue
        try:
            g_m = to_mercator(g)
        except Exception:
            continue

        if g_m.geom_type == 'LineString' or g_m.geom_type == 'MultiLineString':
            val = g_m.length
            # Robust MAD-based anomaly
            score_mad = abs(mad_length.robust_z(val))
            if score_mad >= threshold and val > 0:
                upsert_anomaly(conn, feature, 'length_m_mad', float(val), float(score_mad))
            # Model-based anomaly (IsolationForest)
            if use_if:
                if_model_len.update(val)
                score_if = if_model_len.score(val)
                # IsolationForest predict uses contamination; here, threshold using zero boundary or top-k by score. Use > 0 as anomaly proxy.
                if score_if is not None and score_if > 0:
                    upsert_anomaly(conn, feature, 'length_m_iforest', float(val), float(score_if))
        elif g_m.geom_type == 'Polygon' or g_m.geom_type == 'MultiPolygon':
            val = g_m.area
            score_mad = abs(mad_area.robust_z(val))
            if score_mad >= threshold and val > 0:
                upsert_anomaly(conn, feature, 'area_m2_mad', float(val), float(score_mad))
            if use_if:
                if_model_area.update(val)
                score_if = if_model_area.score(val)
                if score_if is not None and score_if > 0:
                    upsert_anomaly(conn, feature, 'area_m2_iforest', float(val), float(score_if))
        # Points: skip for now or implement density-based methods later


if __name__ == '__main__':
    main()
