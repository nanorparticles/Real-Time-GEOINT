import os
import json
import logging

import psycopg2
from kafka import KafkaConsumer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_json(path: str) -> dict:
    with open(path, 'r') as f:
        return json.load(f)


def get_kafka_consumer():
    cfg_path = os.getenv('KAFKA_CONSUMER_CONFIG', 'config/kafka_consumer_config.json')
    cfg = load_json(cfg_path)
    topic = os.getenv('KAFKA_TOPIC', cfg.get('topic', 'geospatial-data'))
    group_id = os.getenv('KAFKA_GROUP_ID', cfg.get('group_id', 'geospatial-consumer'))
    bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS') or ','.join(cfg.get('bootstrap_servers', ['localhost:9092']))
    servers = [s.strip() for s in bootstrap.split(',') if s.strip()]

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=servers,
        group_id=group_id,
        enable_auto_commit=True,
        auto_offset_reset=os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest'),
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    return consumer


def get_pg_conn():
    # Prefer env vars compatible with libpq
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
            CREATE TABLE IF NOT EXISTS features (
                osm_type TEXT NOT NULL,
                id BIGINT NOT NULL,
                tags JSONB,
                geom GEOMETRY(Geometry, 4326),
                PRIMARY KEY (osm_type, id)
            );
            """
        )


def upsert_feature(conn, feature: dict):
    props = feature.get('properties', {})
    osm_type = props.get('osm_type', 'way')
    oid = int(props.get('id'))
    tags = json.dumps(props.get('tags', {}))
    geom = json.dumps(feature.get('geometry', {}))
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO features (osm_type, id, tags, geom)
            VALUES (%s, %s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))
            ON CONFLICT (osm_type, id)
            DO UPDATE SET tags = EXCLUDED.tags, geom = EXCLUDED.geom;
            """,
            (osm_type, oid, tags, geom)
        )


def consume_and_store():
    consumer = get_kafka_consumer()
    pg = get_pg_conn()
    ensure_schema(pg)
    logging.info("Starting Kafka consumer and writing to PostGIS...")
    try:
        for msg in consumer:
            feature = msg.value
            try:
                upsert_feature(pg, feature)
            except Exception as e:
                logging.error(f"DB error for feature {feature.get('properties', {}).get('id')}: {e}")
    except Exception as e:
        logging.error(f"Error consuming data from Kafka: {e}")


if __name__ == "__main__":
    consume_and_store()
