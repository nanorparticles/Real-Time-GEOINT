import os
import json
import logging
import tempfile
from typing import Optional

import boto3
import pyosmium
from kafka import KafkaProducer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_json(path: str) -> dict:
    with open(path, 'r') as f:
        return json.load(f)


def build_kafka_producer(config_path: str) -> KafkaProducer:
    cfg = load_json(config_path)
    bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS') or ','.join(cfg.get('bootstrap_servers', ['localhost:9092']))
    # kafka-python expects a list for bootstrap_servers
    servers = [s.strip() for s in bootstrap.split(',') if s.strip()]
    return KafkaProducer(
        bootstrap_servers=servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        linger_ms=int(os.getenv('KAFKA_LINGER_MS', '50')),
        acks=os.getenv('KAFKA_ACKS', '1')
    )


class OSMFeatureEmitter(pyosmium.SimpleHandler):
    def __init__(self, kafka_producer: KafkaProducer, kafka_topic: str):
        super().__init__()
        self.producer = kafka_producer
        self.topic = kafka_topic
        self.count_lines = 0
        self.count_polys = 0
        self.count_points = 0

    def _emit(self, feature: dict):
        self.producer.send(self.topic, value=feature)

    def node(self, n):
        tags = dict(n.tags)
        if not tags:
            return
        # Emit notable POIs as points
        poi_keys = ('amenity', 'shop', 'tourism', 'public_transport', 'aeroway', 'natural', 'man_made')
        if not any(k in tags for k in poi_keys):
            return
        try:
            lon, lat = n.location.lon, n.location.lat
        except Exception:
            return
        feature = {
            'type': 'Feature',
            'geometry': {
                'type': 'Point',
                'coordinates': [lon, lat]
            },
            'properties': {
                'id': int(n.id),
                'osm_type': 'node',
                'tags': tags
            }
        }
        self._emit(feature)
        self.count_points += 1
        if self.count_points % 2000 == 0:
            logging.info(f"Emitted {self.count_points} point features")

    def way(self, w):
        tags = dict(w.tags)
        if len(w.nodes) < 2:
            return

        # Buildings as polygons from closed ways
        if 'building' in tags:
            try:
                ring = [(n.lon, n.lat) for n in w.nodes]
            except Exception:
                return
            if len(ring) < 3:
                return
            if ring[0] != ring[-1]:
                ring.append(ring[0])
            if len(ring) < 4:
                return
            feature = {
                'type': 'Feature',
                'geometry': {
                    'type': 'Polygon',
                    'coordinates': [ring]
                },
                'properties': {
                    'id': int(w.id),
                    'osm_type': 'way',
                    'tags': tags
                }
            }
            self._emit(feature)
            self.count_polys += 1
            if self.count_polys % 1000 == 0:
                logging.info(f"Emitted {self.count_polys} polygon features")
            return

        # Common linear features
        if any(k in tags for k in ('highway', 'waterway', 'railway')):
            try:
                coords = [(n.lon, n.lat) for n in w.nodes]
            except Exception:
                return
            feature = {
                'type': 'Feature',
                'geometry': {
                    'type': 'LineString',
                    'coordinates': coords
                },
                'properties': {
                    'id': int(w.id),
                    'osm_type': 'way',
                    'tags': tags
                }
            }
            self._emit(feature)
            self.count_lines += 1
            if self.count_lines % 2000 == 0:
                logging.info(f"Emitted {self.count_lines} line features")


def download_s3_object(bucket: str, key: str, region_name: Optional[str] = None, endpoint_url: Optional[str] = None) -> str:
    """Download S3 object to a temporary file and return the file path."""
    logging.info(f"Downloading s3://{bucket}/{key} ...")
    s3 = boto3.client('s3', region_name=region_name, endpoint_url=endpoint_url)
    tmp = tempfile.NamedTemporaryFile(prefix='osm_', suffix='.pbf', delete=False)
    with tmp as f:
        s3.download_fileobj(bucket, key, f)
    logging.info(f"Downloaded to {tmp.name}")
    return tmp.name


def process_osm_pbf(producer: KafkaProducer, kafka_topic: str, pbf_path: str):
    handler = OSMFeatureEmitter(producer, kafka_topic)
    logging.info(f"Parsing {pbf_path} with pyosmium...")
    handler.apply_file(pbf_path, locations=True)
    total = handler.count_lines + handler.count_polys + handler.count_points
    logging.info(
        f"Finished parsing. Emitted {total} features "
        f"(lines={handler.count_lines}, polys={handler.count_polys}, points={handler.count_points})."
    )


def main():
    # Config paths
    kafka_cfg_path = os.getenv('KAFKA_PRODUCER_CONFIG', 'config/kafka_producer_config.json')
    s3_bucket = os.getenv('S3_BUCKET', 'mymapdata')
    s3_key = os.getenv('S3_KEY', 'central-america-latest.osm.pbf')
    s3_region = os.getenv('AWS_REGION')  # optional
    s3_endpoint = os.getenv('S3_ENDPOINT_URL')  # optional (for S3-compatible stores)
    kafka_topic = os.getenv('KAFKA_TOPIC', 'geospatial-data')

    producer = build_kafka_producer(kafka_cfg_path)

    try:
        pbf_path = download_s3_object(s3_bucket, s3_key, region_name=s3_region, endpoint_url=s3_endpoint)
        process_osm_pbf(producer, kafka_topic, pbf_path)
    except Exception as e:
        logging.error(f"Error in producer: {e}")
    finally:
        try:
            producer.flush()
        except Exception:
            pass
        producer.close()


if __name__ == '__main__':
    main()
