import logging
import signal
import json
from kafka import KafkaConsumer
import osmium
from io import BytesIO

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load Kafka consumer configuration
consumer_config_path = 'config/kafka_consumer_config.json'

def load_consumer_config():
    with open(consumer_config_path, 'r') as file:
        config = json.load(file)
    return config

# Create Kafka consumer
def create_kafka_consumer():
    config = load_consumer_config()
    consumer = KafkaConsumer(
        config['topic'],
        bootstrap_servers=config['bootstrap_servers'],
        group_id='my-consumer-group',
        value_deserializer=lambda x: x,  # Receive raw bytes
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )
    return consumer

# Custom handler to process OSM PBF data
class SimpleHandler(osmium.SimpleHandler):
    def __init__(self):
        super().__init__()
        self.count = 0

    def node(self, n):
        self.count += 1
        logging.info(f"Processed node: id={n.id}, location=({n.location.lat}, {n.location.lon})")

    def way(self, w):
        self.count += 1
        logging.info(f"Processed way: id={w.id}, nodes={len(w.nodes)}")

    def relation(self, r):
        self.count += 1
        logging.info(f"Processed relation: id={r.id}, members={len(r.members)}")

# Process the full PBF geospatial data
def process_geospatial_data(pbf_data):
    try:
        buffer = BytesIO(pbf_data)  # Wrap the raw data in a BytesIO stream
        handler = SimpleHandler()
        handler.apply_file(buffer, "osm.pbf")  # Process the PBF data
        logging.info(f"Processed {handler.count} OSM objects.")
    except Exception as e:
        logging.error(f"Error processing geospatial data: {e}")

def signal_handler(sig, frame):
    logging.info("Received signal to terminate, shutting down consumer.")
    global running
    running = False

if __name__ == "__main__":
    running = True
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Initialize Kafka consumer
    kafka_consumer = create_kafka_consumer()

    try:
        # Consume full PBF data from Kafka and process it
        for message in kafka_consumer:
            if not running:
                break
            logging.info(f"Received message from Kafka (size: {len(message.value)} bytes)")
            
            raw_pbf_data = message.value  # Raw PBF data from Kafka
            process_geospatial_data(raw_pbf_data)
    except Exception as e:
        logging.error(f"Error in consumer: {e}")
    finally:
        kafka_consumer.close()
        logging.info("Consumer closed successfully.")
