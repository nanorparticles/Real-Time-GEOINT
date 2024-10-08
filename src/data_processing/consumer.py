import logging
import json
from kafka import KafkaConsumer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Kafka Consumer configuration
consumer = KafkaConsumer(
    'osm-geospatial-data',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Function to consume data from Kafka and process it
def consume_osm_data():
    try:
        for message in consumer:
            geojson_data = message.value

            # Process the GeoJSON data (this is where your custom logic would go)
            process_geojson_data(geojson_data)
    
    except Exception as e:
        logging.error(f"Error consuming data from Kafka: {e}")

# Function to process the GeoJSON data
def process_geojson_data(data):
    # Example processing: Print the data (can be replaced with actual processing logic)
    logging.info(f"Received GeoJSON data: {data}")

if __name__ == "__main__":
    logging.info("Starting Kafka consumer...")
    consume_osm_data()
