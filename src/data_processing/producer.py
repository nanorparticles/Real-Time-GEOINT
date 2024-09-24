import logging
from kafka import KafkaProducer
import boto3
import osmpbf
from io import BytesIO
from botocore.exceptions import NoCredentialsError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# AWS S3 Configuration
bucket_name = 'mymapdata'
file_key = 'data\\raw\\central-america-latest.osm.pbf'


# Kafka Configuration
topic = 'geospatial-data'
bootstrap_servers = ['localhost:9092']

def create_kafka_producer():
    """Create Kafka producer."""
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: v  # Send raw binary data without encoding
    )
    return producer

def download_pbf_from_s3(bucket_name, file_key):
    """Download PBF file from S3."""
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        file_data = response['Body'].read()
        logging.info(f"Downloaded PBF file from S3 bucket {bucket_name}, key: {file_key}")
        return file_data
    except NoCredentialsError:
        logging.error("AWS credentials not found.")
    except Exception as e:
        logging.error(f"Error downloading PBF file from S3: {e}")
        return None

def parse_and_send_to_kafka(producer, topic, pbf_data):
    """Parse PBF data and send structured data to Kafka."""
    try:
        # Parse PBF data
        file_stream = BytesIO(pbf_data)
        decoder = osmpbf.FileStream(file_stream)

        logging.info("Parsing PBF data and sending to Kafka.")

        for entity in decoder:
            if isinstance(entity, osmpbf.Node):  # If it's a Node
                message = {
                    "type": "node",
                    "id": entity.id,
                    "lat": entity.lat,
                    "lon": entity.lon,
                    "tags": entity.tags
                }
            elif isinstance(entity, osmpbf.Way):  # If it's a Way
                message = {
                    "type": "way",
                    "id": entity.id,
                    "refs": entity.refs,
                    "tags": entity.tags
                }
            elif isinstance(entity, osmpbf.Relation):  # If it's a Relation
                message = {
                    "type": "relation",
                    "id": entity.id,
                    "members": entity.members,
                    "tags": entity.tags
                }
            else:
                continue

            # Send each structured message to Kafka
            producer.send(topic, value=json.dumps(message).encode('utf-8'))

        producer.flush()  # Ensure all data is sent to Kafka
        logging.info("Data parsed and streamed to Kafka successfully.")
    
    except Exception as e:
        logging.error(f"Error parsing and sending PBF data to Kafka: {e}")

if __name__ == "__main__":
    # Initialize Kafka producer
    kafka_producer = create_kafka_producer()

    # Download PBF file from S3
    pbf_data = download_pbf_from_s3(bucket_name, file_key)

    if pbf_data:
        # Parse and send structured data to Kafka
        parse_and_send_to_kafka(kafka_producer, topic, pbf_data)
