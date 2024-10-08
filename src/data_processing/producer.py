import boto3
import logging
import json
from kafka import KafkaProducer
import OSMPythonTools
from io import BytesIO
print(dir(OSMPythonTools))
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize S3 client
s3 = boto3.client('s3', endpoint_url='https://mymapdata.s3.amazonaws.com/central-america-latest.osm.pbf')

# List objects in the bucket
bucket = "mymapdata"
response = s3.list_objects_v2(Bucket=bucket)

# Print out all the keys in the bucket to verify the exact key name
if 'Contents' in response:
    for obj in response['Contents']:
        print(f"Key: {obj['Key']}")
else:
    print("No objects found in the bucket.")



# Kafka Producer configuration
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'], 
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Function to fetch OSM data from S3 and process it with OSMPythonTools
def process_osm_from_s3(bucket_name, object_key, kafka_topic):
    try:
        # Fetch the OSM PBF file from S3
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        osm_pbf_data = BytesIO(response['Body'].read())

        # Parse OSM data using OSMPythonTools
        osm_data = osmxml.OSM(osm_pbf_data)

        # Iterate through the OSM data and convert it to GeoJSON
        for element in osm_data.ways:
            geojson_data = {
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": [(node.lon, node.lat) for node in element.nodes]
                },
                "properties": {
                    "id": element.id,
                    "tags": element.tags
                }
            }
            
            # Send parsed GeoJSON data to Kafka
            send_to_kafka(kafka_topic, geojson_data)

        logging.info("OSM data processing and Kafka streaming completed.")
        
    except Exception as e:
        logging.error(f"Error processing OSM data from S3: {e}")

# Function to send data to Kafka
def send_to_kafka(topic, data):
    try:
        producer.send(topic, value=data)
        logging.info(f"Successfully sent data to Kafka topic {topic}")
    except Exception as e:
        logging.error(f"Error sending data to Kafka: {e}")

if __name__ == "__main__":
    # S3 bucket and object details
    bucket_name = "mymapdata"
    object_key = 'central-america-latest.osm.pbf'

    # Kafka topic to send data
    kafka_topic = "geospatial-data"

    # Process OSM data from S3 and send to Kafka
    process_osm_from_s3(bucket_name, object_key, kafka_topic)

    # Cleanup
    producer.close()
