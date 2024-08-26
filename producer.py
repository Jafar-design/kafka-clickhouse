import requests
from confluent_kafka import Producer
import json
import time

# API and Kafka configurations
api_url = ''  # Replace with your API endpoint
kafka_topic = 'demo1'
kafka_bootstrap_servers = 'localhost:9092'  # Replace with your Kafka broker


# Function to fetch data from API
def fetch_data_from_api():
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()  # Assuming the API returns JSON data
    else:
        response.raise_for_status()

# Delivery report callback function (optional)
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def main():
    # Initialize Kafka producer
    producer = Producer({
        'bootstrap.servers': kafka_bootstrap_servers
    })

    while True:
        try:
            # Fetch data from API
            data = fetch_data_from_api()

            # Produce data to Kafka
            producer.produce(kafka_topic, value=json.dumps(data), callback=delivery_report)

            # Poll for delivery reports (callback will be triggered)
            producer.poll(0)

            # Log success (optional)
            print(f"Produced data to Kafka: {data}")

            # Wait for a while before fetching again
            time.sleep(60)  # Fetch data every 60 seconds, adjust as needed

        except Exception as e:
            # Log errors (optional)
            print(f"Error: {e}")
            time.sleep(60)  # Wait before retrying

    # Flush any remaining messages before shutting down
    producer.flush()

if __name__ == '__main__':
    main()
