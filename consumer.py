from confluent_kafka import Consumer, KafkaError
import clickhouse_connect
import json

# Kafka and ClickHouse configurations
kafka_topic = 'demo1'
kafka_bootstrap_servers = 'localhost:9092'
kafka_group_id = 'consumer_group_1'
clickhouse_host = 'localhost'
clickhouse_port = 8123
clickhouse_database = 'default'
clickhouse_table = 'forex_data_flat'
clickhouse_user = 'default'
clickhouse_password = ''

# Initialize ClickHouse client
def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=clickhouse_host,
        port=clickhouse_port,
        username=clickhouse_user,
        password=clickhouse_password,
        database=clickhouse_database
    )

# Function to insert data into ClickHouse
def insert_into_clickhouse(client, table, data):
    query = f"INSERT INTO {table} VALUES"
    client.insert(table, data)

def main():
    # Initialize Kafka consumer
    consumer = Consumer({
        'bootstrap.servers': kafka_bootstrap_servers,
        'group.id': kafka_group_id,
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe([kafka_topic])
    clickhouse_client = get_clickhouse_client()

    try:
        while True:
            msg = consumer.poll(1.0)  # Poll with a 1 second timeout

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print(f"{msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}")
                elif msg.error():
                    print(f"Kafka error: {msg.error()}")
                continue

            # Parse the message and insert into ClickHouse
            record = json.loads(msg.value().decode('utf-8'))
            flattened_record = (
            record['ticker'],
            record['queryCount'],
            record['resultsCount'],
            record['adjusted'],
            record['results'][0]['v'],
            record['results'][0]['vw'],
            record['results'][0]['o'],
            record['results'][0]['c'],
            record['results'][0]['h'],
            record['results'][0]['l'],
            record['results'][0]['t'],
            record['results'][0]['n'],
            record['status'],
            record['request_id'],
            record['count']
            )

            # Convert the flattened record to a tuple and print it
            record_tuple = tuple(flattened_record)
            print(record_tuple)
            insert_into_clickhouse(clickhouse_client, clickhouse_table, [record_tuple])

            print(f"Consumed message: {record}")

    except KeyboardInterrupt:
        pass
    finally:
        # Close the consumer and flush ClickHouse client
        consumer.close()
        clickhouse_client.close()

if __name__ == '__main__':
    main()
