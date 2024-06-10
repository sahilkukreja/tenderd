from kafka import KafkaConsumer
import json
import psycopg2
import os
import logging
from datetime import datetime
import signal
import sys

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('DataPipelineLogger')

# Global variable to keep track of the anomaly summary
anomaly_summary = {
    'total_messages': 0,
    'invalid_format': 0,
    'missing_value': 0,
    'outliers': 0
}


def signal_handler(sig, frame):
    logger.info('Received termination signal. Generating summary report...')
    generate_summary_report()
    sys.exit(0)


# Register the signal handler for graceful shutdown
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def check_kafka_connection():
    try:
        # Kafka consumer setup
        consumer = KafkaConsumer(
            'test_topic',
            bootstrap_servers='kafka:9092',
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='my-group-1',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        logger.info("Kafka connection established successfully")
        return consumer
    except Exception as e:
        logger.error(f"Failed to connect to Kafka: {e}")
        raise


def check_postgres_connection():
    try:
        # PostgreSQL connection setup
        postgres_host = os.getenv('POSTGRES_HOST')
        postgres_port = os.getenv('POSTGRES_PORT')
        postgres_db = os.getenv('POSTGRES_DB')
        postgres_user = os.getenv('POSTGRES_USER')
        postgres_password = os.getenv('POSTGRES_PASSWORD')

        connection = psycopg2.connect(
            host=postgres_host,
            port=postgres_port,
            database=postgres_db,
            user=postgres_user,
            password=postgres_password
        )

        cursor = connection.cursor()
        logger.info("PostgreSQL connection established successfully")
        return connection, cursor
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise


def consume(consumer, connection, cursor):
    logger.info('Starting consumption of Kafka messages')

    def validate_format(data):
        required_keys = {'unique_id', 'room_id', 'noted_date', 'temp', 'in_out'}
        if not required_keys.issubset(data.keys()):
            logger.warning(f"Invalid format: {data}")
            return False
        return True

    def validate_missing_values(data):
        for key in ['unique_id', 'room_id', 'noted_date', 'temp']:
            if data.get(key) is None:
                logger.warning(f"Missing value for {key}: {data}")
                return True
        return False

    def validate_outliers(data):
        if not (0 <= data['temp'] <= 100):
            logger.warning(f"Outlier detected: {data}")
            return True
        return False

    def insert_into_db(data):
        try:
            noted_date = datetime.strptime(data['noted_date'], '%d-%m-%Y %H:%M')
            cursor.execute(
                "INSERT INTO temp_readings (unique_id, room_id, noted_date, temp, in_out) VALUES (%s, %s, %s, %s, %s)",
                (data['unique_id'], data['room_id'], noted_date, data['temp'], data['in_out'])
            )
            connection.commit()
        except Exception as e:
            connection.rollback()
            logger.error(f"Error inserting data into DB: {e}")

    def generate_summary_report():
        report = (
            f"Data Pipeline Summary Report ({datetime.now().isoformat()})\n"
            f"Total Messages Processed: {anomaly_summary['total_messages']}\n"
            f"Invalid Format: {anomaly_summary['invalid_format']}\n"
            f"Missing Values: {anomaly_summary['missing_value']}\n"
            f"Outliers: {anomaly_summary['outliers']}\n"
        )
        print(report)
        logger.info(report)

    while True:
        message_batch = consumer.poll(timeout_ms=1000)
        if not message_batch:
            continue

        for tp, messages in message_batch.items():
            for message in messages:
                data = message.value
                anomaly_summary['total_messages'] += 1

                if not validate_format(data):
                    anomaly_summary['invalid_format'] += 1
                    continue

                if validate_missing_values(data):
                    anomaly_summary['missing_value'] += 1
                    continue

                if validate_outliers(data):
                    anomaly_summary['outliers'] += 1
                    continue

                insert_into_db(data)

        generate_summary_report()


if __name__ == "__main__":
    try:
        kafka_consumer = check_kafka_connection()
        postgres_connection, postgres_cursor = check_postgres_connection()
        consume(kafka_consumer, postgres_connection, postgres_cursor)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        generate_summary_report()
