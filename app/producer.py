import os
import sys
import json
from kafka import KafkaProducer
import logging
import csv

producer = KafkaProducer(bootstrap_servers='kafka:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('DataPipelineLogger')


def produce(file_name):
    csv_file_path = os.path.join(os.path.dirname(__file__), 'data', file_name)
    with open(csv_file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data = {
                'unique_id': row.get('id', None),
                'room_id': row.get('room_id', None),
                'noted_date': row.get('noted_date', None),
                'temp': float(row.get('temp', None)),
                'in_out': row.get('out_in', None)
            }
            producer.send('test_topic', data)
            print(f'Produced {data}')


if __name__ == "__main__":
    if len(sys.argv) != 2:
        logger.error("Usage: python producer.py <file_name>")
        sys.exit(1)

    file_name = sys.argv[1]
    produce(file_name)
