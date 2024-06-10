import unittest
from unittest.mock import patch, MagicMock
from kafka import KafkaConsumer
import json
import psycopg2
from datetime import datetime
import os

# Import the functions to be tested
from consumer import check_kafka_connection, check_postgres_connection, consume, anomaly_summary


class TestConsumer(unittest.TestCase):

    @patch('consumer.KafkaConsumer')
    def test_check_kafka_connection(self, mock_kafka_consumer):
        mock_kafka_consumer.return_value = MagicMock(spec=KafkaConsumer)
        consumer = check_kafka_connection()
        self.assertIsInstance(consumer, KafkaConsumer)
        mock_kafka_consumer.assert_called_once()

    @patch('consumer.psycopg2.connect')
    def test_check_postgres_connection(self, mock_connect):
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        connection, cursor = check_postgres_connection()

        self.assertEqual(connection, mock_connection)
        self.assertEqual(cursor, mock_cursor)
        mock_connect.assert_called_once_with(
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT'),
            database=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD')
        )

    @patch('consumer.check_postgres_connection')
    @patch('consumer.check_kafka_connection')
    def test_consume(self, mock_check_kafka_connection, mock_check_postgres_connection):
        # Setup mock Kafka consumer
        mock_consumer = MagicMock()
        mock_consumer.poll.return_value = {
            'test_topic': [MagicMock(value=json.dumps({
                'unique_id': '123',
                'room_id': '456',
                'noted_date': '01-01-2020 12:00',
                'temp': 25,
                'in_out': 'in'
            }).encode('utf-8'))]
        }
        mock_check_kafka_connection.return_value = mock_consumer

        # Setup mock PostgreSQL connection and cursor
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_check_postgres_connection.return_value = (mock_connection, mock_cursor)

        # Reset anomaly summary for each test
        global anomaly_summary
        anomaly_summary = {
            'total_messages': 0,
            'invalid_format': 0,
            'missing_value': 0,
            'outliers': 0
        }

        consume(mock_consumer, mock_connection, mock_cursor)

        # Assertions to check if the data is processed correctly
        self.assertEqual(anomaly_summary['total_messages'], 1)
        self.assertEqual(anomaly_summary['invalid_format'], 0)
        self.assertEqual(anomaly_summary['missing_value'], 0)
        self.assertEqual(anomaly_summary['outliers'], 0)

        mock_cursor.execute.assert_called_once_with(
            "INSERT INTO temp_readings (unique_id, room_id, noted_date, temp, in_out) VALUES (%s, %s, %s, %s, %s)",
            ('123', '456', datetime.strptime('01-01-2020 12:00', '%d-%m-%Y %H:%M'), 25, 'in')
        )


if __name__ == '__main__':
    unittest.main()
