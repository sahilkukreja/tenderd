import pytest
from kafka import KafkaConsumer
import json
import os
from producer import produce

@pytest.fixture(scope="module")
def kafka_consumer():
    consumer = KafkaConsumer(
        'test_topic',
        bootstrap_servers='kafka:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='test-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    return consumer

def test_produce(kafka_consumer):
    produce()
    kafka_consumer.poll(timeout_ms=1000)
    kafka_consumer.seek_to_beginning()

    for message in kafka_consumer:
        data = message.value
        assert 'value' in data
        assert isinstance(data['value'], float)
        break
