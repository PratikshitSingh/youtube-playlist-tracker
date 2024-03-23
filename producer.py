
import os
from dotenv import load_dotenv

from confluent_kafka import SerializingProducer

from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


def delivery_report(err, event):
    if err:
        print(f'Delivery failed for event: {event.key().decode("utf-8")}: {err}')
    else:
        print(f'Successfully delivered message: {event.key().decode("utf-8")}')


def producer():

    load_dotenv()

    sr_config = {
        "url": os.getenv("url"),
        "basic.auth.user.info": os.getenv("basic.auth.user.info")
    }

    schema_registry_client = SchemaRegistryClient(sr_config)
    youtube_videos_value_schema = schema_registry_client.get_latest_version(
        "youtube_videos-value"
    )

    kafka_config = {
        "bootstrap.servers": os.getenv("bootstrap.servers"),
        "security.protocol": os.getenv("security.protocol"),
        "sasl.mechanisms": os.getenv("sasl.mechanisms"),
        "sasl.username": os.getenv("sasl.username"),
        "sasl.password": os.getenv("sasl.password"),
        "key.serializer": StringSerializer(),
        "value.serializer": AvroSerializer(
            schema_registry_client,
            youtube_videos_value_schema.schema.schema_str,
        ),
    }

    Producer = SerializingProducer(kafka_config)

    return Producer
