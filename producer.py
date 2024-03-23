from dotenv import dotenv_values

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

    config = dotenv_values(".env")
    sr_config = {
        "url": config["url"],
        "basic.auth.user.info": config["basic.auth.user.info"],
    }

    schema_registry_client = SchemaRegistryClient(sr_config)
    youtube_videos_value_schema = schema_registry_client.get_latest_version(
        "youtube_videos-value"
    )

    kafka_config = {
        "bootstrap.servers": config["bootstrap.servers"],
        "security.protocol": config["security.protocol"],
        "sasl.mechanisms": config["sasl.mechanisms"],
        "sasl.username": config["sasl.username"],
        "sasl.password": config["sasl.password"],
        "key.serializer": StringSerializer(),
        "value.serializer": AvroSerializer(
            schema_registry_client,
            youtube_videos_value_schema.schema.schema_str,
        ),
    }

    Producer = SerializingProducer(kafka_config)

    return Producer
