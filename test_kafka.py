import json
import os

from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

NAMESPACE = os.environ["EVENT_HUBS_NAMESPACE"]
EVENT_HUB = os.environ["EVENT_HUB_NAME"]
CONNECTION_STRING = os.environ["EVENT_HUB_CONNECTION_STRING"]

print("Creating producer...")

producer = KafkaProducer(
    bootstrap_servers=[f"{NAMESPACE}:9093"],
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username="$ConnectionString",
    sasl_plain_password=CONNECTION_STRING,
    request_timeout_ms=60000,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Producer created.")

future = producer.send(
    EVENT_HUB,
    {
        "test": True,
        "message": "Hello Azure Event Hubs"
    }
)

print("Waiting for Azure...")
result = future.get(timeout=60)

print("SUCCESS!")
print(result)

producer.flush()
producer.close()