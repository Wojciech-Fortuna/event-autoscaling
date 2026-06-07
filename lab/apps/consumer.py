import os
import time
from confluent_kafka import Consumer, KafkaException

bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
topic = os.getenv("TOPIC_NAME", "events")
consumer_group = os.getenv("CONSUMER_GROUP", "event-processors")
processing_delay_ms = int(os.getenv("PROCESSING_DELAY_MS", "100"))

consumer = Consumer({
    "bootstrap.servers": bootstrap_servers,
    "group.id": consumer_group,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True
})


def process_message(message):
    time.sleep(processing_delay_ms / 1000)


if __name__ == "__main__":
    print("Starting consumer")
    print(f"Topic: {topic}")
    print(f"Consumer group: {consumer_group}")
    print(f"Processing delay: {processing_delay_ms} ms")

    consumer.subscribe([topic])

    processed = 0
    last_log_time = time.time()

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())

            process_message(msg.value())
            processed += 1

            now = time.time()
            if now - last_log_time >= 5:
                print(f"Processed total: {processed}")
                last_log_time = now

    except KeyboardInterrupt:
        print("Stopping consumer")

    finally:
        consumer.close()
