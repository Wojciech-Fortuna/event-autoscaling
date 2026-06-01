import os
import time
import json
import uuid
from confluent_kafka import Producer

bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
topic = os.getenv("TOPIC_NAME", "events")

messages_per_second = int(os.getenv("MESSAGES_PER_SECOND", "100"))
burst_messages_per_second = int(os.getenv("BURST_MESSAGES_PER_SECOND", "3000"))
burst_duration_seconds = int(os.getenv("BURST_DURATION_SECONDS", "60"))
message_size_bytes = int(os.getenv("MESSAGE_SIZE_BYTES", "1024"))

producer = Producer({
    "bootstrap.servers": bootstrap_servers
})


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")


def create_message():
    payload = {
        "id": str(uuid.uuid4()),
        "timestamp": time.time(),
        "data": "x" * message_size_bytes
    }
    return json.dumps(payload)


def send_messages(rate_per_second, duration_seconds=None):
    sent = 0
    start = time.time()

    while True:
        if duration_seconds and time.time() - start >= duration_seconds:
            break

        loop_start = time.time()

        for _ in range(rate_per_second):
            producer.produce(
                topic,
                key=str(uuid.uuid4()),
                value=create_message(),
                callback=delivery_report
            )
            sent += 1

        producer.poll(0)

        elapsed = time.time() - loop_start
        sleep_time = max(0, 1 - elapsed)
        time.sleep(sleep_time)

        print(f"Sent total: {sent}, current rate: {rate_per_second} msg/s")


if __name__ == "__main__":
    print("Starting producer")
    print(f"Topic: {topic}")
    print(f"Normal rate: {messages_per_second} msg/s")
    print(f"Burst rate: {burst_messages_per_second} msg/s")

    try:
        print("Normal load")
        send_messages(messages_per_second, duration_seconds=30)

        print("Burst load")
        send_messages(burst_messages_per_second, duration_seconds=burst_duration_seconds)

        print("Back to normal load")
        send_messages(messages_per_second)

    except KeyboardInterrupt:
        print("Stopping producer")

    finally:
        producer.flush()