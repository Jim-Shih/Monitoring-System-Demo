from confluent_kafka import Consumer, KafkaException

# Define the Kafka consumer with the broker address and group id
c = Consumer(
    {
        "bootstrap.servers": "localhost:9093,localhost:9095",
        "group.id": "mygroup",
        "auto.offset.reset": "earliest",
    }
)

# Subscribe to the 'mytopic' topic
topic_list = [f"topic_{i}" for i in range(3)]
c.subscribe(topic_list)

try:
    while True:
        msg = c.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            # Proper message
            key = msg.key()
            value = msg.value()
            print(f"Received message: {value}")

except KeyboardInterrupt:
    pass
finally:
    # Close down consumer to commit final offsets.
    c.close()
