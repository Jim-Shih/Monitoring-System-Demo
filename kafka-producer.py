from confluent_kafka import Producer, KafkaException
import random
import string
import time

# Define the Kafka producer with the broker address
producer = Producer({"bootstrap.servers": "localhost:9093,localhost:9095"})


# Define a callback function to handle delivery reports
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        partition = msg.partition()
        offset = msg.offset()
        topic = msg.topic()
        print(
            f"Message delivered to topic: {topic}, partition: {partition}, offset: {offset}"
        )


def health_check(producer):
    try:
        producer.poll(0)
    except Exception as e:
        print(f"Exception: {e}")
        return False
    return True


def generate_user_activity():
    user_id = random.randint(1, 100)
    activity = random.choice(['page_view', 'click', 'purchase', 'sign_up', 'log_in'])
    timestamp = int(time.time())
    return f"{user_id},{activity},{timestamp}"


# Define the topic to which the data will be sent
topic_list = [f"topic_{i}" for i in range(3)]
while True:
    try:
        # Generate a random key and value

        # Send the message to the Kafka broker
        if health_check(producer):
            from confluent_kafka.serialization import StringSerializer
            serializer = StringSerializer('utf_8')
            message = serializer(generate_user_activity())
            topic = random.choice(topic_list)
            producer.produce(topic, key="key", value=message, callback=delivery_report)
            producer.poll()
        producer.flush()

    except KafkaException as e:
        print(f"Exception: {e}")
        break
