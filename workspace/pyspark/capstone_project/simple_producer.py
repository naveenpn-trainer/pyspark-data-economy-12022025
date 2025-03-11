from confluent_kafka import Producer

# Kafka Broker COnfiguration
configuration = {
    "bootstrap.servers":"localhost:9092"
}

# Create instance of Producer class
producer = Producer(configuration)
topic = "my-demo-topic"
while True:
    message = input("Enter message")
    producer.produce(topic=topic,value=message)
    producer.flush()
    print("Message sent")