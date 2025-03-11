from confluent_kafka import Consumer

if __name__ == '__main__':
    configuration = {
         "bootstrap.servers":"localhost:9092",
        "group.id":"group-one-two"
    }

    consumer = Consumer(configuration)
    consumer.subscribe(topics=["my-demo-topic"])
    while True:
        messages = consumer.poll(1.0)
        print(messages)
        if messages is None:
            continue
        else:
            print(f"Received message: {messages.value()} ")