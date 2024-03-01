import json
from confluent_kafka import Consumer


def main():
    conf = {
        "bootstrap.servers": "kafka1:9092",
        "group.id": "tiny-app-consumer",
    }

    print("Waiting for Kafka to start")

    consumer = Consumer(conf)

    consumer.subscribe(["debezium.public.cdctable"])

    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.value() is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        json_string = msg.value().decode("utf-8")
        
        json_object = json.loads(json_string)

        json_formatted_str = json.dumps(json_object, indent=2)
        
        print("<-- {}".format(json_formatted_str))


if __name__ == "__main__":
    main()
