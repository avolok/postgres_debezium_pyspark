from datetime import datetime
import random
from time import sleep
from confluent_kafka import Producer
import socket
from confluent_kafka.admin import AdminClient


KAFKA_TIMEOUT = 60000

def main():
    conf = {
        'bootstrap.servers': 'kafka1:9092',
        'client.id': socket.gethostname(),
    }    

    try:
        admin_client = AdminClient(conf)
        #topics = admin_client.l # list_topics(timeout=KAFKA_TIMEOUT)
        
        admin_client.describe_cluster()
        
    except Exception as e:
        print("Seems Kafka is not ready yet. Please wait a few seconds and try again.")
        print(e)
        exit(1)
        
        
    producer = Producer(conf)
    

    for _ in range(1000):
        value_str = random.choice(['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'])
        
        value_row = datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "," + value_str
        
        try: 
            producer.produce("topic1",  value=value_row)
            print("--> " + value_row)
            producer.flush()
            sleep(3)
        except Exception as e:
            print(e)
            pass
        
if __name__ == '__main__':
    main()