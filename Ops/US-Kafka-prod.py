from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
import time
import pandas as pd
import os

import DDSClient as dds

def Kafka_setup():
    topic_name = 'my_topic'

symlist=list()
US_name_file = os.path.join("..", "Product_List", "US-symbols.csv")
df = pd.read_csv(US_name_file)
symlist = df.Symbol.tolist()
print(symlist)

bootstrap_servers = ['192.168.11.106:9094']
admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
exist_topic = admin_client.list_topics()
print('existed topics: ', exist_topic)

topic_list = []
for sym in symlist:
    if sym not in exist_topic:
        n_topic = NewTopic(name=sym, num_partitions=1, replication_factor=1)
        topic_list.append(n_topic)

print('new topic: ', topic_list)

if len(topic_list):
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

while True:
    for sym in symlist:
        message = dds.DDSServer.snapshot_str(sym)
        rec = dds.DDSServer.convertRecord(message)
        if rec["header"] != "error":
            producer.send(sym, message.encode('utf-8'))
            print(f"Message sent: {sym}")
        else:
            print(f"Error {sym}")
    time.sleep(120)
