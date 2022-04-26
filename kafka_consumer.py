import json
from pydoc_data.topics import topics
from kafka import KafkaConsumer
from pymongo import mongo_client
from json import loads

topics = ['RCBvsRR', 'EscaypeLiveTrailer', 'TheGrayMan', 'PrashantKishor', 'Karachi']

try:
    client = mongo_client.MongoClient('localhost',27017)
    db = client['test']
    collection = db['test']
    print("Connection successful!")
        
except Exception as e:
    print("[Error1]:",e)

try:
    #connect consumer to desired kafka topic
    consumer = KafkaConsumer(
        topics,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='tweets_consumer',
        value_deserializer=lambda x:loads(x.decode("utf-8"))
    )

except Exception as e:
    print('[ERROR2]:',e)

# try:
#     for record in consumer:

#         db_rec = {'topic':'test','count':record}
#         rec_id = collection.insert_one(db_rec)
#         print(f"Data inserted into mongodb with id<{rec_id}>")
# except Exception as e:
#     print('ERROR3',e)

while True:
    msg = consumer.poll(1.0)

    db_rec = {'topic':msg.topic(),'count':}
    rec_id = collection.insert_one(db_rec)
