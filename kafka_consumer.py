from confluent_kafka import Consumer
from pymongo import mongo_client
import pymongo
from bson.objectid import ObjectId

topics = ['RCBvsRR', 'EscaypeLiveTrailer', 'TheGrayMan', 'PrashantKishor', 'Karachi']
# topics = ['TheGrayMan']

try:
    client = mongo_client.MongoClient('localhost',27017)
    db = client['test_1']
    collection = db['test_1']
    print("Connection successful!")
        
except Exception as e:
    print("[Error1]:",e)

#connect consumer to desired kafka topic
conf = {'bootstrap.servers': "localhost:9092",
        'group.id': "tweet_consumer",
        'enable.auto.commit':True,
        'auto.offset.reset': 'earliest'}

consumer = Consumer(conf)
consumer.subscribe(topics)

while True:
    msg = consumer.poll(timeout=1.0)
    try:
        if msg!=None and msg.value()!=None:
            print("------------------------------------------------------------")
            print("message",msg.topic(),int(msg.value().decode('utf-8')))

            db_rec = {'topic':msg.topic(),'count':int(msg.value().decode('utf-8'))}

            rec_id = collection.insert_one(db_rec)

            print(f"Data inserted into mongodb with id<{rec_id}>")

            print(collection.find_one({'_id':ObjectId(str(rec_id.inserted_id))}))
            print("------------------------------------------------------------")
            print()
        else:
            print("Empty message from kafka...")
    except Exception as e:
        print("[ERROR]",e)


