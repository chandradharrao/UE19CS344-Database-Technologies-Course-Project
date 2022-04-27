from confluent_kafka import Consumer
from pymongo import mongo_client

# topics = ['RCBvsRR', 'EscaypeLiveTrailer', 'TheGrayMan', 'PrashantKishor', 'Karachi']
topics = ['TheGrayMan']

try:
    client = mongo_client.MongoClient('localhost',27017)
    db = client['test']
    collection = db['test']
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
        if msg!=None:
            print("message",msg.topic(),int(msg.value().decode('utf-8')))
            db_rec = {'topic':msg.topic(),'count':int(msg.value().decode('utf-8'))}
            rec_id = collection.insert_one(db_rec)
            print(f"Data inserted into mongodb with id<{rec_id}>")
        else:
            print("EMpty message from kafka...")
    except Exception as e:
        print("[ERROR]",e)


