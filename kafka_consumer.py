import json
from kafka import KafkaConsumer
from pymongo import mongo_client
from json import loads

topics = ['RamVsYash', 'DelhiRiots', 'sundayvibes', 'jahagirpuri', '#HindusUnderAttackInIndia']

try:
    client = mongo_client.MongoClient('localhost',27017)
    db = client['tweet_warehouse']
    collection = db['topicwise_data']
    print("Connection successful!")

    #connect consumer to desired kafka topic
    consumer = KafkaConsumer(
        topics=topics,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='tweets_consumer',
        value_deserializer=lambda x:loads(x.decode("utf-8"))
    )

    for message in consumer:
        record = json.loads(message.value)

        db_rec = {'topic':record['topic'],'count':record['count']}
        rec_id = collection.insert_one(db_rec)
        print(f"Data inserted into mongodb with id<{rec_id}>")
        
except Exception as e:
    print("[Error]:",e)
