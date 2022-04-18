from kafka import KafkaConsumer
from pymongo import mongo_client

try:
    client = mongo_client.MongoClient('localhost',27017)
    db = client.tweet_warehouse
    print("Connection successful!")
except Exception as e:
    print("[Error]:",e)

#connect consumer to desired kafka topic
consumer = KafkaConsumer()
