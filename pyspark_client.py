from ensurepip import bootstrap
from pyspark import SparkContext
import pyspark
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import *
from kafka import KafkaProducer, producer
from json import dumps

'''
This is the pyspark client which connects to the twitter streaming server called as "twitter_Server.py" to collect tweets
'''

counter = {
    'RamVsYash':0, 'DelhiRiots':0, 'sundayvibes':0, 'jahagirpuri':0, '#HindusUnderAttackInIndia':0
}

#convert json rdd into dataframe 
def handleRDD(rdd:pyspark.RDD):
    if not rdd.isEmpty():
        df = spark.read.json(rdd,multiLine=True)
        # df.show(truncate=False)

        #convert str(timestamps) to timestamp object 
        df = df.withColumn('timestamp',\
            to_timestamp('input_timestamp'))

        # df.show(truncate=False)
        
        #tumbling window of size 10 seconds
        windowedCounts = df.groupBy(
            window('timestamp',"10 seconds","10 seconds"),
            'hashtag'
        ).count()

        windowedCounts.show()

        #push to kafka 
        for row in windowedCounts.collect():
            #0th col is the topic name
            topic = row[0]
            #1th col is the count of tweets in that topic
            count = row[1]

            #send to the topic the count
            producer.send(topic,count)


if __name__ == "__main__":
    #spark session
    sc = SparkContext(appName="twitter stream")
    spark = SparkSession(sc)
    spark.sparkContext.setLogLevel('ERROR')

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8'))

    #spark streaming context
    ssc = StreamingContext(sc,batchDuration=2)

    #dstream from streaming server
    dstream = ssc.socketTextStream("localhost",9090)

    #each rdd has multiple tweets
    dstream.foreachRDD(lambda rdd: handleRDD(rdd))


    ssc.start()
    ssc.awaitTermination()