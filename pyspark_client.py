# from time import time
from pyspark import SparkContext
import pyspark
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import *
from json import dumps
# from kafka import KafkaProducer
# SparkContext.addFile("")
from pyspark.sql.functions import lit

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
        df.withColumn("hashtag",lit("testval!!"))

        # df.show(truncate=False)
        
        #tumbling window of size 10 seconds
        windowedCounts = df.groupBy(
            window('timestamp',"10 seconds","10 seconds"),
            'hashtag'
        ).count()

        windowedCounts.show()

        push_df = windowedCounts.selectExpr("count as value").selectExpr("CAST(value as string)")
        push_df.show()

        push_df\
            .write\
                .format('kafka')\
                    .option('kafka.bootstrap.servers','localhost:9092')\
                        .option("topic","test")\
                        .save()

        #push to kafka 
        for row in windowedCounts.collect():
            topic,count = row.__getitem__("hashtag").__getitem__("text"),row.__getitem__("count")
            print(topic,count)

            #send the count to the topic
            # producer.send(topic,value=count)


if __name__ == "__main__":
    #spark session
    sc = SparkContext(appName="twitter stream")
    spark = SparkSession(sc)
    spark.sparkContext.setLogLevel('ERROR')

    # producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
    # value_serializer=lambda x: dumps(x).encode('utf-8'))

    #spark streaming context
    ssc = StreamingContext(sc,batchDuration=2)

    #dstream from streaming server
    dstream = ssc.socketTextStream("localhost",9090)

    #each rdd has multiple tweets
    dstream.foreachRDD(lambda rdd: handleRDD(rdd))

    ssc.start()
    ssc.awaitTermination()
    