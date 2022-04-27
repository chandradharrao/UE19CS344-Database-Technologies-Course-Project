# from time import time
from asyncore import read
import json
from pyspark import Row, SparkContext
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

topics = ['RCBvsRR', 'EscaypeLiveTrailer', 'TheGrayMan', 'PrashantKishor', 'Karachi']
counts = {}

def saveRDD(rdd:pyspark.RDD):
    df = rdd.toDF()
    df = df.filter(col('_1').isin(topics))
    df.show(truncate=False)
    for row in df.collect():
        print(row)
        df_ = spark.createDataFrame([Row(topic=row['_1'],value=row['_2'])]).selectExpr("CAST(value as string)")
        df_.show(truncate=False)
        df_.write\
            .format('kafka')\
                .option('kafka.bootstrap.servers','localhost:9092')\
                    .option("topic",row['_1'])\
                        .save()

if __name__ == "__main__":
    #spark session
    sc = SparkContext(appName="twitter stream")
    spark = SparkSession(sc)
    spark.sparkContext.setLogLevel('ERROR')

    #spark streaming context
    ssc = StreamingContext(sc,batchDuration=2)

    #dstream from streaming server
    dstream = ssc.socketTextStream("localhost",9090)

    #each rdd has multiple tweets
    # dstream.foreachRDD(lambda rdd: handleRDD(rdd))
    dstream.window(10,10).map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y).foreachRDD(lambda rdd: saveRDD(rdd))

    ssc.start()
    ssc.awaitTermination()
    