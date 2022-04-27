from pyspark import SparkContext
import pyspark
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import *
from pyspark.sql.functions import lit

'''
This is the pyspark client which connects to the twitter streaming server called as "twitter_Server.py" to collect tweets
'''

topics = ['RCBvsRR', 'EscaypeLiveTrailer', 'TheGrayMan', 'PrashantKishor', 'Karachi']

def saveRDD(rdd:pyspark.RDD):
    df = rdd.toDF()
    df = df.filter(col('_1').isin(topics))
    df.show(truncate=False)

    for topic in topics:
        try:
            df_ = df.filter(df['_1']==topic).selectExpr("_2 as value").selectExpr("cast(value as string) value")
            if df.count()>0:
                print("Processing ",topic)
                df_.show(truncate=False)
                df_.write\
                    .format('kafka')\
                        .option('kafka.bootstrap.servers','localhost:9092')\
                            .option("topic",topic)\
                                .save()
        except Exception as e:
            print("[ERROR]:",e)

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
    dstream.window(60,60).map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y).foreachRDD(lambda rdd: saveRDD(rdd))

    ssc.start()
    ssc.awaitTermination()
    