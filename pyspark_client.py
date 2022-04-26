# from time import time
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

        # try:
        #     temp = windowedCounts.groupBy())
        #     temp.sh

        # except Exception as e:
        #     print(e)

        windowedCounts.show(truncate=False)

        #push the count to the correct topic
        #filter out the df for specific topic
        # cut_df = windowedCounts.filter(windowedCounts.hashtag.__getitem__("text").isin(topics))

        '''
        'GroupedData' object has no attribute 'show'

        Groupby the window for each hashtag <hastag,count>
        create 1 df for each hashtag with counts
        write that df to the desired topic

        +-------+--------------------------------+--------------------------+---------------+
        |content|hashtag                         |input_timestamp           |screenName     |
        +-------+--------------------------------+--------------------------+---------------+
        |testing|{[76, 97], ECP_Disqualify_Lotas}|2022-04-26 21:49:31.207065|HMariAdeel     |
        |testing|{[97, 108], TheGrayMan}         |2022-04-26 21:49:31.412271|faroolabdullah |
        |testing|{[56, 67], TheGrayMan}          |2022-04-26 21:49:31.413885|Arunachalamkd  |
        |testing|{[32, 43], TheGrayMan}          |2022-04-26 21:49:31.415642|andreamallorca2|
        |testing|{[100, 113], NaaneVaruven}      |2022-04-26 21:49:31.615105|Karthik26910555|
        |testing|{[44, 55], Rawalpindi}          |2022-04-26 21:49:31.616586|Mazari_6996    |
        +-------+--------------------------------+--------------------------+---------------+

        +-------+--------------------------------+--------------------------+---------------+--------------------------+
        |content|hashtag                         |input_timestamp           |screenName     |timestamp                 |
        +-------+--------------------------------+--------------------------+---------------+--------------------------+
        |testing|{[76, 97], ECP_Disqualify_Lotas}|2022-04-26 21:49:31.207065|HMariAdeel     |2022-04-26 21:49:31.207065|
        |testing|{[97, 108], TheGrayMan}         |2022-04-26 21:49:31.412271|faroolabdullah |2022-04-26 21:49:31.412271|
        |testing|{[56, 67], TheGrayMan}          |2022-04-26 21:49:31.413885|Arunachalamkd  |2022-04-26 21:49:31.413885|
        |testing|{[32, 43], TheGrayMan}          |2022-04-26 21:49:31.415642|andreamallorca2|2022-04-26 21:49:31.415642|
        |testing|{[100, 113], NaaneVaruven}      |2022-04-26 21:49:31.615105|Karthik26910555|2022-04-26 21:49:31.615105|
        |testing|{[44, 55], Rawalpindi}          |2022-04-26 21:49:31.616586|Mazari_6996    |2022-04-26 21:49:31.616586|
        +-------+--------------------------------+--------------------------+---------------+--------------------------+

        +------------------------------------------+--------------------------------+-----+
        |window                                    |hashtag                         |count|
        +------------------------------------------+--------------------------------+-----+
        |{2022-04-26 21:49:30, 2022-04-26 21:49:40}|{[76, 97], ECP_Disqualify_Lotas}|1    |
        |{2022-04-26 21:49:30, 2022-04-26 21:49:40}|{[44, 55], Rawalpindi}          |1    |
        |{2022-04-26 21:49:30, 2022-04-26 21:49:40}|{[100, 113], NaaneVaruven}      |1    |
        |{2022-04-26 21:49:30, 2022-04-26 21:49:40}|{[56, 67], TheGrayMan}          |1    |
        |{2022-04-26 21:49:30, 2022-04-26 21:49:40}|{[32, 43], TheGrayMan}          |1    |
        |{2022-04-26 21:49:30, 2022-04-26 21:49:40}|{[97, 108], TheGrayMan}         |1    |
        +------------------------------------------+--------------------------------+-----+

        '''

        for row in windowedCounts.collect():
            print(row.__getitem__("hashtag").__getitem__("text"))
            topic_name = row.__getitem__("hashtag").__getitem__("text")

            if topic_name in counts:
                counts[topic_name]+=1
            else:
                counts[topic_name]=1

        print("Counts....")
        print(counts)

        for ke,value in counts.items():
            if ke in topics:
                df = spark.createDataFrame([Row(topic=ke,value=str(value))]).selectExpr("CAST(value as string)")
                print("The dataframe")
                df.show()
                
                print("saving to topic:",ke)
                df.write\
                    .format('kafka')\
                        .option('kafka.bootstrap.servers','localhost:9092')\
                            .option("topic",ke)\
                                .save()


        # cut_df.show(truncate=False)

        #retrieve count column
        # push_df = cut_df.selectExpr("count as value").selectExpr("CAST(value as string)")
        # push_df.show()

        # cut_df\
            # .write\
            #     .format('kafka')\
            #         .option('kafka.bootstrap.servers','localhost:9092')\
            #             .option("topic",topic)\
            #                 .save()

            

        # push_df\
        #     .write\
        #         .format('kafka')\
        #             .option('kafka.bootstrap.servers','localhost:9092')\
        #                 .option("topic","test")\
        #                     .save()


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
    