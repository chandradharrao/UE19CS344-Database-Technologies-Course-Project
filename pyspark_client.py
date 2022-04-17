from pyspark import SparkContext
import pyspark
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import col, when, explode, arrays_zip, concat, lit

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
        df.show(truncate=False)



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
    dstream.foreachRDD(lambda rdd: handleRDD(rdd))


    ssc.start()
    ssc.awaitTermination()