from pyspark import SparkContext
from pyspark.streaming import StreamingContext

'''
This is the pyspark client which connects to the twitter streaming server called as "twitter_Server.py" to collect tweets
'''

if __name__ == "__main__":
    sc = SparkContext("local[2]","twitter_analysis")
    ssc = StreamingContext(sc,1)

    #read the tweet from the server
    lines = ssc.socketTextStream(
        "localhost",
        9090
    )

    lines.pprint()

    ssc.start()
    ssc.awaitTermination()