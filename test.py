from pyspark import SparkContext
import pyspark
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import *
from json import dumps
from kafka import KafkaProducer

print("Hello")