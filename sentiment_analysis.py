from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from textblob import TextBlob
import time
import re
from Scripts.data_processing import DataProcessing

if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import explode
    from pyspark.sql.functions import split

    spark = SparkSession \
        .builder \
        .appName("StructuredNetworkWordCount") \
        .getOrCreate()

    lines = spark \
        .readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 9999) \
        .load()
    words=DataProcessing().preprocessing(lines)

    query =words\
        .writeStream \
        .outputMode("append") \
        .trigger(processingTime='30 seconds') \
        .format("csv") \
        .option("header", "true") \
        .option("quoteALL", "true")\
        .option("path", "output/filesink_4") \
        .option("checkpointLocation", "/tmp/destination/checkpoint_4")\
        .start()

    query.awaitTermination(30)
    query.stop()