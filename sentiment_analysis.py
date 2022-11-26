from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from textblob import TextBlob
import time

def preprocessing(lines):
    #words = lines.select(explode(split(lines.value,'\n')).alias("word"))
    df=lines.withColumn("value", decode("value", 'UTF-8'))
    df=df.withColumn("value", regexp_replace(col("value"), r'\n\r', " "))
    df=df.withColumn("value", regexp_replace(col("value"), r"[\n\r]", " "))
    df=df.withColumn("value", regexp_replace(col("value"), r"[\n\r]", " "))
    df=df.withColumn("value", regexp_replace(col("value"), r"[\n\n]", " "))
    df = df.withColumn('value', F.regexp_replace('value', r'http\S+', ''))
    df=df.withColumn('value', F.regexp_replace('value', '@\w+', ''))
    df=df.withColumn('value', F.regexp_replace('value', '#', ''))
    df=df.withColumn('value', F.regexp_replace('value', 'RT', ''))
    df=df.withColumn('value', F.regexp_replace('value', ':', ''))
    df = df.withColumn('value', F.regexp_replace('value', '\"', ''))
    return df

# text classification
def polarity_detection(text):
    return TextBlob(text).sentiment.polarity

def subjectivity_detection(text):
    return TextBlob(text).sentiment.subjectivity

def text_classification(words):
    # polarity detection
    polarity_detection_udf = udf(polarity_detection, StringType())
    words = words.withColumn("polarity", polarity_detection_udf("word"))
    # subjectivity detection
    subjectivity_detection_udf = udf(subjectivity_detection, StringType())
    words = words.withColumn("subjectivity", subjectivity_detection_udf("word"))
    return words

if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import explode
    from pyspark.sql.functions import split

    spark = SparkSession \
        .builder \
        .appName("StructuredNetworkWordCount") \
        .getOrCreate()
    # Create DataFrame representing the stream of input lines from connection to localhost:9999
    lines = spark \
        .readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 9999) \
        .load()
    words=preprocessing(lines)
    # query =words\
    #     .writeStream \
    #     .outputMode("update") \
    #     .trigger(processingTime='0 seconds')\
    #     .format("console") \
    #     .start()

    query =words\
        .writeStream \
        .outputMode("append") \
        .trigger(processingTime='50 seconds') \
        .format("csv") \
        .option("path", "output/filesink") \
        .option("checkpointLocation", "/tmp/destination/checkpoint")\
        .start()

    #time.sleep(100)
    query.awaitTermination(60)
    query.stop()