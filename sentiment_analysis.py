from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from textblob import TextBlob
import time

def preprocessing(lines):
    #words = lines.select(explode(split(lines.value,'\n')).alias("word"))
    lines.withColumn("value", decode("value", 'UTF-8'))
    lines.withColumn("value", regexp_replace(col("value"), r'\n\r', " "))
    lines.withColumn("value", regexp_replace(col("value"), r"[\n\r]", " "))
    lines.withColumn("value", regexp_replace(col("value"), r"[\n\r]", " "))
    lines.withColumn("value", regexp_replace(col("value"), r"[\n\n]", " "))
    # words = lines.value.na.replace('', None)
    # words = words.na.drop()
    # words = words.withColumn('word', F.regexp_replace('word', r'http\S+', ''))
    # words = words.withColumn('word', F.regexp_replace('word', '@\w+', ''))
    # words = words.withColumn('word', F.regexp_replace('word', '#', ''))
    # words = words.withColumn('word', F.regexp_replace('word', 'RT', ''))
    # words = words.withColumn('word', F.regexp_replace('word', ':', ''))
    return lines

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
        .format("parquet") \
        .option("path", "output/filesink_output") \
        .option("checkpointLocation", "/tmp/destination/checkpointLocation")\
        .start()

    # query=words\
    #         .writeStream \
    #         .outputMode("append") \
    #         .format("parquet") \
    #         .option("checkpointLocation", "/tmp/destination/checkpointLocation")\
    #         .option("path", "twitter_sentiment_analysis/data")\
    #         .start()
    #time.sleep(100)
    query.awaitTermination(120)
    query.stop()