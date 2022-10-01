from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from textblob import TextBlob
import time


def preprocessing(lines):
    words = lines.select(explode(split(lines.value, "t_end")).alias("word"))
    words = words.na.replace('', None)
    words = words.na.drop()
    words = words.withColumn('word', F.regexp_replace('word', r'http\S+', ''))
    words = words.withColumn('word', F.regexp_replace('word', '@\w+', ''))
    words = words.withColumn('word', F.regexp_replace('word', '#', ''))
    words = words.withColumn('word', F.regexp_replace('word', 'RT', ''))
    words = words.withColumn('word', F.regexp_replace('word', ':', ''))
    return words

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

    # Split the lines into words
    words = lines.select(
        explode(
            split(lines.value, " ")
        ).alias("word")
    )

    # Generate running word count
    wordCounts = words.groupBy("word").count()
    query = wordCounts \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    query.awaitTermination()


    # query=words.writeStream.format("console").start()
    # time.sleep(10)  # sleep 10 seconds
    # query.stop()



    #query.awaitTermination()

    # query = words.writeStream.queryName("all_tweets")\
    #     .format("console")\
    #     .outputMode("append").format("parquet")\
    #     .option("path", "./parc")\
    #     .option("checkpointLocation", "./check")\
    #     .trigger(processingTime='60 seconds').start()
    # query.awaitTermination()