from datetime import datetime
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import isnan, when, count, col, to_date, udf, lower, regexp_replace, transform
from pyspark.sql.types import DateType
from pyspark.ml.feature import StopWordsRemover, Tokenizer, CountVectorizer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression

spark = SparkSession.builder.master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()
csv_PATH = "/home/saranoor/Data/spark_project/twitter_sentiment_analysis/Data/training.1600000.processed.noemoticon.csv"
df = spark.read. \
    option('headers', True). \
    option('inferSchema', True). \
    csv(csv_PATH)

df.printSchema()
# data = df.selectExpr("_c0 as target","id","date","flag","user","text")
df = df.toDF("target", "id", "date", "flag", "user", "text")

### finding out outliers

### count of null and none values
df.select([count(when(isnan(c) | col(c).isNull(), True)).alias(c) for c in df.columns]).show()

### replace null values/missing values

###convert string to datetime
func = udf(lambda x: datetime.strptime(x, "%a %b %d %H:%M:%S PDT %Y"), DateType())
df = df.withColumn('date', func(col('date')))

### converting column text to lower case
df = df.withColumn("text", lower(col("text")))

# removing unicode characters form string
df = df.withColumn("text", regexp_replace(col("text"), r'[^a-z0-9]', ' '))

# removing extra spaces
df = df.withColumn("text", regexp_replace(col("text"), r"\s\s+", ' '))

#tokenzing the data
tokenizer = Tokenizer(inputCol="text", outputCol="text_tokenized")
#df=tokenizer.transform(df)

# removing stopwords
remover=StopWordsRemover(inputCol="text_tokenized",outputCol="text_clean")
#df=remover.transform(df)

# countvectorizer
cv = CountVectorizer(inputCol="text_clean", outputCol="features")

# vectorAssembler
va=VectorAssembler(inputCols=['features'],
                          outputCol='attributes')

# training model
model=LogisticRegression(featuresCol='attributes', labelCol='target')

#creating pipeline
pipeline=Pipeline().setStages([tokenizer, remover, cv, va,model])
pipeline_model=pipeline.fit(df)
df_transform=pipeline_model.transform(df)
df_tranform.select('attributes', 'target', 'rawPrediction').show()
df_transform.show()

#df.printSchema()

