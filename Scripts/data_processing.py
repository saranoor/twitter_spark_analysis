from pyspark.sql.functions import decode, regexp_replace, col, split
from pyspark.sql import functions as F
class DataProcessing():
    def preprocessing(self, lines):
        # words = lines.select(explode(split(lines.value,'\n')).alias("word"))
        df = lines.withColumn("value", decode("value", 'UTF-8'))
        df = df.withColumn("value", regexp_replace(col("value"), r'\n\r', " "))
        df = df.withColumn("value", regexp_replace(col("value"), r"[\n\r]", " "))
        df = df.withColumn("value", regexp_replace(col("value"), r"[\n\r]", " "))
        df = df.withColumn("value", regexp_replace(col("value"), r"[\n\n]", " "))
        df = df.withColumn('value', F.regexp_replace('value', r'http\S+', ''))
        df = df.withColumn('value', F.regexp_replace('value', '@\w+', ''))
        df = df.withColumn('value', F.regexp_replace('value', '#', ''))
        df = df.withColumn('value', F.regexp_replace('value', 'RT', ''))
        df = df.withColumn('value', F.regexp_replace('value', ':', ''))
        df = df.withColumn('value', F.regexp_replace('value', '\"', ''))
        df=df.withColumn("value", F.regexp_replace('value',r'[^A-Za-z0-9,]+',' '))
        df=df.withColumn('id',split(df.value, ',', 2).getItem(0))\
                  .withColumn('text', split(df.value, ',', 2).getItem(1))
        # #df_mod=df_mod.withColumn("clean_text", F.regexp_replace('text','[^a-zA-Z]+',' '))


        return df