from datetime import datetime
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import isnan, when, count, col, to_date, udf, lower, regexp_replace, transform
from pyspark.sql.types import DateType
from pyspark.ml.feature import StopWordsRemover, Tokenizer, CountVectorizer, VectorAssembler, StringIndexer,IndexToString
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics

class ModelTraining():

    def __init__(self):
        pass

    def training(self):
        spark = SparkSession.builder.master("local[1]") \
            .appName("SparkByExamples.com") \
            .getOrCreate()
        csv_PATH = "/home/saranoor/Data/spark_project/twitter_sentiment_analysis/Data/twitter_training.csv"
        df = spark.read. \
            option('headers', True). \
            option('inferSchema', True). \
            csv(csv_PATH)

        df.printSchema()
        df.show()

        # data = df.selectExpr("_c0 as target","id","date","flag","user","text")
        df = df.toDF("id", "dontknow", "str_target", "text")

        # convert target to int value
        stringIndexer = StringIndexer(inputCol="str_target", outputCol="target").fit(df)

        ### finding out outliers

        ### count of null and none values
        df.select([count(when(isnan(c) | col(c).isNull(), True)).alias(c) for c in df.columns]).show()
        df=df.na.drop()
        ### replace null values/missing values

        ###convert string to datetime
        # func = udf(lambda x: datetime.strptime(x, "%a %b %d %H:%M:%S PDT %Y"), DateType())
        # df = df.withColumn('date', func(col('date')))

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
        # df=remover.transform(df)
        # df.show()
        # countvectorizer
        print("we are here")
        cv = CountVectorizer(inputCol="text_clean", outputCol="features")
        # cv_model=cv.fit(df)
        # df=cv_model.transform(df)

        #vectorAssembler
        va=VectorAssembler(inputCols=['features'],
                                  outputCol='attributes')

        #training model
        model=LogisticRegression(featuresCol='attributes', labelCol='target')

        #indextoString
        labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel",
                                       labels=stringIndexer.labels)#creating pipeline
        pipeline=Pipeline().setStages([stringIndexer, tokenizer, remover, cv, va, model, labelConverter])
        pipeline_model=pipeline.fit(df)
        df_transform=pipeline_model.transform(df)
        df_transform.select('attributes', 'target', 'prediction').show()
        df_transform.show()
        lr_model=pipeline_model.stages[5]
        trainingSummary=lr_model.summary
        #trainingSummary.roc.show(5)
        print("accuracy: " + str(trainingSummary.accuracy))
        evaluator = BinaryClassificationEvaluator(labelCol="target",rawPredictionCol="prediction")
        print("Test Area Under ROC: " + str(evaluator.evaluate(df_transform, {evaluator.metricName: "areaUnderROC"})))
        predictionAndLabels = df_transform.select("prediction","target").rdd
        # Instantiate metrics objects
        multi_metrics = MulticlassMetrics(predictionAndLabels)
        precision_score = multi_metrics.weightedPrecision
        recall_score = multi_metrics.weightedRecall
        print(precision_score, recall_score)
