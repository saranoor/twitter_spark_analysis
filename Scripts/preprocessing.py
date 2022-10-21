from datetime import datetime
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import isnan, when, count, col, to_date, udf, lower, regexp_replace, transform
from pyspark.sql.types import DateType
from pyspark.ml.feature import StopWordsRemover, Tokenizer, CountVectorizer, VectorAssembler, StringIndexer,IndexToString
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics

class ModelTraining():

    def __init__(self):
        self.spark = SparkSession.builder.master("local[1]") \
            .appName("SparkByExamples.com") \
            .getOrCreate()
        self.csv_PATH = "/home/saranoor/Data/spark_project/twitter_sentiment_analysis/Data/twitter_training.csv"
        self.model_path= '/home/saranoor/Data/spark_project/twitter_sentiment_analysis/Models/'

    def read_data(self, PATH, type):
         if type=='train':
             self.df=self.spark.read. \
                option('headers', True). \
                option('inferSchema', True). \
                csv(PATH)
         elif type=='test':
             schema = StructType(
                 [
                  StructField('id', IntegerType(), True),
                  StructField('dontknow', StringType(), True),
                  StructField('str_target', StringType(), True),
                  StructField('text', StringType(), True),
                  ]
             )
             self.df_test=self.spark.read. \
                option('headers', True). \
                schema(schema). \
                csv(PATH)

    def cleaning(self, type):
        if type=='train':
            # data = self.df.selectExpr("_c0 as target","id","date","flag","user","text")
            self.df = self.df.toDF("id", "dontknow", "str_target", "text")
            ### finding out outliers
            ### count of null and none values
            self.df.select([count(when(isnan(c) | col(c).isNull(), True)).alias(c) for c in self.df.columns]).show()
            self.df=self.df.na.drop()
            ### replace null values/missing values
            ###convert string to datetime
            # func = udf(lambda x: datetime.strptime(x, "%a %b %d %H:%M:%S PDT %Y"), DateType())
            # self.df = self.df.withColumn('date', func(col('date')))
            ### converting column text to lower case
            self.df = self.df.withColumn("text", lower(col("text")))
            # removing unicode characters form string
            self.df = self.df.withColumn("text", regexp_replace(col("text"), r'[^a-z0-9]', ' '))
            # removing extra spaces
            self.df = self.df.withColumn("text", regexp_replace(col("text"), r"\s\s+", ' '))
        else:
            # data = self.df.selectExpr("_c0 as target","id","date","flag","user","text")
            self.df_test= self.df_test.toDF("id", "dontknow", "str_target", "text")
            ### finding out outliers
            ### count of null and none values
            self.df_test.select([count(when(isnan(c) | col(c).isNull(), True)).alias(c) for c in self.df_test.columns]).show()
            self.df_test=self.df_test.na.drop()
            ### replace null values/missing values
            ###convert string to datetime
            # func = udf(lambda x: datetime.strptime(x, "%a %b %d %H:%M:%S PDT %Y"), DateType())
            # self.df = self.df.withColumn('date', func(col('date')))
            ### converting column text to lower case
            self.df_test = self.df_test.withColumn("text", lower(col("text")))
            # removing unicode characters form string
            self.df_test = self.df_test.withColumn("text", regexp_replace(col("text"), r'[^a-z0-9]', ' '))
            # removing extra spaces
            self.df_test = self.df_test.withColumn("text", regexp_replace(col("text"), r"\s\s+", ' '))

    def Pipeline(self):
        # convert target to int value
        stringIndexer = StringIndexer(inputCol="str_target", outputCol="target").fit(self.df)

        #tokenzing the data
        tokenizer = Tokenizer(inputCol="text", outputCol="text_tokenized")
        #self.df=tokenizer.transform(self.df)

        # removing stopwords
        remover=StopWordsRemover(inputCol="text_tokenized",outputCol="text_clean")
        # self.df=remover.transform(self.df)
        # self.df.show()
        # countvectorizer
        print("we are here")
        cv = CountVectorizer(inputCol="text_clean", outputCol="features")
        # cv_model=cv.fit(self.df)
        # self.df=cv_model.transform(self.df)

        #vectorAssembler
        va=VectorAssembler(inputCols=['features'],
                                  outputCol='attributes')

        #training model
        model=LogisticRegression(featuresCol='attributes', labelCol='target')

        #indextoString
        labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel",
                                       labels=stringIndexer.labels)#creating pipeline

        pipeline=Pipeline().setStages([stringIndexer, tokenizer, remover, cv, va, model, labelConverter])
        pipeline_model=pipeline.fit(self.df)
        self.df_transform=pipeline_model.transform(self.df)

        self.df_transform.select('attributes', 'target', 'prediction').show()
        #self.df_transform.show()

        lr_model=pipeline_model.stages[5]
        trainingSummary=lr_model.summary
        print("Accuracy of model is:" + str(trainingSummary.accuracy))

        evaluator = MulticlassClassificationEvaluator(labelCol="target",predictionCol="prediction")
        print("Train Accuracy " + str(evaluator.evaluate(self.df_transform, {evaluator.metricName: "accuracy"})))

        predictionAndLabels = self.df_transform.select("prediction","target").rdd
        multi_metrics = MulticlassMetrics(predictionAndLabels)
        precision_score = multi_metrics.weightedPrecision
        recall_score = multi_metrics.weightedRecall
        print('Precision and Recall are:',precision_score, recall_score)

        pipeline_model.write().overwrite().save(self.model_path+'pyspark-log-reg-model')

    def evaluation(self):
        self.read_data('/home/saranoor/Data/spark_project/twitter_sentiment_analysis/Data/twitter_validation.csv', 'test')
        load_model = PipelineModel.load(self.model_path+'pyspark-log-reg-model')
        self.cleaning('test')
        self.df_test_transform = load_model.transform(self.df_test)
        self.df_test_transform.select('prediction','target').show()
        evaluator = MulticlassClassificationEvaluator(labelCol="target",predictionCol="prediction")
        print("Test Accuracy " + str(evaluator.evaluate(self.df_test_transform, {evaluator.metricName: "accuracy"})))
        predictionAndLabels = self.df_test_transform.select("prediction","target").rdd
        multi_metrics = MulticlassMetrics(predictionAndLabels)
        precision_score = multi_metrics.weightedPrecision
        recall_score = multi_metrics.weightedRecall
        print('Precision and Recall are:',precision_score, recall_score)