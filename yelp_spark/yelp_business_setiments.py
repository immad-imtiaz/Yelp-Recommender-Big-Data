from pyspark.sql.types import *
from spark_base import SparkBase
from nltk.corpus import stopwords
from pyspark.sql import functions as fn
from pyspark.ml.feature import CountVectorizer
import numpy as np
import sys, re, string
from settings import BUSINESS_CITY, CASSANDRA_KEY_SPACE, BUSINESS_SENTIMENTS
from pyspark.sql import functions

engStopWords = stopwords.words('english')

class YelpBusinessSentiments(SparkBase):

    def __init__(self, *args, **kwargs):
        super(YelpBusinessSentiments, self).__init__(*args, **kwargs)
        self.parquet_file = self.INPUT_DIRECTORY + 'sentiments.parquet/*.gz.parquet'

    @staticmethod
    def map_tip_rdd(x):
        def text_process(text):
            nopunc = [char for char in text if char not in string.punctuation]
            nopunc = ''.join(nopunc)
            return [word for word in nopunc.split() if word.lower() not in engStopWords]

        return {
            "id": x['business_id'],
            "user_id": x['user_id'],
            "date": x['date'],
            "tip": text_process(x['text'])
        }

    @staticmethod
    def addTipWords(a, b):
        # check if tip is empty or not
        if (isinstance(a, list) and isinstance(b, list)):
            tip = a + b
        elif isinstance(a, list):
            tip = a
        elif isinstance(b, list):
            tip = b
        else:
            tip = []
        return tip

    @staticmethod
    def mapBusinessTipToDic(x):
        if (isinstance(x[1], list)):
            tipVal = x[1]
        else:
            tipVal = []
        returnVal = {
            "id": x[0],
            "tip": tipVal
        }
        return returnVal

    def save_business_sentiment(self):

        business_df = self.df_for(CASSANDRA_KEY_SPACE, BUSINESS_CITY)
        tips_df = self.sql_ctx.read.json(self.INPUT_DIRECTORY+'/tip.json')
        tip_RDD = tips_df.rdd
        loadTipRDD = tip_RDD.map(self.map_tip_rdd)
        tips_now = loadTipRDD.toDF()
        # make a left outer join
        business_joined = business_df.join(tips_now, business_df.business_id == tips_now.id, "left_outer")

        business_joined_RDD = business_joined.rdd
        businessjoinedKeyVal = business_joined_RDD.map(lambda n: (n["business_id"], n["tip"]))

        businessTipReducedRDD = businessjoinedKeyVal.reduceByKey(self.addTipWords)
        businessTipRDD = businessTipReducedRDD.map(self.mapBusinessTipToDic)
        businessTipDF = self.sql_ctx.createDataFrame(businessTipRDD)

        sentimentsDF = self.sql_ctx.read.parquet(self.parquet_file)

        businessTipDFexploded = businessTipDF.select('id', fn.explode('tip').alias('word'))
        businessTipExplodedSentiment = businessTipDFexploded.join(sentimentsDF, "word")

        businessTipSentimentCalculate = businessTipExplodedSentiment.groupBy('id') \
            .agg(fn.avg('sentiment').alias('avg_sentiment')) \
            .withColumn('predicted', fn.when(fn.col('avg_sentiment') > 0, 1.0).otherwise(0.))

        # joining with main table
        businessTipPredictedData = businessTipDF.join(businessTipSentimentCalculate, 'id', "left_outer")
        businessSentimentToCassandra = businessTipPredictedData.select('id', 'tip', 'avg_sentiment') \
            .withColumnRenamed('id', 'business_id') \
            .withColumnRenamed('avg_sentiment', 'avg_sentiments')

        self.save_data_frame_to_cassandra(businessSentimentToCassandra, BUSINESS_SENTIMENTS)

ys = YelpBusinessSentiments()
ys.save_business_sentiment()
