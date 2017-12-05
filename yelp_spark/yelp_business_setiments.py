from pyspark.sql.types import *
from spark_base import SparkBase
from nltk.corpus import stopwords
from pyspark.sql import functions as fn
from pyspark.ml.feature import CountVectorizer
import numpy as np
import sys, re, string
from settings import BUSINESS_CITY, CASSANDRA_KEY_SPACE, BUSINESS_SENTIMENTS, POSITIVE_WORDS, NEGATIVE_WORDS
from pyspark.sql import functions
from wordcloud import WordCloud

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
        tips_df = self.sql_ctx.read.json(self.INPUT_DIRECTORY + '/tip.json')
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

        sentimentsDF_pos = self.df_for(CASSANDRA_KEY_SPACE, POSITIVE_WORDS)
        sentimentsDF_neg = self.df_for(CASSANDRA_KEY_SPACE, NEGATIVE_WORDS)
        sentimentsDF = sentimentsDF_pos.unionAll(sentimentsDF_neg)
        businessTipDFexploded = businessTipDF.select('id', fn.explode('tip').alias('word'))
        businessTipExplodedSentiment = businessTipDFexploded.join(sentimentsDF, "word")

        businessTipSentimentCalculate = businessTipExplodedSentiment.groupBy('id') \
            .agg(fn.avg('sentiment').alias('avg_sentiment')) \
            .withColumn('predicted', fn.when(fn.col('avg_sentiment') > 0, 1.0).otherwise(0.))

        businessTipPosWordsExplode = businessTipDFexploded.join(sentimentsDF_pos, "word")
        businessTipPosWords = businessTipPosWordsExplode.groupBy('id').agg(
            fn.collect_list('word').alias('pos_words'))

        businessTipNegWordsExplode = businessTipDFexploded.join(sentimentsDF_neg, "word")
        businessTipNegWords = businessTipNegWordsExplode.groupBy('id').agg(
            fn.collect_list('word').alias('neg_words'))

        # joining with main table
        businessTipPredictedData = businessTipDF.join(businessTipSentimentCalculate, 'id', "left_outer")
        businessTipPredictedDataNeg = businessTipPredictedData.join(businessTipNegWords, 'id', "left_outer")
        businessTipPredictedDataNegPos = businessTipPredictedDataNeg.join(businessTipPosWords, 'id', "left_outer")
        businessSentimentToCassandra = businessTipPredictedDataNegPos.select('id', 'tip', 'pos_words', 'neg_words',
                                                                             'avg_sentiment') \
            .withColumnRenamed('id', 'business_id') \
            .withColumnRenamed('avg_sentiment', 'avg_sentiments')

        self.save_data_frame_to_cassandra(businessSentimentToCassandra, BUSINESS_SENTIMENTS)

    def give_pos_words(self, business_id_list):
        data_frame = self.df_for(CASSANDRA_KEY_SPACE, POSITIVE_WORDS)
        # number of business_id
        posWords = []
        y = data_frame.filter(data_frame.business_id == '').select('pos_words')
        listLen = len(business_id_list)
        for busid in business_id_list:
            x = data_frame.filter(data_frame.business_id == busid).select('pos_words')
            y = y.union(x)
        ylist = y.collect()
        for i in range(listLen):
            if isinstance(ylist[i]['pos_words'], list):
                posWords = posWords + ylist[i]['pos_words']
        return posWords
        # input1: business_id_list = ['--9e1ONYQuAa-CB_Rrw7Tw', 'fzQdcOOxJTEQVdM5G1WHzg', '7q3EukNc4COYCvgf3h-yeA']
        # input2: data_frame = dataframe that has the negative sentiment word list
        # returns: list of all the negative words for all of the businesses


    def give_neg_words(self, business_id_list):
        data_frame = self.df_for(CASSANDRA_KEY_SPACE, NEGATIVE_WORDS)
        negWords = []
        y = data_frame.filter(data_frame.business_id == '').select('neg_words')
        # number of business_id
        listLen = len(business_id_list)
        for busid in business_id_list:
            x = data_frame.filter(data_frame.business_id == busid).select('neg_words')
            y = y.union(x)
        ylist = y.collect()
        for i in range(listLen):
            if isinstance(ylist[i]['neg_words'], list):
                negWords = negWords + ylist[i]['neg_words']
        return negWords

    def save_sentiments_words(self, positive=True):
        schema = StructType([
            StructField('word', StringType(), True)
        ])
        if positive:
            words = self.spark.textFile(self.INPUT_DIRECTORY+'positive_words.txt')\
                .map(lambda x: {'word': str(x), 'sentiment': 1})
        else:
            words = self.spark.textFile(self.INPUT_DIRECTORY+'negative_words.txt') \
                .map(lambda x: {'word': str(x), 'sentiment': -1})

        words_df = self.sql_ctx.createDataFrame(words, schema)

        if positive:
            self.save_data_frame_to_cassandra(words_df, POSITIVE_WORDS)
        else:
            self.save_data_frame_to_cassandra(words_df, NEGATIVE_WORDS)

    def save_other_sentiment_words(self):
        schema = StructType([
           StructField('word', StringType(), True),
           StructField('sentiment', IntegerType())
        ])
        words_df = self.sql_ctx.read.schema(schema).option("header", "true").csv(self.INPUT_DIRECTORY+"sentiments.csv")
        positive_words_df = words_df.filter(words_df.sentiment == 1)
        negative_words_df = words_df.filter(words_df.sentiment == -1)
        self.save_data_frame_to_cassandra(positive_words_df, POSITIVE_WORDS)
        self.save_data_frame_to_cassandra(negative_words_df, NEGATIVE_WORDS)


ys = YelpBusinessSentiments()
ys.save_sentiments_words(positive=True)
ys.save_sentiments_words(positive=False)
ys.save_other_sentiment_words()
ys.save_business_sentiment()
