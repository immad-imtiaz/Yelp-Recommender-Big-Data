import sys
from pyspark import SparkConf
from pyspark.sql import functions, types, Row, SQLContext
from pyspark.sql.functions import split
from pyspark.sql import SparkSession, types

class SparkBase(object):

    INPUT_DIRECTORY = ''

    def __init__(self):
        self.conf = SparkConf().setAppName('Yelp Recommender')
        self.conf.setMaster('local[*]')
        self.spark = SparkSession.builder.appName('Shortest Path').getOrCreate()
        self.spark_ctx = self.spark.sparkContext
        self.types = types
