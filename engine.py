import re
from pyspark.mllib.recommendation import ALS
from yelp_spark.spark_base import SparkBase
from yelp_spark.settings import *

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class YelpRecommenderEngine(SparkBase):

    def __init__(self, *args, **kwargs):
        super(YelpRecommenderEngine, self).__init__(*args, **kwargs)

    def get_business_categories(self, cat):
        categories = self.df_for(CASSANDRA_KEY_SPACE, CATEGORIES)
        return categories.filter(categories.category.contains(cat) | categories.category.contains(cat.title()))

    def get_business_cities(self):
        self.df_for(CASSANDRA_KEY_SPACE, BUSINESS_CITY)
        return self.sql_ctx.sql('SELECT DISTINCT city, state from %s ORDER BY city' % BUSINESS_CITY)








