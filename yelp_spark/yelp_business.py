from pyspark.sql.types import *
from spark_base import SparkBase
from settings import BUSINESS_CATEGORIES_TABLE


class YelpBusiness(SparkBase):

    def __init__(self, *args, **kwargs):
        super(YelpBusiness, self).__init__(*args, **kwargs)
        self.input = self.INPUT_DIRECTORY + '/business.json'

    def get_all_business_categories(self):
        def flatten_categories(passed_item):
            for i in range(0, len(passed_item[1])):
                yield passed_item[0], passed_item[1][i]

        all_business = self.sql_ctx.read.json(self.input)
        bus_cat_select = all_business.select('business_id', 'categories')
        bus_cat_rdd = bus_cat_select.rdd
        bus_cat_rdd = bus_cat_rdd.flatMap(flatten_categories)

        schema = self.types.StructType(
            [self.types.StructField('business_id', self.types.StringType(), False),
             self.types.StructField('category', self.types.StringType(), False)])
        bus_cat_df = self.sql_ctx.createDataFrame(bus_cat_rdd, schema)
        return bus_cat_df


def populate_yelp_business():
    spark_business = YelpBusiness()
    business_categories = spark_business.get_all_business_categories()
    spark_business.save_data_frame_to_cassandra(business_categories, BUSINESS_CATEGORIES_TABLE)

populate_yelp_business()
