from spark_base import SparkBase
from yelp_cassandra import YelpCassandra

class SparkBusiness(SparkBase):

    def __init__(self, *args, **kwargs):
        self.input = self.INPUT_DIRECTORY + '/business.json'

    def get_all_business_categories(self):

        def flatten_categories(passed_item):
            for i in range(0, len(passed_item[1])):
                yield passed_item[0], passed_item[1][i]

        all_business = self.spark.read.json(self.input)
        bus_cat_select = all_business.select('business_id', 'categories')
        bus_cat_rdd = bus_cat_select.rdd
        bus_cat_rdd = bus_cat_rdd.flatMap(flatten_categories)

        schema = self.types.StructType(
            [self.types.StructField('business_id', self.types.StringType(), False),
             self.types.StructField('categories', self.types.StringType(), False)])
        bus_cat_df = self.spark.createDataFrame(bus_cat_rdd, schema)
        return bus_cat_df



spark_business = SparkBusiness()
spark_business.get_utility_matrix()