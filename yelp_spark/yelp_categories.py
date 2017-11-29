from spark_base import SparkBase
from settings import CATEGORIES, CASSANDRA_KEY_SPACE, BUSINESS_CATEGORIES_TABLE

class Yelp_Categories(SparkBase):

    def __init__(self, *args, **kwargs):
        super(Yelp_Categories, self).__init__(*args, **kwargs)

    def transfer_all_categories(self):
        self.df_for(CASSANDRA_KEY_SPACE, BUSINESS_CATEGORIES_TABLE)
        all_categories = self.sql_ctx.sql('SELECT DISTINCT category FROM %s' % BUSINESS_CATEGORIES_TABLE)
        self.save_data_frame_to_cassandra(all_categories, CATEGORIES)

yc = Yelp_Categories()
yc.transfer_all_categories()

