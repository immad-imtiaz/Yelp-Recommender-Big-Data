from spark_base import SparkBase
from settings import CATEGORIES, CASSANDRA_KEY_SPACE, BUSINESS_CITY


class Yelp_Business_City(SparkBase):

    def __init__(self, *args, **kwargs):
        super(Yelp_Business_City, self).__init__(*args, **kwargs)
        self.input = self.INPUT_DIRECTORY + '/business.json'

    def get_business_city_state(self):
        all_business = self.sql_ctx.read.json(self.input)
        bus_city_state = all_business.select('business_id',
                                             'city',
                                             'latitude',
                                             'longitude',
                                             'name',
                                             'postal_code',
                                             'is_open',
                                             'state',
                                             'stars',
                                             'address',
                                             'review_count',
                                             'neighborhood')
        self.save_data_frame_to_cassandra(bus_city_state, BUSINESS_CITY)

yc = Yelp_Business_City()
yc.get_business_city_state()