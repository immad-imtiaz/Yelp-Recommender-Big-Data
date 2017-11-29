from settings import YELP_USERS
from spark_base import SparkBase


class YelpUsers(SparkBase):
    def __init__(self, *args, **kwargs):
        super(YelpUsers, self).__init__(*args, **kwargs)
        self.input = self.INPUT_DIRECTORY + '/user.json'

    def save_user_data(self):
        all_users = self.sql_ctx.read.json(self.input)
        all_users = all_users.select('user_id', 'name', 'yelping_since', 'fans', 'review_count')\
            .withColumnRenamed('yelping_since', 'yelp_since')
        all_users.createOrReplaceTempView('users')
        all_users = self.sql_ctx.sql('SELECT *, NULL AS gender FROM users')
        self.save_data_frame_to_cassandra(all_users, YELP_USERS)

   
yu = YelpUsers()
yu.save_user_data()
# yu.get_gender_for_user_name()


