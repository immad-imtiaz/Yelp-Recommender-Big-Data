from settings import BUSINESS_DAY_CHECK_INS, BUSINESS_TIME_CHECK_INS, CASSANDRA_KEY_SPACE
from pyspark.sql import functions, types
import json
from spark_base import SparkBase


class YelpBusinessCheckIn(SparkBase):

    def __init__(self, *args, **kwargs):
        super(YelpBusinessCheckIn, self).__init__(*args, **kwargs)

    @staticmethod
    def parse_json(passed):
        '''
        This function will take the check in JSON object and will return the tuple of form
        (business_id,day,hour,checkin_count)
        :param passed:
        :return:
        '''
        for day, value in passed['time'].items():
            for hour, checkin in passed['time'][day].items():
                yield passed['business_id'], day, hour, checkin

    def transfer_check_ins_by_hour(self):
        text = self.spark.textFile(self.INPUT_DIRECTORY+'/checkin.json')
        checkInRdd = text.map(lambda line: json.loads(line))
        tupleRdd = checkInRdd.flatMap(self.parse_json)
        schema = types.StructType(
            [types.StructField('business_id', types.StringType(), False),
             types.StructField('day_of_week', types.StringType(), False)
                , types.StructField('day_hour', types.StringType(), False),
             types.StructField('check_in', types.StringType(), False)])
        check_in_df = self.sql_ctx.createDataFrame(tupleRdd, schema)
        self.save_data_frame_to_cassandra(check_in_df, BUSINESS_TIME_CHECK_INS)

    def transfer_check_in_by_day(self):
        self.df_for(BUSINESS_TIME_CHECK_INS)
        day_check_in_df = self.sql_ctx.sql("""SELECT business_id, day_of_week,
                                            SUM(check_in) FROM %s GROUP BY
                                            business_id, day_of_week""" % BUSINESS_TIME_CHECK_INS)
        day_check_in_df = day_check_in_df.withColumnRenamed('sum(check_in)', 'check_in')
        self.save_data_frame_to_cassandra(day_check_in_df, BUSINESS_DAY_CHECK_INS)

ybci = YelpBusinessCheckIn()
#ybci.transfer_check_ins_by_hour()
ybci.transfer_check_in_by_day()
