from yelp_spark.spark_base import SparkBase
from yelp_spark.settings import *

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class YelpRecommenderEngine(SparkBase):

    BUSINESS_RADIUS_SQL = """
        SELECT latitude, longitude, city, business_id, name, stars, review_count,
        (6371 * acos(cos(radians(%s)) * cos(radians(latitude)) * cos(radians(longitude)
        - radians(%s)) + sin(radians(%s)) * sin(radians(latitude )))) AS distance
        FROM %s
        having distance < %s ORDER BY distance
    """

    def __init__(self, *args, **kwargs):
        super(YelpRecommenderEngine, self).__init__(*args, **kwargs)

    def get_business_categories(self, cat):
        categories = self.df_for(CASSANDRA_KEY_SPACE, CATEGORIES)
        return categories.filter(categories.category.contains(cat) | categories.category.contains(cat.title()))

    def get_business_report(self, categories, lon, lat, kms):
        all_business_in_range = self._get_business_with_in_radius(categories, lon, lat, kms)
        all_business_in_range_map = all_business_in_range.select('name', 'business_id', 'distance',
                                                                 'latitude', 'longitude', 'stars', 'review_count')
        all_business_ids = all_business_in_range.select('business_id')
        all_business_ids.cache()
        day_wise = self._get_day_wise_check_in_report(all_business_ids)
        hour_wise = self._get_hourly_check_in_report(all_business_ids)
        top_competitors = all_business_in_range_map.orderBy("stars", ascending=False).take(5)
        all_business_in_range_map = all_business_in_range_map.collect()
        pos_words = self._get_pos_words(all_business_ids)
        neg_words = self._get_neg_words(all_business_ids)
        return day_wise, hour_wise, all_business_in_range_map, top_competitors, \
            neg_words, pos_words

    def _get_business_with_in_radius(self, categories, lon, lat, kms):
        self.df_for(CASSANDRA_KEY_SPACE, BUSINESS_CITY)
        self.df_for(CASSANDRA_KEY_SPACE, BUSINESS_CATEGORIES_TABLE)
        bus_df = self.sql_ctx.sql(self.BUSINESS_RADIUS_SQL % (lat, lon, lat, BUSINESS_CITY, kms))
        bus_cat = self.sql_ctx.sql('SELECT business_id, category FROM %s' % BUSINESS_CATEGORIES_TABLE)
        bus_cat_rdd = bus_cat.rdd.map(lambda row: (row['business_id'], [row['category']])) \
            .reduceByKey(lambda cat1, cat2: cat1 + cat2)

        def find_jaccard_similarity(kv):
            business_id, business_categories = kv
            intersection_cardinality = list(set(business_categories) & set(categories))
            # union_cardinality = list(set(business_categories) | set(categories))
            jaccard_similarity = len(intersection_cardinality) / float(len(categories))
            return {
                'business_id': business_id,
                'jaccard_score': jaccard_similarity
            }

        bus_cat_rdd = bus_cat_rdd.map(find_jaccard_similarity)
        bus_cat_df = bus_cat_rdd.toDF()
        bus_cat_df.createOrReplaceTempView('business_similarity')
        bus_df.createOrReplaceTempView('business_location')
        all_business = self.sql_ctx.sql("""SELECT bl.*,bs.jaccard_score  FROM business_location AS bl
                        INNER JOIN business_similarity AS bs
                        ON bs.business_id = bl.business_id
                        WHERE bs.jaccard_score >= %s
                        ORDER BY bs.jaccard_score"""%JACCARD_SIMILARITY_THRESHOLD)

        return all_business



    def _get_pos_words(self, business_id_list):
        data_frame = self.df_for(CASSANDRA_KEY_SPACE, BUSINESS_SENTIMENTS)
        words = data_frame.join(business_id_list, business_id_list.business_id == data_frame.business_id, 'inner') \
            .select('pos_words')
        words = words.rdd.flatMap(lambda x: x['pos_words']).map(lambda x: (x, 1))
        words = words.reduceByKey(lambda x, y: x + y)
        return words.map(lambda x: {'text': x[0], 'size': x[1]}).collect()

    def _get_neg_words(self, business_id_list):
        data_frame = self.df_for(CASSANDRA_KEY_SPACE, BUSINESS_SENTIMENTS)
        words = data_frame.join(business_id_list, business_id_list.business_id == data_frame.business_id, 'inner') \
            .select('neg_words')
        words = words.rdd.flatMap(lambda x: x['neg_words']).map(lambda x: (x, 1))
        words = words.reduceByKey(lambda x, y: x + y)
        return words.map(lambda x: {'text': x[0], 'size': x[1]}).collect()

    def _get_day_wise_check_in_report(self, business_id_rdd):
        business_id_rdd.createOrReplaceTempView('business_ids')
        self.df_for(CASSANDRA_KEY_SPACE, BUSINESS_DAY_CHECK_INS)
        df_check_in = self.sql_ctx.sql("""SELECT check_in.day_of_week AS day, AVG(check_in.check_in) AS b_check_ins
                            FROM %s AS check_in
                            INNER JOIN business_ids AS bids
                            ON bids.business_id = check_in.business_id
                            GROUP BY check_in.day_of_week""" % BUSINESS_DAY_CHECK_INS)
        days_sorting = {
            'Sunday': 1,
            'Monday': 2,
            'Tuesday': 3,
            'Wednesday': 4,
            'Thursday': 5,
            'Friday': 6,
            'Saturday': 7
        }

        def transform(x):
            return days_sorting[x['day']], (x['day'], x['b_check_ins'])

        def transform2(x):
            return {
                'day': x[1][0],
                'check_ins': x[1][1],
                'color': COLORS[x[0]]
            }

        df_check_in = df_check_in.rdd\
            .map(transform)\
            .sortByKey(lambda x: x[0])\
            .map(transform2)

        return df_check_in.collect()



    def _get_hourly_check_in_report(self, business_id_rdd):
        business_id_rdd.createOrReplaceTempView('business_ids')
        self.df_for(CASSANDRA_KEY_SPACE, BUSINESS_TIME_CHECK_INS)
        df_check_in = self.sql_ctx.sql("""SELECT check_in.day_hour AS hour, AVG(check_in.check_in) AS b_check_ins
                                    FROM %s AS check_in
                                    INNER JOIN business_ids AS bids
                                    ON bids.business_id = check_in.business_id
                                    GROUP BY check_in.day_hour""" % BUSINESS_TIME_CHECK_INS)

        def transform(x):
            return int(x['hour'].split(':')[0]), (x['hour'], x['b_check_ins'])

        def transform2(x):
            return {
                'hour': x[1][0],
                'check_ins': x[1][1],
                'color': COLORS[x[0]]
            }

        df_check_in = df_check_in.rdd \
            .map(transform) \
            .sortByKey() \
            .map(transform2)
        return df_check_in.collect()









