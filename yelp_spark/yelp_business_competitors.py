from pyspark.sql.types import *
from spark_base import SparkBase
from settings import BUSINESS_CATEGORIES_TABLE, BUSINESS_SIMILARITY, CASSANDRA_KEY_SPACE, CASSANDRA_SERVERS


class YelpBusinessCompetitors(SparkBase):

    def __init__(self, *args, **kwargs):
        super(YelpBusinessCompetitors, self).__init__(*args, **kwargs)

    def get_business_pairs(self, all_business):
        pass

    def get_business_categories(self):
        self.df_for(BUSINESS_CATEGORIES_TABLE)
        bus_cat = self.sql_ctx.sql('SELECT business_id, category FROM %s' % BUSINESS_CATEGORIES_TABLE)
        bus_cat = bus_cat.rdd.map(lambda row: (row['business_id'], [row['category']]))\
            .reduceByKey(lambda cat1, cat2: cat1+cat2)
        return bus_cat



    # [Row(count(DISTINCT business_id)=156261)]
    # [Row(count(DISTINCT category)=1240)]
    def all_business_list(self):
        self.df_for(BUSINESS_CATEGORIES_TABLE)
        all_business = self.sql_ctx.sql('SELECT business_id FROM %s'%BUSINESS_CATEGORIES_TABLE)
        all_business = all_business.rdd.flatMap(list)
        return all_business

    @staticmethod
    def calculate_similarity(row):
        business_1, business_2 = row
        business_1_cat = business_1[1]
        business_2_cat = business_2[1]
        intersection_cardinality = list(set(business_1_cat) & set(business_2_cat))
        union_cardinality = list(set(business_1_cat) | set(business_2_cat))
        jaccard_similarity = len(intersection_cardinality) / float(len(union_cardinality))
        return {
            'business_id1':  business_1[0],
            'business_id2': business_2[0],
            'jaccard_similarity': jaccard_similarity
        }

    # def save_jaccard_similarity_to_cassandra(self, similarity_df):
    #
    #     def save_similarity_to_cassandra(row):
    #         if row['jaccard_similarity'] != 0:
    #             from cassandra.cluster import Cluster
    #             INSERT_CQL = """
    #                                       INSERT INTO %s
    #                                       (business_id1, business_id2, jaccard_similarity)
    #                                       VALUES (%s, %s, %.5f)
    #             """
    #             cluster = Cluster(CASSANDRA_SERVERS)
    #             # session = cluster.connect(CASSANDRA_KEY_SPACE)
    #             # session.execute(INSERT_CQL % (BUSINESS_SIMILARITY,
    #             #     row['business_id1'],
    #             #     row['business_id2'],
    #             #     row['jaccard_similarity']
    #             # ))
    #         return None
    #
    #     return similarity_df.map(save_similarity_to_cassandra)



    def calculate_business_pair_similarity(self, business_pairs_rdd):
        return business_pairs_rdd.map(self.calculate_similarity)

ybc = YelpBusinessCompetitors()
all_bus_cat = ybc.get_business_categories()
all_bus_pairs = all_bus_cat.cartesian(all_bus_cat)
all_bus_pairs = all_bus_pairs.filter(lambda row: row[0][0] != row[1][0])
all_bus_sim = ybc.calculate_business_pair_similarity(all_bus_pairs)
all_bus_sim = all_bus_sim.toDF()
all_bus_sim.cache()
ybc.save_data_frame_to_cassandra(all_bus_sim, BUSINESS_SIMILARITY)
# all_bus_sim = all_bus_sim.filter(lambda row: row[1]!=0)

# print(all_bus_sim.take(1))



# all_business_list = ybc.all_business_list()
#
#
# import itertools
# pairs = itertools.combinations(all_business_list.collect(), 2)
# print(list(pairs))
#
#
# ybc.calculate_business_pair_similarity(pairs)


