from cassandra.cluster import Cluster

from yelp_spark.settings import CASSANDRA_SERVERS, \
    CASSANDRA_KEY_SPACE, \
    BUSINESS_CATEGORIES_TABLE, \
    BUSINESS_SIMILARITY,\
    BUSINESS_TIME_CHECK_INS,\
    BUSINESS_DAY_CHECK_INS, \
    CATEGORIES,\
    YELP_USERS,\
    NAMES_GENDER, \
    BUSINESS_CITY, \
    BUSINESS_SENTIMENTS, \
    POSITIVE_WORDS,\
    NEGATIVE_WORDS




class CassandraCreator(object):

    KEY_SPACE_QUERY = """
                        CREATE KEYSPACE IF NOT EXISTS %s
                        WITH replication = {'class': 'SimpleStrategy',
                        'replication_factor' : 2}
    """

    BUSINESS_CATEGORY_TABLE_CQL = """
                      CREATE TABLE IF NOT EXISTS %s(
                      business_id TEXT,
                      category TEXT,
                      PRIMARY KEY(business_id, category));
    """

    BUSINESS_CATEGORY_SIMILARITY = """
                      CREATE TABLE IF NOT EXISTS %s(
                      business_id1 TEXT,
                      business_id2 TEXT,
                      jaccard_similarity FLOAT,
                      PRIMARY KEY(business_id1, business_id2))
    """

    BUSINESS_TIME_CHECK_INS_TABLE = """
                          CREATE TABLE IF NOT EXISTS %s(
                          business_id TEXT,
                          day_of_week TEXT,
                          day_hour TEXT,
                          check_in INT,
                          PRIMARY KEY(business_id, day_of_week, day_hour))
    """

    BUSINESS_DAY_CHECK_INS_TABLE = """
                              CREATE TABLE IF NOT EXISTS %s(
                              business_id TEXT,
                              day_of_week TEXT,
                              check_in INT,
                              PRIMARY KEY(business_id, day_of_week))
    """

    CATEGORIES_TABLE = """
                              CREATE TABLE IF NOT EXISTS %s(
                              category TEXT,
                              PRIMARY KEY(category)
                              )
    """

    YELP_USER_TABLE = """     CREATE TABLE IF NOT EXISTS %s(
                              user_id TEXT,
                              name TEXT,
                              gender TEXT,
                              yelp_since DATE,
                              fans INT,
                              review_count INT,
                              PRIMARY KEY(user_id)
                              )
    """

    NAME_GENDER = """
                    CREATE TABLE IF NOT EXISTS %s(
                    name TEXT,
                    gender TEXT,
                    PRIMARY KEY(name, gender)
                    )

    """

    BUSINESS_CITY = """
                    CREATE TABLE IF NOT EXISTS %s(
                    business_id TEXT,
                    city TEXT,
                    state TEXT,
                    latitude FLOAT,
                    longitude FLOAT,
                    stars FLOAT,
                    review_count INT,
                    name TEXT,
                    neighborhood TEXT,
                    postal_code TEXT,
                    address TEXT,
                    is_open INT,
                    PRIMARY KEY(business_id, city, state)
                    )
    """

    BUSINESS_SENTIMENTS_TABLE = """
                    CREATE TABLE IF NOT EXISTS %s (
                      business_id TEXT,
                      tip LIST<text>,
                      avg_sentiments FLOAT,
                      pos_words LIST<text>,
                      neg_words LIST<text>,
                      PRIMARY KEY(business_id)
                    )

    """

    SENTIMENT_WORDS = """
                    CREATE TABLE IF NOT EXISTS %s (
                      word TEXT,
                      sentiment INT,
                      PRIMARY KEY (word)
                    )
    """

    @classmethod
    def create_key_space(cls):
        cluster = Cluster(CASSANDRA_SERVERS)
        session = cluster.connect()
        session.execute(cls.KEY_SPACE_QUERY % CASSANDRA_KEY_SPACE)
        session = cluster.connect(CASSANDRA_KEY_SPACE)
        session.execute(cls.BUSINESS_CATEGORY_TABLE_CQL % BUSINESS_CATEGORIES_TABLE)
        session.execute(cls.BUSINESS_CATEGORY_SIMILARITY % BUSINESS_SIMILARITY)
        session.execute(cls.BUSINESS_DAY_CHECK_INS_TABLE % BUSINESS_DAY_CHECK_INS)
        session.execute(cls.BUSINESS_TIME_CHECK_INS_TABLE % BUSINESS_TIME_CHECK_INS)
        session.execute(cls.CATEGORIES_TABLE % CATEGORIES)
        session.execute(cls.YELP_USER_TABLE % YELP_USERS)
        session.execute(cls.NAME_GENDER % NAMES_GENDER)
        session.execute(cls.BUSINESS_CITY % BUSINESS_CITY)
        session.execute(cls.BUSINESS_SENTIMENTS_TABLE % BUSINESS_SENTIMENTS)
        session.execute(cls.SENTIMENT_WORDS % POSITIVE_WORDS)
        session.execute(cls.SENTIMENT_WORDS % NEGATIVE_WORDS)



