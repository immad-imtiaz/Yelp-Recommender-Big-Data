from cassandra.cluster import Cluster
from settings import CASSANDRA_SERVERS, CASSANDRA_KEYSPACE


class YelpCassandra(object):

    KEY_SPACE_QUERY = """
                        CREATE KEYSPACE IF NOT EXISTS %s
                        WITH replication = {'class': 'SimpleStrategy',
                        'replication_factor' : 3}
    """

    BUSINESS_CATEGORY_TABLE_CQL = """
                    CREATE TABLE IF NOT EXISTS business_categories(
                      business_id TEXT,
                      category TEXT,
                      id uuid,
                      PRIMARY KEY(id, business_id, category));
    """

    def __init__(self):
        self.cluster = Cluster(CASSANDRA_SERVERS)
        self.session = self.cluster.connect(CASSANDRA_KEYSPACE)
        self.session.execute(self.KEY_SPACE_QUERY % CASSANDRA_KEYSPACE)
        self.session.execute(self.BUSINESS_CATEGORY_TABLE_CQL)


cassadra_connector = YelpCassandra()