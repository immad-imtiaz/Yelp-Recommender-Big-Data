from cassandra.cluster import Cluster
from yelp_spark.settings import CASSANDRA_KEY_SPACE, CASSANDRA_SERVERS, YELP_USERS, NAMES_GENDER
import requests


cluster = Cluster(CASSANDRA_SERVERS)
session = cluster.connect(CASSANDRA_KEY_SPACE)

rows = session.execute_async('SELECT user_id, name FROM %s' % YELP_USERS)

result = rows.result()
all_name = [user.name for user in result]
all_unique_names = list(set(all_name))


for name in all_unique_names[:5]:
    r = requests.get('https://api.genderize.io/?name=%s' % name)
    if r.status_code == 200:
        gender = r.json()['gender']
        if gender:
            rows = session.execute('INSERT INTO %s (id, data) VALUES (%s, %)' % (NAMES_GENDER, gender, name))
