import time, sys, cherrypy, os
from paste.translogger import TransLogger
from app import create_app
from yelp_spark.settings import CASSANDRA_SERVERS
from pyspark import SparkContext, SparkConf


def init_spark_context():
    conf = SparkConf().setAppName('Yelp Recommendation') \
            .set('spark.cassandra.connection.host', ','.join(CASSANDRA_SERVERS)) \
            .set('spark.dynamicAllocation.maxExecutors', 20)
    conf.setMaster('local[*]')
    spark = SparkContext(conf=conf, pyFiles=['engine.py', 'app.py'])
    return spark


def run_server(app):
    # Enable WSGI access logging via Paste
    app_logged = TransLogger(app)

    # Mount the WSGI callable object (app) on the root directory
    cherrypy.tree.graft(app_logged, '/')

    # Set the configuration of the web server
    cherrypy.config.update({
        'engine.autoreload.on': True,
        'log.screen': True,
        'server.socket_port': 5432,
        'server.socket_host': '0.0.0.0'
    })

    # Start the CherryPy WSGI web server
    cherrypy.engine.start()
    cherrypy.engine.block()


if __name__ == "__main__":
    # Init spark context and load libraries
    sc = init_spark_context()
    # dataset_path = os.path.join('datasets', 'ml-latest')
    app = create_app(sc)
    # start web server
    run_server(app)
