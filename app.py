from flask import Blueprint

main = Blueprint('main', __name__)

import json
from engine import YelpRecommenderEngine

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from flask import Flask, request


@main.route("/categories/", methods=["GET"])
def get_categories():
    logger.debug("All Yelp Categories", user_id)
    all_categories = yelp_engine.get_business_categories()
    return json.dumps(all_categories)


def create_app(spark_context, dataset_path):
    global recommendation_engine
    recommendation_engine = YelpRecommenderEngine()
    app = Flask(__name__)
    app.register_blueprint(main)
    return app

