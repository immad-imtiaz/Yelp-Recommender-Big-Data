from flask import Blueprint, request
from flask import render_template
from flask_bootstrap import Bootstrap
from flask_bower import Bower

main = Blueprint('main', __name__)

import json
from engine import YelpRecommenderEngine

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from flask import Flask, jsonify


@main.route("/categories/<string:cat>", methods=["GET"])
def get_categories(cat):
    # logger.debug("All Yelp Categories", user_id)
    print(cat)
    all_categories = recommendation_engine.get_business_categories(cat).rdd.map(lambda x: {'category': x['category']})\
        .collect()
    return jsonify(categories=all_categories)


@main.route("/cities/", methods=["GET"])
def get_cities():
    # logger.debug("All Yelp Categories", user_id)
    all_cities = recommendation_engine.get_business_cities().rdd\
        .map(lambda x: {'city': x['city'].title(), 'state': x['state']})\
        .collect()
    return jsonify(cities=all_cities)


@main.route("/report/", methods=["POST"])
def get_report():
    print('----------Data-----------')
    data = request.form.to_dict()
    lng = data['vicinity[lng]']
    lat = data['vicinity[lat]']
    kms = data['kmRange']
    categories = data['categories'].split(',')
    report = recommendation_engine.get_business_report(categories, lng, lat, kms)
    print('----------Data-----------')
    return render_template('report.html')


@main.route('/')
@main.route('/index')
def index():
    user = {'nickname': 'Miguel'}  # fake user
    return render_template('main.html',
                           title='Home',
                           user=user)


def create_app(spark_context):
    global recommendation_engine
    recommendation_engine = YelpRecommenderEngine({'spark_context': spark_context})
    app = Flask(__name__)
    app.register_blueprint(main)
    app.static_folder = 'static'
    Bootstrap(app)
    Bower(app)
    return app

