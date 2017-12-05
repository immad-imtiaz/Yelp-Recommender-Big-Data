from flask import Blueprint, request
from flask import render_template, redirect, url_for
from flask_bootstrap import Bootstrap
from flask_bower import Bower
from flask_googlemaps import Map
from flask_googlemaps import GoogleMaps


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


@main.route("/report/", methods=["POST", "GET"])
def get_report():
    data = request.form.to_dict()
    lng = float(data['lng'])
    lat = float(data['lat'])
    kms = data['kms']
    categories = data['categories'].split(',')
    day_script, day_div, hour_script, hour_div, map_business, top_5 = recommendation_engine.get_business_report(categories,
                                                                                                         lng, lat, kms)

    markers = []
    for bus in map_business:
        info_box = """
                            <div class='b-map'>
                                <div class='b-map-name'>%s</div>
                                <div class='b-map-rating'>Rating: %s</div>
                                <div class='b-map-reviews'>Reviews: %s</div>
                                <div class='b-map-distance'>Distance in kms: %s</div>
                            </div>
                       """ % (bus['name'], bus['review_count'], bus['stars'], bus['distance'])
        markers.append({
            'icon': 'http://maps.google.com/mapfiles/ms/icons/blue-dot.png',
            'lat': bus['latitude'],
            'lng': bus['longitude'],
            'infobox': info_box
        })
    markers.append({
            'icon': 'http://maps.google.com/mapfiles/ms/icons/green-dot.png',
            'lat': lat,
            'lng': lng,
            'infobox': "<b>Your Location</b>"
    })

    map = Map(
        identifier="sndmap",
        lat=lat,
        lng=lng,
        markers=markers,
        style="height:500px;margin:0;"

    )
    return render_template('report.html',
                           map=map,
                           day_script=day_script,
                           day_div=day_div,
                           hour_script=hour_script,
                           hour_div=hour_div,
                           top_5=top_5,
                           title='New Business Report')


@main.route("/completeReport/")
def get_complete_report():
    print(request.args)

    return render_template('report2.html')


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
    GoogleMaps(app, key='AIzaSyAT3qnHmIi6ujVBhyoFGtgwKIPQMPaTWA4')
    return app

