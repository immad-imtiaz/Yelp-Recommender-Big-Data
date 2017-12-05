import re
from pyspark.mllib.recommendation import ALS
from yelp_spark.spark_base import SparkBase
from yelp_spark.settings import *
from bokeh.models import (HoverTool, FactorRange, Plot, LinearAxis, Grid,
                          Range1d)
from bokeh.models.glyphs import VBar
from bokeh.plotting import figure
from bokeh.charts import Bar
from bokeh.palettes import Spectral6
from bokeh.embed import components
from bokeh.models.sources import ColumnDataSource

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class YelpRecommenderEngine(SparkBase):

    BUSINESS_RADIUS_SQL = """
        SELECT latitude, longitude, city, business_id,
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
        all_business_ids = all_business_in_range.select('business_id')
        all_business_ids.cache()
        day_script, day_div = self._get_day_wise_check_in_report(all_business_ids)
        hour_script, hour_div = self._get_hourly_check_in_report(all_business_ids)
        return day_script, day_div, hour_script, hour_div

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

    def _get_business_positive_negative_keywords(self, business_id_rdd):
        pass

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
            return days_sorting[x['day']], ([x['day']], [x['b_check_ins']])

        def transform2(x, y):
            return x[0]+y[0], x[1]+y[1]

        df_check_in = df_check_in.rdd\
            .map(transform)\
            .sortByKey(lambda x: x[0])\
            .map(lambda x: x[1])\
            .reduce(transform2)

        graph_data = {
            'day': df_check_in[0][:7],
            'check_in': df_check_in[1][:7],
            'color': Spectral6
        }

        plot = self._create_bar_chart(graph_data, 'Competitors Day Wise Check Ins', 'day',
                                      'check_in', 'Week Days', 'Average Check Ins')
        script, div = components(plot)
        return script, div

    def _get_hourly_check_in_report(self, business_id_rdd):
        business_id_rdd.createOrReplaceTempView('business_ids')
        self.df_for(CASSANDRA_KEY_SPACE, BUSINESS_TIME_CHECK_INS)
        df_check_in = self.sql_ctx.sql("""SELECT check_in.day_hour AS hour, AVG(check_in.check_in) AS b_check_ins
                                    FROM %s AS check_in
                                    INNER JOIN business_ids AS bids
                                    ON bids.business_id = check_in.business_id
                                    GROUP BY check_in.day_hour""" % BUSINESS_TIME_CHECK_INS)

        def transform(x):
            return int(x['hour'].split(':')[0]), ([int(x['hour'].split(':')[0])], [x['b_check_ins']])

        def transform2(x, y):
            return 0, (x[1][0] + y[1][0], x[1][1]+y[1][1])

        df_check_in = df_check_in.rdd \
            .map(transform) \
            .sortByKey() \
            .reduce(transform2)

        graph_data = {
            'hour': df_check_in[1][0],
            'check_in': df_check_in[1][1],
            'color': Spectral6
        }

        print(graph_data)

        plot = self._create_bar_chart(graph_data, 'Competitors Hour Wise Check Ins', 'hour',
                                      'check_in', 'Daily Hours', 'Average Check Ins')
        script, div = components(plot)
        return script, div

    def _get_business_categories(self, business_id_rdd):
        pass

    def create_hover_tool(self):
        """Generates the HTML for the Bokeh's hover data tool on our graph."""
        hover_html = """
          <div>
            <span class="hover-tooltip">$x</span>
          </div>
          <div>
            <span class="hover-tooltip">@bugs bugs</span>
          </div>
          <div>
            <span class="hover-tooltip">$@costs{0.00}</span>
          </div>
        """
        return HoverTool(tooltips=hover_html)

    def _create_bar_chart(self, data, title, x_name, y_name, x_label, y_label, hover_tool=None,
                         width=1200, height=400):
        """Creates a bar chart plot with the exact styling for the centcom
           dashboard. Pass in data as a dictionary, desired plot title,
           name of x axis, y axis and the hover tool HTML.
        """
        source = ColumnDataSource(data)
        xdr = FactorRange(factors=data[x_name])
        ydr = Range1d(start=0, end=max(data[y_name]) * 1.5)

        tools = []
        if hover_tool:
            tools = [hover_tool, ]

        plot = figure(title=title, x_range=xdr, y_range=ydr, plot_width=width,
                      plot_height=height, h_symmetry=False, v_symmetry=False,
                      min_border=0, toolbar_location="above", tools=tools,
                      responsive=True, outline_line_color="#666666")

        glyph = VBar(x=x_name, top=y_name, bottom=0, width=.4,
                     fill_color="#f38c59")
        plot.add_glyph(source, glyph)

        xaxis = LinearAxis()
        yaxis = LinearAxis()

        plot.add_layout(Grid(dimension=0, ticker=xaxis.ticker))
        plot.add_layout(Grid(dimension=1, ticker=yaxis.ticker))
        plot.toolbar.logo = None
        plot.min_border_top = 0
        plot.xgrid.grid_line_color = None
        plot.ygrid.grid_line_color = "#999999"
        plot.yaxis.axis_label = y_label
        plot.ygrid.grid_line_alpha = 0.1
        plot.background_fill_alpha = 0
        # plot.border_fill_color = 'transparent'
        plot.xaxis.axis_label = x_label
        plot.xaxis.major_label_orientation = 1
        return plot









