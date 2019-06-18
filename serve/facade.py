from __future__ import absolute_import, unicode_literals

from celery import group
from celery import task
from celery import Celery

from serve.lfmc.query.ShapeQuery import ShapeQuery
from serve.lfmc.query.GeoQuery import GeoQuery
from serve.lfmc.models.ModelRegister import ModelRegister
from serve.lfmc.results.ModelResult import ModelResult
from serve.lfmc.results.MPEGFormatter import MPEGFormatter
from serve.lfmc.results.ModelResult import ModelResultSchema
from serve.lfmc.process.Conversion import Conversion

import time
import json
import asyncio
import logging
import base64
import marshmallow

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.debug("logger set to DEBUG")

app = Celery('facade',
             backend='redis://caching:6379/0',
             broker='redis://caching:6379/0')

app.Task.resultrepr_maxsize = 2000

##################
# NetCDF Results #
##################


@app.task(trail=True)
def do_netcdf(geo_json, start, finish, model):
    result = {}
    try:
        sq = GeoQuery(geo_json=geo_json,
                      start=start,
                      finish=finish)
        mr = ModelRegister()
        model = mr.get(model)

        looped = asyncio.new_event_loop()
        result = looped.run_until_complete(
            model.get_netcdf_results(sq))
    except ValueError as e:
        logger.error("ValueError")
        # result['error'] = json.dumps(e)

    # Return result as filename string
    # HUG will stream the binary data
    return result


###############
# MP4 Results #
###############

@app.task(trail=True)
def do_mp4(geo_json, start, finish, model):
    result = {}
    try:
        sq = GeoQuery(geo_json=geo_json,
                      start=start,
                      finish=finish)
        mr = ModelRegister()
        model = mr.get(model)

        looped = asyncio.new_event_loop()
        result = looped.run_until_complete(
            model.get_mp4_results(sq))
    except ValueError as e:
        logger.error("ValueError")
        # result['error'] = json.dumps(e)

    # Return result as filename string
    # HUG will stream the binary data
    logger.debug(result)
    # TODO - Double Check model.code is always var_name of DataSet!
    return result

################
# JSON Results #
################


@app.task(trail=True)
def do_query(geo_json, start, finish, model):
    result = {}
    try:
        sq = GeoQuery(geo_json=geo_json,
                      start=start,
                      finish=finish)
        mr = ModelRegister()
        model = mr.get(model)

        looped = asyncio.new_event_loop()
        result = looped.run_until_complete(
            model.get_timeseries_results(sq))
    except ValueError as e:
        logger.error("ValueError")
        # result['error'] = json.dumps(e)

    mrs = ModelResultSchema()
    json_result, errors = mrs.dump(result)
    return json_result


@app.task(trail=True)
def consolidate(year):
    mr = ModelRegister()
    model = mr.get('DFMC')
    looped = asyncio.new_event_loop()
    result = looped.run_until_complete(model.consolidate_year(year))
    return result


@app.task(trail=True)
def log_error(e):
    logger.warning(e)
    logger.debug(e)


@app.task(trail=True)
def do_conversion(shp):
    logger.debug('Got conversion request: ' + shp)
    c = Conversion()
    return c.convert_this(shp)

# if __name__ == '__main__':
#     ModelFacade.create_models()
