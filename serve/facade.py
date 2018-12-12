from __future__ import absolute_import, unicode_literals

from celery import group
from celery import task
from celery import Celery

from serve.lfmc.query.ShapeQuery import ShapeQuery
from serve.lfmc.models.ModelRegister import ModelRegister
from serve.lfmc.results import ModelResult
from serve.lfmc.results.ModelResult import ModelResultSchema
import time
import json
import asyncio
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.debug("logger set to DEBUG")

app = Celery('facade',
             backend='redis://caching:6379/0',
             broker='redis://caching:6379/0')


@app.task(trail=True)
def do_query(geo_json, start, finish, model):
    result = {}
    try:
        sq = ShapeQuery(geo_json=geo_json,
                        start=start,
                        finish=finish)
        mr = ModelRegister()
        model = mr.get(model)

        looped = asyncio.new_event_loop()
        result = looped.run_until_complete(
            model.get_shaped_timeseries(sq))
    except ValueError as e:
        logger.error(e.message)

    mrs = ModelResultSchema()
    json_result, errors = mrs.dump(result)
    logger.debug(json_result)
    return json.dumps(json_result)


@app.task
def get_query_status(uuid):
    return ""


@app.task
def get_result(uuid):
    return ""


@app.task
def log_error(e):
    logger.warning(e)
    print(e)


# if __name__ == '__main__':
#     ModelFacade.create_models()
