#!/usr/bin/env python3

import hug
import asyncio
import base64
from marshmallow import fields, pprint
from rx import Observer
from rx import Observable

from serve.lfmc.models.Model import ModelSchema

from serve.lfmc.query.ShapeQuery import ShapeQuery
from serve.lfmc.results import ModelResult
from serve.lfmc.results.ModelResult import ModelResultSchema
from serve.lfmc.models.ModelRegister import ModelRegister, ModelsRegisterSchema

from serve.facade import do_query
from serve.facade import log_error
from serve.facade import consolidate
from serve.facade import do_conversion

import uuid
import numpy as np
import pandas as pd
import xarray as xr
import geojson
import json

import os
import socket
import sys
import io
import traceback
import logging
import serve.lfmc.config.debug as dev

from celery.result import AsyncResult, GroupResult, ResultBase
from celery import group
from celery import chain
from celery import chord
from celery import Celery

import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.debug("logger set to DEBUG")

api_ = hug.API(__name__)
api_.http.add_middleware(hug.middleware.CORSMiddleware(api_, max_age=10))

api = hug.get(on_invalid=hug.redirect.not_found)

suffix_output = hug.output_format.suffix({'.json': hug.output_format.pretty_json,
                                          '.mp4': hug.output_format.mp4_video,
                                          '.mov': hug.output_format.mov_video,
                                          '.nc': hug.output_format.file})

content_output = hug.output_format.on_content_type(
    {'application/x-netcdf4': hug.output_format.file})

app = Celery('facade',
             backend='redis://caching:6379/0',
             broker='redis://caching:6379/0')
app.Task.resultrepr_maxsize = 2000

logger.debug(app)


@api.get('/revoke', version=1)
def revoke(uuid):
    app.control.revoke(uuid)


# @hug.cli()
# @api.get('/validate', versions=range(1, 2))
# def validate():
#     mr = ModelRegister()
#     return mr.validate_catalog()


@hug.cli()
@api.urls('/models/idents', versions=range(1, 2), content_output=hug.output_format.pretty_json)
def model_idents():
    mr = ModelRegister()
    return mr.get_model_ids()


@hug.cli()
@api.urls('/models/names', versions=range(1, 2), content_output=hug.output_format.pretty_json)
def model_names():
    mr = ModelRegister()
    return mr.get_model_names()


@hug.cli()
@api.urls('/models/codes', versions=range(1, 2), content_output=hug.output_format.pretty_json)
def model_codes():
    mr = ModelRegister()
    return mr.get_model_codes()


# @hug.cli()
# @hug.get('/fuel.mp4', versions=1, output=suffix_output)
# async def fuel_mp4(geo_json,
#                    start: fields.String(),
#                    finish: fields.String(),
#                    models: hug.types.delimited_list(','),
#                    response_as: hug.types.number = None):
#     logger.debug("Responding to NETCDF query...")
#     query = ShapeQuery(start=start, finish=finish,
#                        geo_json=geojson.loads(geo_json), weighted=True)
#
#     logger.debug(query.temporal.start.strftime("%Y%m%d"))
#     logger.debug(query.temporal.finish.strftime("%Y%m%d"))
#
#     # Which models are we working with?
#     model_subset = ['DFMC']
#     if models is not None:
#         model_subset = models
#
#     mr = ModelRegister()
#
#     logger.debug("Responding to MP4 query...")
#     # TODO - only returns first model at the moment
#     mpg = (await asyncio.gather(*[mr.get("dead_fuel").mpg(query)]))[0]

    # response = ['http://cdn.landscapefuelmoisture.bushfirebehaviour.net.au/movies/{}'.format(m) for m in mpg]
    # response = "http://cdn.landscapefuelmoisture.bushfirebehaviour.net.au/movies/{0}".format(
    #     mpg)
    # errors = []
    #
    # asyncio.sleep(0.1)
    #
    # if dev.DEBUG:
    #     logger.debug(response)
    #
    # if len(errors) > 0:
    #     logger.debug(errors)
    #     return errors
    # else:
    #     # Default Response
    #     query.logResponse()
    #     return response


# @hug.cli()
# @hug.get('/fuel.nc', versions=1, output=suffix_output)
# async def fuel_nc(geo_json,
#                   start: fields.String(),
#                   finish: fields.String(),
#                   models: hug.types.delimited_list(',')):
#     logger.debug("Responding to NETCDF query...")
#     logger.debug(geojson.dumps(geo_json))
#
#     query = ShapeQuery(start=start, finish=finish,
#                        geo_json=geo_json)
#
#     logger.debug(query.temporal.start.strftime("%Y%m%d"))
#     logger.debug(query.temporal.finish.strftime("%Y%m%d"))
#
#     # Which models are we working with?
#     model_subset = ['DFMC']
#     if models is not None:
#         model_subset = models
#
#     mr = ModelRegister()
#
#     # TODO - only returns first model at the moment
#     response = await asyncio.gather(*[mr.get(model).get_netcdf(query) for model in model_subset])
#     logger.debug(response[0])
#
#     tuuid = uuid.uuid4()
#     tfile = '/tmp/{}.nc'.format(tuuid)
#     response[0].to_netcdf(tfile, format='NETCDF4')
#
#     errors = []
#
#     asyncio.sleep(0.1)
#
#     if dev.DEBUG:
#         logger.debug(response)
#
#     if len(errors) > 0:
#         logger.debug(errors)
#         return errors
#     else:
#         # Default Response
#         query.logResponse()
#         return tfile

@hug.post('/result.json', versions=1, output=suffix_output)
def result(uuid):
    res = AsyncResult(uuid, app=app)
    if res.state == 'SUCCESS':
        return res.get()


@hug.get('/submit_query.json', versions=1, output=suffix_output)
@hug.post('/submit_query.json', versions=1, output=suffix_output)
def submit_query(geo_json,
                 start: fields.String(),
                 finish: fields.String(),
                 models: hug.types.delimited_list(',')):
    """
    Takes query parameters and returns endpoint where progress/status can be monitored.

    Utilises Partial Chain to use result of ShapeQuery in call signature of 'do_query'.

    HUG then handles formatting the result as a json object.
    """
    final_result = group(
        [do_query.s(geo_json, start, finish, model) for model in models])

    res = final_result.delay()  # Removed 1 minute from now

    return [{'uuid': r.id} for r in res.children]


@hug.get('/progress.json', versions=1)
@hug.post('/progress.json', versions=1)
def get_progress(uuid):
    res = AsyncResult(uuid, app=app)
    if not res.ready():
        o = {
            'id': res.id,
            'STATE': res.state  # PENDING, STARTED, RETRY, FAILURE, SUCCESS
        }
    else:
        if res.successful():
            o = {
                'id': res.id,
                'STATE': res.state  # PENDING, STARTED, RETRY, FAILURE, SUCCESS
            }
        else:
            o = {
                'id': res.id,
                'STATE': res.state,  # PENDING, STARTED, RETRY, FAILURE, SUCCESS
                'error': res.traceback
            }
    return o


@hug.cli()
@hug.get('/fuel.json', versions=1, output=suffix_output)
@hug.post('/fuel.json', versions=1, output=suffix_output)
async def fuel_json(geo_json,
                    start: fields.String(),
                    finish: fields.String(),
                    models: hug.types.delimited_list(','),
                    hashkey=None):
    """
    :param geo_json:
    :param start:
    :param finish:
    :param models:
    :param hashkey:
    :return:
    """
    mr = ModelRegister()

    try:
        query = ShapeQuery(start=start,
                           finish=finish,
                           geo_json=geo_json)
    except ValueError as ve:
        return {'ValueError': json.dumps(ve)}

    logger.debug(query.temporal.start.strftime("%Y%m%d"))
    logger.debug(query.temporal.finish.strftime("%Y%m%d"))

    # Which model are we working with?

    if models is None:
        return {'ModelError': 'No suitable model found for: ' + models}
    elif len(models) > 1:
        raise ValueError(
            "LFMC API Server got Multiple model request. This shouldn't happen!")
    else:
        model = models[0]
        logger.debug("Responding to JSON query on model: %s" % model)
        model_future = await asyncio.gather(*[(mr.get(model)).get_shaped_timeseries(query)])
        schema = ModelResultSchema(many=True)
        response, errors = schema.dump(model_future)

        # HACK to fix hashkey
        response[0]['hashkey'] = hashkey

        asyncio.sleep(0.1)
        if dev.DEBUG:
            logger.debug(response)

        if len(errors) > 0:
            logger.debug(errors)
            return errors
        else:
            # Default Response
            query.logResponse()
            return response


@hug.exception(Exception)
def handle_exception(exception):
    logger.debug(exception)

    exc_type, exc_value, exc_traceback = sys.exc_info()
    output = io.StringIO()

    traceback.print_exception(exc_type, exc_value, exc_traceback,
                              limit=2, file=output)

    message = {'code': 500, 'error': '{}'.format(output.getvalue())}
    output.close()
    return message


@hug.cli()
@hug.get('/consolidate', versions=range(1, 2))
def consolidation():
    res = AsyncResult(consolidate(2019), app=app)
    if res.state == 'SUCCESS':
        return "OK"


@hug.cli()
@api.urls('/hostname', versions=range(1, 2))
def get_hostname():
    return os.uname()[1]


@hug.cli()
@api.urls('/models', versions=range(1, 2))
def get_models():
    if dev.DEBUG:
        logger.debug('Got models call. Answering now...')

    mr = ModelRegister()
    models_list_schema = ModelsRegisterSchema()
    resp, errors = models_list_schema.dump(mr)
    return resp


@hug.cli()
@api.urls('/model', examples='?name=ffdi', versions=range(1, 2), content_output=hug.output_format.pretty_json)
def get_model(name):
    mr = ModelRegister()
    model_schema = ModelSchema()
    resp, errors = model_schema.dump(mr.get(name))
    return resp


@hug.post('/convert.json', versions=range(1, 2))
def convert_this_shapefile(shp: str):
    final_result = do_conversion.s(shp)
    r = final_result.delay()
    return {'uuid': r.id}


@hug.get('/converted_shape.json', versions=range(2, 2), content_output=hug.output_format.file)
def get_converted_shape(uuid):
    res = AsyncResult(uuid, app=app)
    if res.state == 'SUCCESS':
        return res.get()


if __name__ == '__main__':
    app.start()
    get_hostname.interface.cli()
    fuel_json.interface.cli()
    get_models.interface.cli()
