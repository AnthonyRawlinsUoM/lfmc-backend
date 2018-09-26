import hug
import asyncio

from marshmallow import fields, pprint
from rx import Observer

from lfmc.models.Model import ModelSchema
from lfmc.process.Conversion import Conversion
from lfmc.query.ShapeQuery import ShapeQuery
from lfmc.results import ModelResult
from lfmc.results.ModelResult import ModelResultSchema
from lfmc.models.ModelRegister import ModelRegister, ModelsRegisterSchema
from lfmc.monitor.RequestMonitor import RequestMonitor
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
import lfmc.config.debug as dev

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


@hug.post('/test', versions=1)
def test(body=None):
    logger.debug(body)
    logger.debug('>> Request complete. <<')
    return {"200": "OK"}


@hug.cli()
@api.urls('/validate', versions=range(1, 2))
def validate():
    mr = ModelRegister()
    return mr.validate_catalog()


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


@hug.cli()
@hug.get('/fuel.mp4', versions=1, output=suffix_output)
async def fuel_mp4(geo_json,
                   start: fields.String(),
                   finish: fields.String(),
                   models: hug.types.delimited_list(','),
                   response_as: hug.types.number = None):
    logger.debug("Responding to NETCDF query...")
    query = ShapeQuery(start=start, finish=finish,
                       geo_json=geojson.loads(geo_json), weighted=True)

    rm = RequestMonitor()
    rm.log_request(query)

    logger.debug(query.temporal.start.strftime("%Y%m%d"))
    logger.debug(query.temporal.finish.strftime("%Y%m%d"))

    # Which models are we working with?
    model_subset = ['DFMC']
    if models is not None:
        model_subset = models

    mr = ModelRegister()

    logger.debug("Responding to MP4 query...")
    # TODO - only returns first model at the moment
    mpg = (await asyncio.gather(*[mr.get("dead_fuel").mpg(query)]))[0]

    # response = ['http://cdn.landscapefuelmoisture.bushfirebehaviour.net.au/movies/{}'.format(m) for m in mpg]
    response = "http://cdn.landscapefuelmoisture.bushfirebehaviour.net.au/movies/{0}".format(
        mpg)
    errors = []

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


@hug.cli()
@hug.get('/fuel.nc', versions=1, output=suffix_output)
async def fuel_nc(geo_json,
                  start: fields.String(),
                  finish: fields.String(),
                  models: hug.types.delimited_list(',')):
    logger.debug("Responding to NETCDF query...")
    logger.debug(geojson.dumps(geo_json))

    query = ShapeQuery(start=start, finish=finish,
                       geo_json=geo_json)

    rm = RequestMonitor()
    rm.log_request(query)

    logger.debug(query.temporal.start.strftime("%Y%m%d"))
    logger.debug(query.temporal.finish.strftime("%Y%m%d"))

    # Which models are we working with?
    model_subset = ['DFMC']
    if models is not None:
        model_subset = models

    mr = ModelRegister()

    # TODO - only returns first model at the moment
    response = await asyncio.gather(*[mr.get(model).get_netcdf(query) for model in model_subset])
    logger.debug(response[0])

    tuuid = uuid.uuid4()
    tfile = '/tmp/{}.nc'.format(tuuid)
    response[0].to_netcdf(tfile, format='NETCDF4')

    errors = []

    asyncio.sleep(0.1)

    if dev.DEBUG:
        logger.debug(response)

    if len(errors) > 0:
        logger.debug(errors)
        return errors
    else:
        # Default Response
        query.logResponse()
        return tfile


@hug.cli()
@hug.get('/fuel.json', versions=1, output=suffix_output)
@hug.post('/fuel.json', versions=1, output=suffix_output)
async def fuel_json(geo_json,
                    start: fields.String(),
                    finish: fields.String(),
                    models: hug.types.delimited_list(','),
                    hashkey):
    """
    :param geo_json:
    :param start:
    :param finish:
    :param models:
    :param hashkey:
    :return:
    """
    try:
        query = ShapeQuery(start=start,
                           finish=finish,
                           geo_json=geo_json)
    except ValueError as ve:
        return {'ValueError': json.dumps(ve)}

    rm = RequestMonitor()
    rm.log_request(query)
    logger.debug(query.temporal.start.strftime("%Y%m%d"))
    logger.debug(query.temporal.finish.strftime("%Y%m%d"))

    # Which model are we working with?

    if models is None:
        return {'ModelError': 'No suitable model found for: ' + models}
    elif len(models) > 1:
        logger.debug(
            "LFMC API Server got Multiple model request. This shouldn't happen!")
        return None
    else:
        model = models[0]
        mr = ModelRegister()
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


# @hug.cli()
# @hug.post(('/fuel', '/fuel.json', '/fuel.mp4', '/fuel.mov', '/fuel.nc'), versions=2, output=suffix_output)
# async def fuel_moisture(geo_json,
#                         start: fields.String(),
#                         finish: fields.String(),
#                         weighted: fields.Bool(),
#                         models: hug.types.delimited_list(','),
#                         response_as: hug.types.number):
#     """
#         :param geo_json:
#         :param start:
#         :param finish:
#         :param weighted:
#         :param models:
#         :param response_as:
#         :return:
#         """
#     query = ShapeQuery(start=start, finish=finish,
#                        geo_json=geo_json, weighted=weighted)
#
#     mr = ModelRegister()
#     omr = ObservedModelResponder()
#     rm = RequestMonitor()
#     rm.log_request(query)
#     print(query)
#
#     # Which models are we working with?
#     model_subset = ['DFMC']
#     if models is not None:
#         model_subset = models
#
#     mr.apply_shape_for_timeseries(query)
#     mr.subscribe(omr)
#
#     return omr.get()


@hug.cli()
@api.urls('/monitors', versions=range(1, 2))
async def monitors():
    return {"monitors": ["processes", "requests"]}


@hug.cli()
@api.urls('/monitors/requests/complete', versions=range(1, 2))
async def monitor_complete_requests():
    rm = RequestMonitor()
    return rm.completed_requests()


@hug.cli()
@api.urls('/monitors/requests/all', versions=range(1, 2))
async def monitor_all_requests():
    return RequestMonitor().all_requests()


@hug.cli()
@api.urls('/monitors/requests/active', versions=range(1, 2))
async def monitor_active_requests():
    return RequestMonitor().open_requests()


@hug.cli()
@api.urls('/monitors/processes', versions=range(1, 2))
async def monitor_processes():
    return pprint({"processes": asyncio.Task.all_tasks()})


@hug.cli()
@api.urls('/hostname', versions=range(1, 2))
def get_hostname():
    return os.uname()[1]


@hug.cli()
@api.urls('/models', versions=range(1, 2))
async def get_models():
    if dev.DEBUG:
        logger.debug('Got models call. Answering now...')
    model_register = ModelRegister()
    models_list_schema = ModelsRegisterSchema()
    resp, errors = models_list_schema.dump(model_register)
    return resp


@hug.cli()
@api.urls('/model', examples='?name=ffdi', versions=range(1, 2), content_output=hug.output_format.pretty_json)
async def get_model(name):
    model_register = ModelRegister()
    model_schema = ModelSchema()
    resp, errors = model_schema.dump(model_register.get(name))
    return resp


@hug.post('/convert.json', versions=range(1, 2), content_output=hug.output_format.file)
async def get_converted_shapefile(shp: str):
    logger.debug('Got conversion request: ' + shp)
    resp = Conversion.convert_this(shp)
    #
    # try:
    #     del resp['crs']
    # except KeyError:
    #     # pass
    #     print(resp)
    return resp


if __name__ == '__main__':
    get_hostname.interface.cli()
    get_log.interface.cli()
    fuel_json.interface.cli()
    fuel_mp4.interface.cli()
    fuel_nc.interface.cli()
    get_models.interface.cli()
    monitors.interface.cli()
    monitor_processes.interface.cli()
    monitor_all_requests.interface.cli()
    monitor_active_requests.interface.cli()
    monitor_complete_requests.interface.cli()
