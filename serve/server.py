#!/usr/bin/env python3

import io
import logging
import os
import sys
import traceback
from pathlib import Path

import hug
from celery import Celery
from celery import group
from celery.result import AsyncResult
from marshmallow import fields

import serve.lfmc.config.debug as dev
from serve.facade import consolidate
from serve.facade import do_conversion
from serve.facade import do_mp4
from serve.facade import do_netcdf
from serve.facade import do_query
from serve.lfmc.models.Model import ModelSchema
from serve.lfmc.models.ModelRegister import ModelRegister, ModelsRegisterSchema

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.debug("logger set to DEBUG")

api_ = hug.API(__name__)
api_.http.add_middleware(hug.middleware.CORSMiddleware(api_, max_age=10))

api = hug.get(on_invalid=hug.redirect.not_found)

suffix_output = hug.output_format.suffix({'.json': hug.output_format.pretty_json,
                                          '.mp4': hug.output_format.mp4_video,
                                          '.nc': hug.output_format.file})

content_output = hug.output_format.on_content_type(
    {'application/x-netcdf4': hug.output_format.file})

app = Celery('facade',
             backend='redis://caching:6379/0',
             broker='redis://caching:6379/0')
app.Task.resultrepr_maxsize = 2000

logger.debug(app)

with open(Path(os.getcwd()).joinpath('VERSION'), 'r') as vers:
    API_VERSION = vers.read()


@api.get('/revoke', version=1)
def revoke(uuid):
    app.control.revoke(uuid)


# @hug.cli()
# @api.get('/validate', versions=range(1, 2))
# def validate():
#     mr = ModelRegister()
#     return mr.validate_catalog()


@hug.directive()
def ip(request=None, **kwargs):
    return request.access_route[0]


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
@hug.get('/result.mp4', versions=1, output=suffix_output)
@hug.post('/result.mp4', versions=1, output=suffix_output)
def result_mpg(uuid):
    res = AsyncResult(uuid, app=app)
    if res.state == 'SUCCESS':
        logger.debug(res)
        return res.get()


@hug.get('/submit_query.mp4', versions=1, output=hug.output_format.pretty_json)
@hug.post('/submit_query.mp4', versions=1, output=hug.output_format.pretty_json)
def submit_mp4_query(geo_json,
                     start: fields.String(),
                     finish: fields.String(),
                     models: hug.types.delimited_list(',')
                     ):
    """
    Takes query parameters and returns endpoint where progress/status can be monitored.
    Utilises Partial Chain to use result of ShapeQuery in call signature of 'do_query'.
    HUG then handles formatting the result as a json object.
    """
    final_result = group(
        [do_mp4.s(geo_json, start, finish, model) for model in models])

    res = final_result.delay()  # Removed 1 minute from now

    resulting_task_uuids = [
        {'uuid': r.id, 'api_version': API_VERSION} for r in res.children]

    return resulting_task_uuids

###############
# NetCDF Code #
###############


@hug.cli()
@hug.get('/result.nc', versions=1, output=suffix_output)
@hug.post('/result.nc', versions=1, output=suffix_output)
def result_netcdf(uuid):
    res = AsyncResult(uuid, app=app)
    if res.state == 'SUCCESS':
        logger.debug(res)
        return res.get()


@hug.get('/submit_query.nc', versions=1, output=hug.output_format.pretty_json)
@hug.post('/submit_query.nc', versions=1, output=hug.output_format.pretty_json)
def submit_nc_query(geo_json,
                    start: fields.String(),
                    finish: fields.String(),
                    models: hug.types.delimited_list(',')):
    """
    Takes query parameters and returns endpoint where progress/status can be monitored.
    Utilises Partial Chain to use result of ShapeQuery in call signature of 'do_query'.
    HUG then handles formatting the result as a json object.
    """
    final_result = group(
        [do_netcdf.s(geo_json, start, finish, model) for model in models])

    res = final_result.delay()  # Removed 1 minute from now
    return [{'uuid': r.id, 'api_version': API_VERSION} for r in res.children]


#############
# JSON Code #
#############

@hug.cli()
@hug.get('/result.json', versions=1, output=suffix_output)
@hug.post('/result.json', versions=1, output=suffix_output)
def result(uuid):
    res = AsyncResult(uuid, app=app)
    if res.state == 'SUCCESS':
        resp = res.get()
        resp['api_version'] = API_VERSION
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
    print(models)

    final_result = group(
        [do_query.s(geo_json, start, finish, model) for model in models])

    res = final_result.delay()  # Removed 1 minute from now
    return [{'uuid': r.id, 'api_version': API_VERSION} for r in res.children]


@hug.get('/progress.json', versions=1)
@hug.post('/progress.json', versions=1)
def get_progress(uuid):
    res = AsyncResult(uuid, app=app)
    if not res.ready():
        o = {
            'id': res.id,
            'STATE': res.state,  # PENDING, STARTED, RETRY, FAILURE, SUCCESS
            'api_version': API_VERSION
        }
    else:
        if res.successful():
            o = {
                'id': res.id,
                'STATE': res.state,  # PENDING, STARTED, RETRY, FAILURE, SUCCESS
                'api_version': API_VERSION
            }
        else:
            o = {
                'id': res.id,
                'STATE': res.state,  # PENDING, STARTED, RETRY, FAILURE, SUCCESS
                'api_version': API_VERSION,
                'error': res.traceback
            }
    return o


# @hug.cli()
# @hug.get('/fuel.json', versions=1, output=suffix_output)
# @hug.post('/fuel.json', versions=1, output=suffix_output)
# async def fuel_json(geo_json,
#                     start: fields.String(),
#                     finish: fields.String(),
#                     models: hug.types.delimited_list(','),
#                     hashkey=None):
#     """
#     :param geo_json:
#     :param start:
#     :param finish:
#     :param models:
#     :param hashkey:
#     :return:
#     """
#     mr = ModelRegister()
#
#     try:
#         query = ShapeQuery(start=start,
#                            finish=finish,
#                            geo_json=geo_json)
#     except ValueError as ve:
#
#         return {'ValueError': '500'}
#
#     logger.debug(query.temporal.start.strftime("%Y%m%d"))
#     logger.debug(query.temporal.finish.strftime("%Y%m%d"))
#
#     # Which model are we working with?
#
#     if models is None:
#         return {'ModelError': 'No suitable model found for: ' + models}
#     elif len(models) > 1:
#         raise ValueError(
#             "LFMC API Server got Multiple model request. This shouldn't happen!")
#     else:
#         model = models[0]
#         logger.debug("Responding to JSON query on model: %s" % model)
#         model_future = await asyncio.gather(*[(mr.get(model)).get_shaped_timeseries(query)])
#         schema = ModelResultSchema(many=True)
#         response, errors = schema.dump(model_future)
#
#         # HACK to fix hashkey
#         response[0]['hashkey'] = hashkey
#
#         asyncio.sleep(0.1)
#         if dev.DEBUG:
#             logger.debug(response)
#
#         if len(errors) > 0:
#             logger.debug(errors)
#             return errors
#         else:
#             # Default Response
#             query.logResponse()
#             return response


@hug.exception(Exception)
def handle_exception(exception):
    logger.debug(exception)

    exc_type, exc_value, exc_traceback = sys.exc_info()
    output = io.StringIO()

    traceback.print_exception(exc_type, exc_value, exc_traceback,
                              limit=2, file=output)

    message = {'code': 500, 'error': '{}'.format(
        output.getvalue()), 'api_version': API_VERSION}
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
    logger.debug('Now Converting: %s' % shp)
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
