import asyncio
import datetime as dt

import requests
import rx
from marshmallow import fields, Schema
from rx import Observable
from lfmc.config import debug as dev
from lfmc.library.GeoServer import GeoServer
from lfmc.library.geoserver.catalog import FailedRequestError, UploadError
from lfmc.models.Curing import CuringModel
from lfmc.models.JASMIN import JasminModel
from lfmc.models.LiveFuel import LiveFuelModel
from lfmc.models.Model import Model, ModelSchema
from lfmc.models.DeadFuel import DeadFuelModel
# from lfmc.models.LiveFuel import LiveFuelModel
from lfmc.models.FFDI import FFDIModel
from lfmc.models.KBDI import KBDIModel
from lfmc.models.GFDI import GFDIModel
from lfmc.models.AWRA import AWRAModel
from lfmc.models.DF import DFModel
from lfmc.models.Temp import TempModel
from lfmc.models.RelativeHumidity import RHModel

# from lfmc.models.Matthews import Matthews
from lfmc.models.Yebra import YebraModel


from lfmc.models.rx.ObservableModelRegister import ObservableModelRegister
from lfmc.process.ProcessQueue import ProcessQueue
from lfmc.query import ShapeQuery
from lfmc.results.DataPoint import DataPoint
from lfmc.results.ModelResult import ModelResult

import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.debug("logger set to DEBUG")


class ModelRegister(Observable):

    def __init__(self):
        dead_fuel = DeadFuelModel()
        live_fuel = LiveFuelModel()
        temp = TempModel()
        humidity = RHModel()
        ffdi = FFDIModel()
        gfdi = GFDIModel()
        kbdi = KBDIModel()
        awra = AWRAModel()
        curing = CuringModel()
        jasmin = JasminModel()
        drought = DFModel()
        # matthews = Matthews()
        yebra = YebraModel()

        self.models = [dead_fuel,
                       live_fuel,
                       ffdi,
                       temp,
                       humidity,
                       gfdi,
                       awra,
                       curing,
                       jasmin,
                       kbdi,
                       drought,
                       yebra
                       ]

        self.model_names = self.get_model_names()
        self.geo_server = GeoServer()
        self.pq = ProcessQueue()
        # validation = self.validate_catalog()
        pass

    def validate_catalog(self):
        validation = []
        errors = None
        published_ncs = []

        try:
            stores = self.geo_server.catalog.get_stores(workspace='lfmc')
            logger.debug(stores)

            for coverage_store in stores:
                published_ncs.append(coverage_store.name)
                logger.debug(coverage_store.name)
                # coverage = self.geo_server.catalog.get_resources(store=coverage_store, workspace='lfmc')
                # logger.debug(coverage)
                # for r in coverage:
                #     logger.debug('Coverage Resource: ', r.title)

        except FailedRequestError as e:
            logger.debug(e)

        for m in self.models:
            # Ensure there's a layer for each NetCDF file held in the DataSources
            # i.e., a one-to-one matching between products and published layers.
            # Some products not yet implemented.
            lg = None
            try:
                lg = self.geo_server.get_layer_group(
                    m.code)  # NB Not m.name, it's m.code!
            except FailedRequestError as e:
                logger.debug(e)

            unpublished = []

            if lg is not None:
                good_model = '\nModel: %s is published under LayerGroup: %s' % (
                    m, lg)
                this_model = {'validation': good_model}
                logger.debug(this_model)

                indexed = m.all_netcdfs()

                logger.debug('Found %d NetCDFs for this model.' % len(indexed))
                logger.debug(indexed)

                for i in indexed:
                    name_part = i.split('/')[-1].replace('.nc', '')
                    if name_part not in published_ncs:
                        logger.debug(
                            'Not found in GeoServer Catalog: %s' % name_part)
                        unpublished.append(i)

                        # self.do_work(name_part, "lfmc", i)

                logger.debug('%d of %s are published.' %
                             (len(lg.layers), len(unpublished)))

                if len(unpublished) > 0:
                    this_model['unpublished'] = unpublished

                if lg.layers:
                    this_model['layers'] = lg.layers
                    for lgl in lg.layers:
                        this_layer = self.geo_server.catalog.get_layer(
                            name=lgl)
                        logger.debug(this_layer.name)

                validation.append(this_model)

        logger.debug('Done checking for layer groups in GeoServer.')
        return validation, errors

    def do_work(self, name, workspace, path):
        """
        GSCONFIG - Python bindings are also available from gsconfig.py, but
        there is no implementation of working with NetCDF files yet! So we are
        using Requests to do the actual ingestion.

        Adding a NetCDF coverage store
        At the moment of writing, the NetCDF plugin supports datasets where each
        variable’s axis is identified by an independent coordinate variable,
        therefore two dimensional non-independent latitude-longitude coordinate
        variables aren’t currently supported.

        """
        headers_json = {'content-type': 'application/json'}
        headers_xml = {'content-type': 'text/xml'}
        headers_zip = {'content-type': 'application/zip'}
        headers_sld = {'content-type': 'application/vnd.ogc.sld+xml'}

        # COVERAGE STORES
        # http://geoserver.landscapefuelmoisture.bushfirebehaviour.net.au/geoserver/rest/workspaces/lfmc/coveragestores

        r_create_coveragestore = requests.post(
            'http://geoserver:8080/geoserver/rest/workspaces/lfmc/coveragestores?configure=all',
            auth=('admin', 'geoserver'),
            data='<coverageStore><name>' + name + '</name><workspace>' + workspace +
                 '</workspace><enabled>true</enabled><type>NetCDF</type><url>' +
            path + '</url></coverageStore>',
            headers=headers_xml)
        #
        # if r_create_coveragestore.status_code == '200':

        # LAYER GROUPS
        # http://geoserver.landscapefuelmoisture.bushfirebehaviour.net.au/geoserver/rest/workspaces/lfmc/layergroups

        # LAYERS
        # http://geoserver.landscapefuelmoisture.bushfirebehaviour.net.au/geoserver/rest/layers

        # Add a layer for the coverage
        # See: http://docs.geoserver.org/latest/en/api/#/1.0.0/gwclayers.yaml
        # r_create_layer = requests.put(
        #     'http://geoserver:8080/geoserver/rest/workspaces/lfmc/coveragestores/{}/coverages/{}'.format(coverageStore, coverage),
        #     auth=('admin', 'geoserver'),
        #     data=,
        #     headers=headers_xml)
        # )

        # Add that layer to the LayerGroup

        #     def add_coverage
        #
        #
        #
        #
        #
        #     def make_layer_xml(self):
        #         return "<?xml version="1.0" encoding="UTF-8"?> \
        # <layer> \
        #     <id>string</id> \
        #     <enabled>true</enabled> \
        #     <inMemoryCached>true</inMemoryCached> \
        #     <name>string</name> \
        #     <mimeFormats>string</mimeFormats> \
        #     <gridSubsets> \
        #         <gridSubset> \
        #             <gridSetName>string</gridSetName> \
        #             <extent> \
        #                 <bounds>0</bounds> \
        #             </extent> \
        #             <zoomStart>0</zoomStart> \
        #             <zoomStop>0</zoomStop> \
        #         </gridSubset> \
        #     </gridSubsets> \
        #     <metaWidthHeight>0</metaWidthHeight> \
        #     <expireCache>0</expireCache> \
        #     <expireClients>0</expireClients> \
        #     <parameterFilters> \
        #     </parameterFilters> \
        #     <gutter>0</gutter> \
        # </layer>"

    def register_new_model(self, new_model: Model):
        self.models.append(new_model)

    def get_model_names(self):
        return [m.name for m in self.models]

    def get_model_ids(self):
        return [m.ident for m in self.models]

    def get_model_codes(self):
        return [m.code for m in self.models]

    def get(self, model_name):
        for m in self.models:
            if m.name == model_name:
                return m
            if m.code == model_name:
                return m
            if m.ident == model_name:
                return m
        return None

    def subscribe(self, observer):
        if dev.DEBUG:
            logger.debug("Got subscription. Building response.")

            for model in self.models:
                dps = []
                logger.debug(
                    'Building dummy response for model: %s' % model.name)
                for j in range(30):
                    # dps.append(DummyResults.dummy_single(j))
                    observer.on_next(ModelResult(
                        model_name=model.name, data_points=dps))
            observer.on_completed()
        else:
            dps = []
            for model in self.models:
                model.subscribe(observer)

            # rx.Observable.merge()
        pass


class ModelsRegisterSchema(Schema):
    models = fields.Nested(ModelSchema, many=True)
