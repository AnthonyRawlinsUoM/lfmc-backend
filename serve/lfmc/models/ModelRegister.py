from marshmallow import fields, Schema

import logging

from marshmallow import fields, Schema

from serve.lfmc.library.GeoServer import GeoServer
from serve.lfmc.models.AWRAlower import AWRAModelLower
from serve.lfmc.models.AWRAroot import AWRAModelRoot
from serve.lfmc.models.AWRAupper import AWRAModelUpper
from serve.lfmc.models.Curing import CuringModel
from serve.lfmc.models.DF import DFModel
from serve.lfmc.models.DeadFuel import DeadFuelModel
from serve.lfmc.models.FFDI import FFDIModel
from serve.lfmc.models.GFDI import GFDIModel
from serve.lfmc.models.JASMIN import JasminModel
from serve.lfmc.models.KBDI import KBDIModel
from serve.lfmc.models.LiveFuel import LiveFuelModel
from serve.lfmc.models.Model import ModelSchema
from serve.lfmc.models.Yebra import YebraModel

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.debug("logger set to DEBUG")


class ModelRegister:

    def __init__(self):
        # self.models = [
        #     ModelAdaptor(DeadFuelModel()),
        #     ModelAdaptor(LiveFuelModel()),
        #     ModelAdaptor(FFDIModel()),
        #     ModelAdaptor(GFDIModel()),
        #     ModelAdaptor(KBDIModel()),
        #     ModelAdaptor(AWRAModelRoot()),
        #     ModelAdaptor(AWRAModelLower()),
        #     ModelAdaptor(AWRAModelUpper()),
        #     ModelAdaptor(CuringModel()),
        #     ModelAdaptor(JasminModel()),
        #     ModelAdaptor(DFModel()),
        #     ModelAdaptor(YebraModel())
        # ]

        self.models = [
            DeadFuelModel(),
            LiveFuelModel(),
            FFDIModel(),
            GFDIModel(),
            KBDIModel(),
            AWRAModelRoot(),
            AWRAModelLower(),
            AWRAModelUpper(),
            CuringModel(),
            JasminModel(),
            DFModel(),
            YebraModel()
        ]

        self.geo_server = GeoServer()

    # @staticmethod
    # def validate_catalog():
    #     validation = []
    #     errors = None
    #     published_ncs = []
    #
    #     try:
    #
    #         stores = ModelRegister.geo_server.catalog.get_stores(
    #             workspace='lfmc')
    #         logger.debug(stores)
    #
    #         for coverage_store in stores:
    #             published_ncs.append(coverage_store.name)
    #             logger.debug(coverage_store.name)
    #             # coverage = self.geo_server.catalog.get_resources(store=coverage_store, workspace='lfmc')
    #             # logger.debug(coverage)
    #             # for r in coverage:
    #             #     logger.debug('Coverage Resource: ', r.title)
    #
    #     except FailedRequestError as e:
    #         logger.debug(e)
    #
    #     for m in ModelRegister.models:
    #         # Ensure there's a layer for each NetCDF file held in the DataSources
    #         # i.e., a one-to-one matching between products and published layers.
    #         # Some products not yet implemented.
    #         lg = None
    #         try:
    #             lg = ModelRegister.geo_server.get_layer_group(
    #                 m.code)  # NB Not m.name, it's m.code!
    #         except FailedRequestError as e:
    #             logger.debug(e)
    #
    #         unpublished = []
    #
    #         if lg is not None:
    #             good_model = '\nModel: %s is published under LayerGroup: %s' % (
    #                 m, lg)
    #             this_model = {'validation': good_model}
    #             logger.debug(this_model)
    #
    #             indexed = m.all_netcdfs()
    #
    #             logger.debug('Found %d NetCDFs for this model.' % len(indexed))
    #             # [logger.debug('>> ' + nf.split('/')[-1]) for nf in indexed]
    #
    #             for i in indexed:
    #                 # Partial Weather files...
    #                 parts = i.split('/')
    #                 logger.debug(parts[2])
    #                 name_part = parts[-1].replace('.nc', '')
    #                 if parts[2] == 'Weather':
    #                     logger.warning(
    #                         'Cannot load partial archives from Weather! A whole year archive is required.')
    #                     # Create the Layer name from the BOM naming conventions and date from the path
    #
    #                 # IDN for NSW ??? might break later... potential bug
    #                 elif not name_part[:3] == 'IDV':
    #                     if name_part not in published_ncs:
    #                         logger.debug(
    #                             'Not found in GeoServer Catalog: %s' % name_part)
    #                         try:
    #                             ModelRegister.geo_server.add_to_catalog(
    #                                 lg, parts[-1], i)
    #                         except:
    #                             unpublished.append(i)
    #
    #             logger.debug('%d of %s were published.' %
    #                          (len(lg.layers), len(indexed)))
    #
    #             if len(unpublished) > 0:
    #                 this_model['unpublished'] = unpublished
    #
    #             if lg.layers:
    #                 this_model['layers'] = lg.layers
    #                 for lgl in lg.layers:
    #                     this_layer = ModelRegister.geo_server.catalog.get_layer(
    #                         name=lgl)
    #                     logger.debug(this_layer.name)
    #
    #             validation.append(this_model)
    #
    #     logger.debug('Done checking for layer groups in GeoServer.')
    #     return validation, errors

    # def register_new_model(self, new_model: Model):
    #     self.models.append(new_model)
    #
    def get_model_names(self):
        return [m.name for m in self.models]

    def get_model_ids(self):
        return [m.ident for m in self.models]

    def get_model_codes(self):
        return [m.code for m in self.models]

    def get(self, model_name):
        logger.debug(model_name)
        for m in self.models:
            if str(m.name).upper() == str(model_name).upper():
                return m
            if str(m.code).upper() == str(model_name).upper():
                return m
            if str(m.ident).upper() == str(model_name).upper():
                return m
        return None


class ModelsRegisterSchema(Schema):
    models = fields.Nested(ModelSchema, many=True)
