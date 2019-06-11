import asyncio
import glob
import os
import os.path
from pathlib import Path

from serve.lfmc.util import compression as util
from serve.lfmc.models.BomBasedModel import BomBasedModel
from serve.lfmc.results.Abstracts import Abstracts
from serve.lfmc.results.Author import Author
import datetime as dt
from serve.lfmc.models.Model import Model
from serve.lfmc.models.ModelMetaData import ModelMetaData


TEMP_PRODUCT = 'IDV71002_VIC_MaxT_SFC.nc'
import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.debug("logger set to DEBUG")


class TempModel(BomBasedModel):

    def __init__(self):
        self.name = "TMAX"

        # TODO - Proper metadata!
        authors = [
            Author(name="", email="",
                   organisation="")
        ]
        pub_date = dt.datetime(2015, 9, 9)
        abstract = Abstracts("")
        self.metadata = ModelMetaData(authors=authors, published_date=pub_date, fuel_types=["surface"],
                                      doi="http://dx.doi.org/10.1016/j.rse.2015.12.010", abstract=abstract)
        self.ident = "Temperature"
        self.code = "Tmax"
        self.path = os.path.abspath(Model.path() + 'Weather') + '/'
        self.crs = "EPSG:4326"
        self.outputs = {
            "type": "index",
            "readings": {
                "path": self.path,
                "url": "",
                "prefix": "MaxT_SFC",
                "suffix": ".nc"
            }
        }

    def all_netcdfs(self):
        gzs = glob.glob(
            Model.path() + "Weather/*/{}.gz".format(TEMP_PRODUCT))
        util.expand_in_place([g for g in gzs if Path(g).is_file()])

        ncs = glob.glob(
            Model.path() + "Weather/*/{}".format(TEMP_PRODUCT))
        return super().all_netcdfs() + [p for p in ncs if Path(p).is_file()]

    def netcdf_name_for_date(self, when):
        return self.netcdf_names_for_date(when, TEMP_PRODUCT)
