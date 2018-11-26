import asyncio
import os
import os.path
import glob
from pathlib import Path

import pandas as pd

from lfmc.util import compression as util
from lfmc.models.BomBasedModel import BomBasedModel
from lfmc.results.Abstracts import Abstracts
from lfmc.results.Author import Author
import datetime as dt
from lfmc.models.Model import Model
from lfmc.models.ModelMetaData import ModelMetaData


FFDI_PRODUCT = 'IDV71117_VIC_FFDI_SFC.nc'
import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.debug("logger set to DEBUG")


class FFDIModel(BomBasedModel):

    def __init__(self):

        self.name = "ffdi"

        # TODO - Proper metadata!
        authors = [
            Author(name="", email="",
                   organisation="")
        ]
        pub_date = dt.datetime(2015, 9, 9)
        abstract = Abstracts("")
        self.metadata = ModelMetaData(authors=authors, published_date=pub_date, fuel_types=["surface"],
                                      doi="http://dx.doi.org/10.1016/j.rse.2015.12.010", abstract=abstract)

        self.path = os.path.abspath(Model.path() + 'FFDI') + '/'
        self.ident = "Forest Fire Danger"
        self.code = "FFDI"
        self.crs = "EPSG:3111"
        self.outputs = {
            "type": "index",
            "readings": {
                "path": self.path,
                "url": "",
                "prefix": "FFDI_SFC",
                "suffix": ".nc"
            }
        }

    def all_netcdfs(self):
        gzs = glob.glob(
            Model.path() + "Weather/*/{}.gz".format(FFDI_PRODUCT))
        util.expand_in_place([g for g in gzs if Path(g).is_file()])

        ncs = glob.glob(
            Model.path() + "Weather/*/{}".format(FFDI_PRODUCT))
        return super().all_netcdfs() + [p for p in ncs if Path(p).is_file()]

    def netcdf_name_for_date(self, when):
        return self.netcdf_names_for_date(when, FFDI_PRODUCT)
