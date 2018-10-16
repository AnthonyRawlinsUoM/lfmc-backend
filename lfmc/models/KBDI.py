import asyncio

import os
import os.path
import glob
from pathlib import Path

from lfmc.models.BomBasedModel import BomBasedModel
from lfmc.query import ShapeQuery
from lfmc.results import ModelResult
from lfmc.results.Abstracts import Abstracts
from lfmc.results.Author import Author
import datetime as dt
from lfmc.models.Model import Model
from lfmc.models.ModelMetaData import ModelMetaData


KBDI_PRODUCT = 'IDV71147_VIC_KBDI_SFC.nc'
import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.debug("logger set to DEBUG")


class KBDIModel(BomBasedModel):

    def __init__(self):
        self.name = "kbdi"

        # TODO - Proper metadata!
        authors = [
            Author(name="Keetch", email="",
                   organisation=""),
            Author(name="Byram", email="",
                   organisation="")
        ]
        pub_date = dt.datetime(2015, 9, 9)
        abstract = Abstracts("")
        self.metadata = ModelMetaData(authors=authors, published_date=pub_date, fuel_types=["surface"],
                                      doi="http://dx.doi.org/10.1016/j.rse.2015.12.010", abstract=abstract)

        self.ident = "Keetch-Byram Drought"
        self.code = "KBDI"
        self.path = os.path.abspath(Model.path() + 'KBDI') + '/'
        self.crs = "EPSG:3111"
        self.outputs = {
            "type": "index",
            "readings": {
                "path": self.path,
                "url": "",
                "prefix": "KBDI_SFC",
                "suffix": ".nc"
            }
        }

    def netcdf_name_for_date(self, when):
        return self.netcdf_names_for_date(when, KBDI_PRODUCT)
