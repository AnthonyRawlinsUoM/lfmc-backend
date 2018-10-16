import asyncio
import glob
import os
import os.path
from pathlib import Path

from lfmc.models.BomBasedModel import BomBasedModel
from lfmc.query.ShapeQuery import ShapeQuery
from lfmc.results.Abstracts import Abstracts
from lfmc.results.Author import Author
import datetime as dt
from lfmc.models.Model import Model
from lfmc.models.ModelMetaData import ModelMetaData

from lfmc.results.MPEGFormatter import MPEGFormatter
from lfmc.results.ModelResult import ModelResult

CURING_PRODUCT = 'IDV71139_VIC_Curing_SFC.nc'
import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.debug("logger set to DEBUG")


class CuringModel(BomBasedModel):

    def __init__(self):
        self.name = "df"

        # TODO - Proper metadata!
        authors = [
            Author(name="", email="",
                   organisation="")
        ]
        pub_date = dt.datetime(2015, 9, 9)
        abstract = Abstracts("")
        self.metadata = ModelMetaData(authors=authors, published_date=pub_date, fuel_types=["surface"],
                                      doi="http://dx.doi.org/10.1016/j.rse.2015.12.010", abstract=abstract)

        self.path = os.path.abspath(Model.path() + 'Curing') + '/'
        self.ident = "Grass Curing"
        self.code = "Curing"
        self.outputs = {
            "type": "index",
            "readings": {
                "path": self.path,
                "url": "",
                "prefix": "Curing_SFC",
                "suffix": ".nc"
            }
        }

    def netcdf_name_for_date(self, when):
        return self.netcdf_names_for_date(when, CURING_PRODUCT)
