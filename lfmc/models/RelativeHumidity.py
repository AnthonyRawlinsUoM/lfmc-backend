import asyncio
import glob
import os
import os.path
from pathlib import Path

from lfmc.models.BomBasedModel import BomBasedModel
from lfmc.results.Abstracts import Abstracts
from lfmc.results.Author import Author
import datetime as dt
from lfmc.models.Model import Model
from lfmc.models.ModelMetaData import ModelMetaData

RH_PRODUCT = 'IDV71018_VIC_RH_SFC.nc'


class RHModel(BomBasedModel):

    def __init__(self):
        self.name = "humidity"

        # TODO - Proper metadata!
        authors = [
            Author(name="", email="",
                   organisation="")
        ]
        pub_date = dt.datetime(2015, 9, 9)
        abstract = Abstracts("")
        self.metadata = ModelMetaData(authors=authors, published_date=pub_date, fuel_types=["surface"],
                                      doi="http://dx.doi.org/10.1016/j.rse.2015.12.010", abstract=abstract)
        self.ident = "Relative Humidity"
        self.code = "RH"
        self.path = os.path.abspath(Model.path() + 'Weather') + '/'
        self.crs = "EPSG:4326"
        self.outputs = {
            "type": "index",
            "readings": {
                "path": self.path,
                "url": "",
                "prefix": "RH_SFC",
                "suffix": ".nc"
            }
        }

    def netcdf_name_for_date(self, when):
        return self.netcdf_names_for_date(when, RH_PRODUCT)
