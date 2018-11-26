import glob
import os
import os.path
from pathlib import Path

from lfmc.util import compression as util
from lfmc.models.BomBasedModel import BomBasedModel
from lfmc.results.Abstracts import Abstracts
from lfmc.results.Author import Author
import datetime as dt
from lfmc.models.Model import Model
from lfmc.models.ModelMetaData import ModelMetaData


DF_PRODUCT = 'IDV71127_VIC_DF_SFC.nc'
import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.debug("logger set to DEBUG")


class DFModel(BomBasedModel):

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

        self.path = os.path.abspath(Model.path() + 'DF') + '/'
        self.ident = "Drought Factor"
        self.code = "DF"
        self.outputs = {
            "type": "index",
            "readings": {
                "path": self.path,
                "url": "",
                "prefix": "DF_SFC",
                "suffix": ".nc"
            }
        }

    def all_netcdfs(self):
        gzs = glob.glob(
            Model.path() + "Weather/*/{}.gz".format(DF_PRODUCT))
        util.expand_in_place([g for g in gzs if Path(g).is_file()])

        ncs = glob.glob(Model.path() + "Weather/*/{}".format(DF_PRODUCT))
        return super().all_netcdfs() + [p for p in ncs if Path(p).is_file()]

    def netcdf_name_for_date(self, when):
        return self.netcdf_names_for_date(when, DF_PRODUCT)
