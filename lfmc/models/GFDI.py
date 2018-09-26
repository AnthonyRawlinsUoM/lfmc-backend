import asyncio
import os
import os.path
import glob
from pathlib import Path

from lfmc.models.BomBasedModel import BomBasedModel
from lfmc.results.Abstracts import Abstracts
from lfmc.results.Author import Author
import datetime as dt
from lfmc.models.Model import Model
from lfmc.models.ModelMetaData import ModelMetaData


GFDI_PRODUCT = "IDV71122_VIC_GFDI_SFC.nc"


class GFDIModel(BomBasedModel):

    def __init__(self):
        self.name = "gfdi"

        # TODO - Proper metadata!
        authors = [
            Author(name="Danielle Martin", email="",
                   organisation="Country Fire Authority"),
            Author(name="Alex Chen", email="",
                   organisation="Country Fire Authority"),
            Author(name="David Nichols", email="",
                   organisation="Country Fire Authority"),
            Author(name="Rachel Bessell", email="",
                   organisation="Country Fire Authority"),
            Author(name="Susan Kiddie", email="",
                   organisation="Country Fire Authority"),
            Author(name="Jude Alexander", email="",
                   organisation="Country Fire Authority")
        ]
        pub_date = dt.datetime(2015, 9, 9)
        abstract = Abstracts("Depending on the growth stage of grass, certain physiological characteristics, such \
                            as water content and degree of curing (senescence), determine the susceptibility of \
                            grass to ignite or to propagate a fire. Grassland curing is an integral component of \
                            the Grassland Fire Danger Index (GFDI), which is used to determine the Fire Danger \
                            Ratings (FDRs). In providing input for the GFDI for the whole state of Victoria, this \
                            paper reports the development of two amalgamated products by the Country Fire \
                            Authority (CFA): (i) an automated web-based system which integrates weekly field \
                            observations with real time satellite data for operational grassland curing mapping, \
                            and (ii) a satellite model based on historical satellite data and historical field \
                            observations. Both products combined will provide an improved state-wide map of \
                            curing tailored for Victorian grasslands.")

        self.metadata = ModelMetaData(authors=authors, published_date=pub_date, fuel_types=["surface"],
                                      doi="http://dx.doi.org/10.1016/j.rse.2015.12.010", abstract=abstract)

        self.ident = "Grass Fire Danger"
        self.code = "GFDI"
        self.path = os.path.abspath(Model.path() + 'GFDI') + '/'
        self.crs = "EPSG:3111"
        self.outputs = {
            "type": "index",
            "readings": {
                "path": self.path,
                "url": "",
                "prefix": "GFDI_SFC",
                "suffix": ".nc"
            }
        }

    def netcdf_name_for_date(self, when):
        return self.netcdf_names_for_date(when, GFDI_PRODUCT)
