import asyncio
import glob
import os
import os.path
from pathlib import Path
import numpy as np
import xarray as xr
import serve.lfmc.config.debug as dev
from serve.lfmc.query import ShapeQuery
from serve.lfmc.query.GeoQuery import GeoQuery
from serve.lfmc.results.Abstracts import Abstracts
from serve.lfmc.results.Author import Author
import datetime as dt
from serve.lfmc.models.Model import Model
from serve.lfmc.results.DataPoint import DataPoint
from serve.lfmc.results.ModelResult import ModelResult
from serve.lfmc.models.ModelMetaData import ModelMetaData
from serve.lfmc.query.SpatioTemporalQuery import SpatioTemporalQuery

import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.debug("logger set to DEBUG")


class AWRAModelUpper(Model):

    def __init__(self):

        # TODO - Proper metadata!
        authors = [
            Author(name="BOM", email="test1@example.com",
                   organisation="Bureau of Meteorology, Australia")
        ]
        pub_date = dt.datetime(2015, 9, 9)
        abstract = Abstracts("The information presented on the Australian Landscape Water Balance website is produced by \
         the Bureau's operational Australian Water Resources Assessment Landscape model (AWRA-L). AWRA-L is a daily 0.05°\
          grid-based, distributed water balance model, conceptualised as a small unimpaired catchment. It simulates the\
           flow of water through the landscape from the rainfall entering the grid cell through the vegetation and soil\
            moisture stores and then out of the grid cell through evapotranspiration, runoff or deep drainage to the groundwater.\n \
        Each spatial unit (grid cell) in AWRA-L is divided into two hydrological response units (HRU) representing deep \
        rooted vegetation (trees) and shallow rooted vegetation (grass). Hydrological processes are modelled separately \
        for each HRU, then the resulting fluxes or stores are combined to give cell outputs. Hydrologically, these two \
        HRUs differ in their aerodynamic control of evaporation and their interception capacities but the main difference\
         is in their degree of access to different soil layers. The AWRA-L model has three soil layers (upper: 0–10 cm, \
         lower: 10–100 cm, and deep: 1–6 m). The shallow rooted vegetation has access to subsurface soil moisture in the \
         upper and lower soil stores only, while the deep rooted vegetation also has access to moisture in the deep store.\
         Upper Soil Moisture estimate represents the percentage of available water content in the top 10 cm of the soil \
         profile. The maximum storage within the soil layer is calculated from the depth of the soil and the relative \
         soil water storage capacity . The soil properties that control the storage of water are derived from the \
         continental scale mapping within Australian Soil Resources Information System (ASRIS) (Johnston et al., 2003).\
         Pedotransfer functions are used to relate soil hydraulic properties to soil textural class. Soil drainage and \
         moisture dynamics are then based on water balance considerations for each soil layer. This upper soil layer is\
         the primary source of soil evaporation. The relative available water capacity of the upper soil layer is derived \
         from ASRIS information as the available water capacity of a layer divided by its thickness. Actual soil moisture \
         grids estimate the percentage of available water content rather than total soil water volume. Relative soil moisture \
         grids, like the other grids, represent the long term deciles")

        self.metadata = ModelMetaData(authors=authors, published_date=pub_date, fuel_types=["surface"],
                                      doi="http://dx.doi.org/10.1016/j.rse.2015.12.010", abstract=abstract)

        self.path = os.path.abspath(Model.path() + 'AWRA-L') + '/'
        self.ident = "Australian Landscape Water Balance (Upper Zone)"
        self.name = "AWRA-L-U"
        self.code = "AWRA_UPPER"
        self.outputs = {
            "type": "soil moisture",
            "readings": {
                "path": self.path,
                "url": "",
                "prefix": "s0_pct",
                "suffix": ".nc"
            }
        }

    def netcdf_name_for_date(self, when):
        return self.path + "{}_{}_Actual_day.nc".format(self.outputs['readings']['prefix'], when.strftime("%Y"))

    def all_netcdfs(self):
        """
        Pattern matches potential paths where files could be stored to those that actually exist.
        Warning: Files outside this directory aren't indexed and won't get ingested.
        : param fname:
        : return:
        """
        possibles = [p for p in glob.glob(
            self.path + self.outputs['readings']['prefix'] + "_*_Actual_day.nc")]
        return [f for f in possibles if Path(f).is_file()]

    # ShapeQuery
    async def get_shaped_resultcube(self, shape_query: ShapeQuery) -> xr.DataArray:
        fs = list(set([self.netcdf_name_for_date(when)
                       for when in shape_query.temporal.dates()]))
        ts = xr.open_mfdataset(fs, chunks={'time': 1})

        ts = ts.sel(time=slice(shape_query.temporal.start.strftime("%Y-%m-%d"),
                               shape_query.temporal.finish.strftime("%Y-%m-%d")))

        return ts

    async def get_shaped_timeseries(self, query: ShapeQuery) -> ModelResult:
        print(
            "\n--->>> Shape Query Called successfully on %s Model!! <<<---" % self.name)
        sr = await (self.get_shaped_resultcube(query))
        sr.load()
        var = self.outputs['readings']['prefix']
        dps = []
        try:
            print('Trying to find datapoints.')

            geoQ = GeoQuery(query)
            dps = geoQ.cast_fishnet({'init': 'EPSG:3111'}, sr[var])
            logger.debug(dps)
        except FileNotFoundError:
            print('Files not found for date range.')
        except ValueError as ve:
            print(ve)
        except OSError as oe:
            print(oe)

        if len(dps) == 0:
            print('Found no datapoints.')
            print(sr)

        return ModelResult(model_name=self.name, data_points=dps)
