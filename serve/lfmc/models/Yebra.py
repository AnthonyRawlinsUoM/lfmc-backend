import asyncio
import glob
import os
import os.path
from pathlib import Path

import xarray as xr
import numpy as np

from serve.lfmc.query import ShapeQuery
from serve.lfmc.query.GeoQuery import GeoQuery
from serve.lfmc.results.MPEGFormatter import MPEGFormatter
from serve.lfmc.results.Abstracts import Abstracts
from serve.lfmc.results.Author import Author
import datetime as dt
from serve.lfmc.models.Model import Model
from serve.lfmc.results.ModelResult import ModelResult
from serve.lfmc.models.ModelMetaData import ModelMetaData
from serve.lfmc.results.DataPoint import DataPoint
from serve.lfmc.query.SpatioTemporalQuery import SpatioTemporalQuery

import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.debug("logger set to DEBUG")


class YebraModel(Model):

    def __init__(self):

        self.name = "yebra"

        authors = [
            Author(name="Marta Yebra", email="marta.yebra@anu.edu.au",
                   organisation="Fenner School of Environment and Society, ANU, BNHCRC"),
            Author(name="Xingwen Quan", email="", organisation="School of Resources and Environment, \
            University of Electronic Science and Technology of China"),
            Author(name="David Riaño", email="",
                   organisation="Center for Spatial Technologies and Remote Sensing (CSTARS)"),
            Author(name="Pablo Rozas Larraondo", email="",
                   organisation="National Computational Infrastructure"),
            Author(name="Albert I.J.M. van Dijk", email="",
                   organisation="Fenner School of Environment and Society, ANU, BNHCRC")
        ]

        pub_date = dt.datetime(2018, 6, 1)

        abstract = Abstracts("Fuel Moisture Content (FMC) is one of the primary drivers affecting fuel flammability that lead to fires. Satellite \
observations well-grounded with field data over the highly climatologically and ecologically diverse Australian \
region served to estimate FMC and flammability for the first time at a continental-scale. The methodology \
includes a physically-based retrieval model to estimate FMC from MODIS (Moderate Resolution Imaging \
Spectrometer) reflectance data using radiative transfer model inversion. The algorithm was evaluated using 360 \
observations at 32 locations around Australia with mean accuracy for the studied land cover classes (grassland, \
shrubland, and forest) close to those obtained elsewhere (r 2 = 0.58, RMSE = 40%) but without site-specific \
calibration. Logistic regression models were developed to generate a flammability index, trained on fire events \
mapped in the MODIS burned area product and four predictor variables calculated from the FMC estimates. The \
selected predictor variables were actual FMC corresponding to the 8-day and 16-day period before burning; the \
same but expressed as an anomaly from the long-term mean for that date; and the FMC change between the two \
successive 8-day periods before burning. Separate logistic regression models were developed for grassland, \
shrubland and forest. The models obtained an “Area Under the Curve” calculated from the Receiver Operating \
Characteristic plot method of 0.70, 0.78 and 0.71, respectively, indicating reasonable skill in fire risk prediction.")

        self.metadata = ModelMetaData(authors=authors,
                                      published_date=pub_date,
                                      fuel_types=["profile"],
                                      doi="http://dx.doi.org/10.1016/j.rse.2018.04.053",
                                      abstract=abstract)

        self.mode = "wet"  # "wet" or "dry"
        self.ident = "Yebra"
        self.code = "LVMC"
        self.path = os.path.abspath(Model.path() + 'Yebra') + '/'
        self.output_path = os.path.abspath(self.path + "c6") + '/'
        self.data_path = self.output_path

        # Metadata about initialisation for use in ModelSchema
        self.parameters = {}

        self.outputs = {
            "type": "fuel moisture",
            "readings": {
                "prefix": "fmc_mean",
                "path": self.output_path,
                "suffix": ".nc"
            }
        }

    def all_netcdfs(self):
        """
        Pattern matches potential paths where files could be stored to those that actually exist.
        Warning: Files outside this directory aren't indexed and won't get ingested.
        :return:
        """
        possibles = [p for p in glob.glob(self.path + "fmc_c6_*.nc")]
        return [f for f in possibles if Path(f).is_file()]

    def netcdf_name_for_date(self, when):
        fname = self.output_path + \
            "fmc_c6_{}.nc".format(when.strftime("%Y"))
        logger.debug(fname)
        return fname

    async def dataset_files(self, when):
        return self.netcdf_name_for_date(when)

    async def get_netcdf(self, query: ShapeQuery):
        sr = await (self.get_shaped_resultcube(query))
        return sr

    async def mpg(self, query: ShapeQuery):
        sr = await (self.get_shaped_resultcube(query))
        logger.debug(sr)
        mp4 = await (MPEGFormatter.format(sr, "fmc_mean"))
        asyncio.sleep(1)
        return mp4

    # ShapeQuery
    async def get_shaped_resultcube(self, shape_query: ShapeQuery) -> xr.DataArray:
        fs = set()
        ps = await asyncio.gather(*[self.dataset_files(when) for when in shape_query.temporal.dates()])
        [fs.add(f) for f in ps if (f is not None and Path(f).is_file())]

        [logger.debug("Confirmed: %s" % f) for f in fs]

        if len(fs) == 1:
            with xr.open_dataset(*fs) as ds:
                ds = xr.decode_cf(ds)
                ds.attrs['var_name'] = "fmc_mean"
                tr = ds.sel(time=slice(shape_query.temporal.start.strftime("%Y-%m-%d"),
                                       shape_query.temporal.finish.strftime("%Y-%m-%d")))
                return tr

        elif len(fs) > 1:
            fs = list(set(fs))
            with xr.open_mfdataset(*fs) as ds:
                ds = xr.decode_cf(ds)
                ds.attrs['var_name'] = "fmc_mean"
                ts = ds.sel(time=slice(shape_query.temporal.start.strftime("%Y-%m-%d"),
                                       shape_query.temporal.finish.strftime("%Y-%m-%d")))

                return ts
        else:
            logger.debug("No files available/gathered for that space/time.")

            return xr.DataArray([])

    # async def get_shaped_timeseries(self, query: ShapeQuery) -> ModelResult:
    #     logger.debug(
    #         "\n--->>> Shape Query Called successfully on %s Model!! <<<---" % self.name)
    #     sr = await (self.get_shaped_resultcube(query))
    #     sr.load()
    #     var = self.outputs['readings']['prefix']
    #     dps = []
    #     try:
    #         logger.debug('Trying to find datapoints.')
    #
    #         geoQ = GeoQuery(query)
    #         dps = geoQ.cast_fishnet({'init': 'EPSG:3577'}, sr[var])
    #         logger.debug(dps)
    #
    #     except FileNotFoundError:
    #         logger.debug('Files not found for date range.')
    #     except ValueError as ve:
    #         logger.debug(ve)
    #     except OSError as oe:
    #         logger.debug(oe)
    #
    #     if len(dps) == 0:
    #         logger.debug('Found no datapoints.')
    #         logger.debug(sr)
    #
    #     asyncio.sleep(1)
    #
    #     return ModelResult(model_name=self.name, data_points=dps)
