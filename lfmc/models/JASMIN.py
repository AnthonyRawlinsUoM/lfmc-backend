import asyncio
import os
import os.path
import pandas as pd
import glob
import lfmc.config.debug as dev
from pathlib2 import Path
import xarray as xr
import numpy as np
from lfmc.query.ShapeQuery import ShapeQuery
from lfmc.query.GeoQuery import GeoQuery
from lfmc.results.DataPoint import DataPoint
from lfmc.results.ModelResult import ModelResult
from lfmc.results.Abstracts import Abstracts
from lfmc.results.Author import Author
import datetime as dt
from lfmc.models.Model import Model
from lfmc.models.ModelMetaData import ModelMetaData
import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.debug("logger set to DEBUG")


class JasminModel(Model):

    def __init__(self):
        self.name = "jasmin"

        # TODO - Proper metadata!
        authors = [
            Author(name="Imtiaz Dharssi", email="",
                   organisation="Bureau of Meteorology, Australia"),
            Author(name="Vinodkumar", email="",
                   organisation="Bureau of Meteorology, Australia")
        ]
        pub_date = dt.datetime(2017, 10, 1)
        abstract = Abstracts("Accurate soil dryness information is essential for the calculation of accurate fire danger \
                ratings, fire behavior prediction, flood forecasting and landslip warnings. Soil dryness \
                also strongly influences temperatures and heatwave development by controlling the \
                partitioning of net surface radiation into sensible, latent and ground heat fluxes. Rainfall \
                forecasts are crucial for many applications and many studies suggest that soil dryness \
                can significantly influence rainfall. Currently, soil dryness for fire danger prediction in \
                Australia is estimated using very simple water balance models developed in the 1960s \
                that ignore many important factors such as incident solar radiation, soil types, vegeta- \
                tion height and root depth. This work presents a prototype high resolution soil moisture \
                analysis system based around the Joint UK Land Environment System (JULES) land \
                surface model. This prototype system is called the JULES based Australian Soil Mois- \
                ture INformation (JASMIN) system. The JASMIN system can include data from many \
                sources; such as surface observations of rainfall, temperature, dew-point temperature, \
                wind speed, surface pressure as well as satellite derived measurements of rainfall, sur- \
                face soil moisture, downward surface short-wave radiation, skin temperature, leaf area \
                index and tree heights. The JASMIN system estimates soil moisture on four soil layers \
                over the top 3 meters of soil, the surface layer has a thickness of 10 cm. The system \
                takes into account the effect of different vegetation types, root depth, stomatal resis- \
                tance and spatially varying soil texture. The analysis system has a one hour time-step \
                with daily updating. For the surface soil layer, verification against ground based soil \
                moisture observations from the OzNet, CosmOz and OzFlux networks shows that the \
                JASMIN system is significantly more accurate than other soil moisture analysis sys- \
                tem used at the Bureau of Meteorology. For the root-zone, the JASMIN system has \
                similar skill to other commonly used soil moisture analysis systems. The Extended \
                Triple Collocation (ETC) verification method also confirms the high skill of the JASMIN \
                system.")
        self.metadata = ModelMetaData(authors=authors, published_date=pub_date, fuel_types=["surface"],
                                      doi="http://dx.doi.org/10.1016/j.rse.2015.12.010", abstract=abstract)

        self.path = os.path.abspath(Model.path() + 'JASMIN') + '/'
        self.ident = "JASMIN"
        self.code = "JASMIN"
        self.outputs = {
            "type": "index",
            "readings": {
                "path": self.path,
                "url": "",
                "prefix": "smd",
                "suffix": ".nc"
            }
        }

    def netcdf_names_for_dates(self, start, finish):
        # Because some of the data is in 7 day observations,
        # we need to pad dates +/- 7 days to ensure we grab the correct nc files that might contain 'when'
        window_begin = start - dt.timedelta(7)
        window_end = finish + dt.timedelta(7)
        cdf_list = []

        for d in pd.date_range(window_begin, window_end):
            cdf_list += [p for p in
                         glob.glob(Model.path() + "JASMIN/rescaled/21vls/jasmin.kbdi/temporal/jasmin.kbdi.cdf_temporal.2lvls.{}.nc".format(d.strftime("%Y")))]

        return [f for f in list(set(cdf_list)) if Path(f).is_file()]

    def all_netcdfs(self):
        """
        Pattern matches potential paths where files could be stored to those that actually exist.
        Warning: Files outside this directory aren't indexed and won't get ingested.
        :param fname:
        :return:
        """
        possibles = [p for p in glob.glob(Model.path(
        ) + "JASMIN/rescaled/21vls/jasmin.kbdi/temporal/jasmin.kbdi.cdf_temporal.2lvls.*.nc")]
        return [f for f in possibles if Path(f).is_file()]

    # ShapeQuery
    async def get_shaped_resultcube(self, shape_query: ShapeQuery) -> xr.DataArray:

        sr = None
        fs = self.netcdf_names_for_dates(
            shape_query.temporal.start, shape_query.temporal.finish)
        if dev.DEBUG:
            logger.debug('{}\n'.format(f) for f in fs)
        asyncio.sleep(1)

        if len(fs) > 0:
            with xr.open_mfdataset(fs, concat_dim='time') as ds:
                if "observations" in ds.dims:
                    sr = ds.squeeze("observations")
                else:
                    sr = ds

            if dev.DEBUG:
                logger.debug(sr)
            sr.attrs['var_name'] = self.outputs['readings']['prefix']
            sr = sr.sel(time=slice(shape_query.temporal.start.strftime("%Y-%m-%d"),
                                   shape_query.temporal.finish.strftime("%Y-%m-%d")))

            return sr
        else:
            return xr.DataArray([])

    async def get_shaped_timeseries(self, query: ShapeQuery) -> ModelResult:
        logger.debug(
            "\n--->>> Shape Query Called successfully on %s Model!! <<<---" % self.name)
        sr = await (self.get_shaped_resultcube(query))
        sr.load()
        var = self.outputs['readings']['prefix']
        dps = []
        try:
            logger.debug('Trying to find datapoints.')
            geoQ = GeoQuery(query)
            dps = geoQ.cast_fishnet({'init': 'EPSG:4326'}, sr[var])
            logger.debug(dps)

        except FileNotFoundError:
            logger.debug('Files not found for date range.')
        except ValueError as ve:
            logger.debug(ve)
        except OSError as oe:
            logger.debug(oe)
        except KeyError as ke:
            logger.debug(ke)

        if len(dps) == 0:
            logger.debug('Found no datapoints.')
            logger.debug(sr)

        asyncio.sleep(1)

        return ModelResult(model_name=self.name, data_points=dps)
