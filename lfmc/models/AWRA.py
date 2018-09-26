import asyncio
import glob
import os
import os.path
from pathlib import Path
import numpy as np
import xarray as xr
import lfmc.config.debug as dev
from lfmc.query import ShapeQuery
from lfmc.results.Abstracts import Abstracts
from lfmc.results.Author import Author
import datetime as dt
from lfmc.models.Model import Model
from lfmc.results.DataPoint import DataPoint
from lfmc.results.ModelResult import ModelResult
from lfmc.models.ModelMetaData import ModelMetaData
from lfmc.query.SpatioTemporalQuery import SpatioTemporalQuery
from lfmc.models.dummy_results import DummyResults


class AWRAModel(Model):

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
         upper and lower soil stores only, while the deep rooted vegetation also has access to moisture in the deep store.")

        self.metadata = ModelMetaData(authors=authors, published_date=pub_date, fuel_types=["surface"],
                                      doi="http://dx.doi.org/10.1016/j.rse.2015.12.010", abstract=abstract)

        self.path = os.path.abspath(Model.path() + 'AWRA-L') + '/'
        self.ident = "Australian Landscape Water Balance"
        self.name = "AWRA-L"
        self.code = "AWRA"
        self.outputs = {
            "type": "soil moisture",
            "readings": {
                "path": self.path,
                "url": "",
                "prefix": "sm_pct",
                "suffix": ".nc"
            }
        }

    def netcdf_name_for_date(self, when):
        return self.path + "{}_{}_Actual_day.nc".format(self.outputs['readings']['prefix'], when.strftime("%Y"))

    def all_netcdfs(self):
        """
        Pattern matches potential paths where files could be stored to those that actually exist.
        Warning: Files outside this directory aren't indexed and won't get ingested.
        :param fname:
        :return:
        """
        possibles = [p for p in glob.glob(
            self.path + "sm_pct_*_Actual_day.nc")]
        return [f for f in possibles if Path(f).is_file()]

    # ShapeQuery
    async def get_shaped_resultcube(self, shape_query: ShapeQuery) -> xr.DataArray:
        fs = list(set([self.netcdf_name_for_date(when)
                       for when in shape_query.temporal.dates()]))
        ts = xr.open_mfdataset(fs, chunks={'time': 1})

        asyncio.sleep(1)
        ts = ts.sel(time=slice(shape_query.temporal.start.strftime("%Y-%m-%d"),
                               shape_query.temporal.finish.strftime("%Y-%m-%d")))

        # if dev.DEBUG:
        #     print(ts)
        return shape_query.apply_mask_to(ts)

    async def get_shaped_timeseries(self, query: ShapeQuery) -> ModelResult:
        print(
            "\n--->>> Shape Query Called successfully on %s Model!! <<<---" % self.name)
        sr, weights = await (self.get_shaped_resultcube(query))
        sr.load()
        var = self.outputs['readings']['prefix']
        dps = []
        try:
            print('Trying to find datapoints.')

            for t in sorted(sr['time'].values):

                d = sr[var].sel(time=t).to_dataframe()
                df = d[var]

                # cleaned_mask = np.ma.masked_array(weights, np.isnan(weights))
                # cleaned = np.ma.masked_array(df, np.isnan(df))

                #wm = np.ma.average(cleaned, axis=1, weights=cleaned_mask)
                wm = -99999

                dps.append(DataPoint(observation_time=str(t).replace('.000000000', '.000Z'),
                                     value=np.nanmedian(df),
                                     mean=np.nanmean(df),
                                     weighted_mean=wm,
                                     minimum=np.nanmin(df),
                                     maximum=np.nanmax(df),
                                     deviation=np.nanstd(df),
                                     count=df.count()))
        except FileNotFoundError:
            print('Files not found for date range.')
        except ValueError as ve:
            print(ve)
        except OSError as oe:
            print(oe)

        if len(dps) == 0:
            print('Found no datapoints.')
            print(sr)
            print(weights)

        asyncio.sleep(1)

        return ModelResult(model_name=self.name, data_points=dps)
