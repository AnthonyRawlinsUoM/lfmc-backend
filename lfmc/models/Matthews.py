import os
import os.path
import numpy as np

import datetime as dt
import xarray as xr
from pathlib2 import Path
import subprocess
import urllib.request
from urllib.error import URLError
import asyncio

from lfmc.results.Abstracts import Abstracts
from lfmc.results.Author import Author
from lfmc.models.Model import Model
from lfmc.models.ModelMetaData import ModelMetaData
from lfmc.results.DataPoint import DataPoint
from lfmc.results.MPEGFormatter import MPEGFormatter
from lfmc.results.ModelResult import ModelResult
from lfmc.query.ShapeQuery import ShapeQuery
from lfmc.query.SpatioTemporalQuery import SpatioTemporalQuery
from lfmc.models.matthews.broadscale.lfmc_dryness import LfmcDryness
from lfmc.models.matthews.broadscale.post_process import PostProcessor

import datetime as dt

import logging
import math
import matplotlib.pyplot as plt

plt.switch_backend('agg')


class Matthews(Model):

    def __init__(self):

        self.name = "matthews"

        # TODO - Proper metadata!
        authors = [
            Author(name="Stuart Matthews",
                   email="Stuart.Matthews@rfs.nsw.gov.au", organisation="RFS")
        ]

        pub_date = dt.datetime(2015, 6, 1)

        abstract = Abstracts("This paper presents the first complete process-based model for fuel moisture in the litter layer. \
        The model predicts fuel moisture by modelling the energy and water budgets of the litter, intercepted precipitation, \
        and air spaces in the litter. The model was tested against measurements of fuel moisture from two sets of field \
        observations, one made in Eucalyptus mallee-heath under dry conditions and the other during a rainy period in \
        Eucalyptus obliqua forest. The model correctly predicted minimum and maximum fuel moisture content and the \
        timing of minima and maxima in the mallee-heath. Under wet conditions, wetting and drying of the litter profile \
        were correctly predicted but wetting of the surface litter was over-predicted. The structure of the model and the \
        dependence of predictions on model parameters were examined using sensitivity and parameter estimation studies. \
        The results indicated that it should be possible to adapt the model to any forest type by specifying a limited number \
        of parameters. A need for further experimental research on the wetting of litter during rain was also identified.")

        self.metadata = ModelMetaData(authors=authors,
                                      published_date=pub_date,
                                      fuel_types=["profile"],
                                      doi="http://dx.doi.org/10.13140/RG.2.2.36184.70403",
                                      abstract=abstract)

        self.mode = "wet"  # "wet" or "dry"
        self.ident = "Matthews"
        self.code = "PFMC"
        self.path = os.path.abspath(Model.path() + 'Matthews') + '/'
        self.output_path = os.path.abspath(self.path + "PFMC") + '/'
        self.data_path = os.path.abspath(Model.path() + "Weather") + '/'

        self.type = "broadscale"

        # Metadata about initialisation for use in ModelSchema
        self.parameters = {
            "mode": self.mode,
            "path": os.path.normpath("Data/"),  # TODO
            "data_path": self.data_path,     # TODO

            "relative humidity": {
                "var": "RH_SFC",
                "path": self.data_path,
                "prefix": "RH_SFC",
                "suffix": ".nc",
                "dataset": ".nc",
                "compression_suffix": ".gz"
            },
            "temperature": {
                "var": "T_SFC",
                "path": self.data_path,
                "prefix": "T_SFC",
                "suffix": ".nc",
                "dataset": ".nc",
                "compression_suffix": ".gz"
            },
            "wind magnitude": {
                "var": "Wind_Mag_SFC",
                "path": self.data_path,
                "prefix": "Wind_Mag_SFC",
                "suffix": ".nc",
                "dataset": ".nc",
                "compression_suffix": ".gz"
            },
            "precipitation": {
                "var": "DailyPrecip50Pct_SFC",
                "path": self.data_path,
                "prefix": "DailyPrecip50Pct_SFC",
                "suffix": ".nc",
                "dataset": ".nc",
                "compression_suffix": ".gz"
            },
            "solar radiation": {
                "var": "Sky_SFC",
                "path": self.data_path,
                "prefix": "Sky_SFC",
                "suffix": ".nc",
                "dataset": ".nc",
                "compression_suffix": ".gz"
            }
        }

        self.outputs = {
            "type": "fuel moisture",
            "readings": {
                "prefix": "MFMC",
                "path": self.output_path,
                "suffix": ".nc"
            },
            "grid": {
                "fmc_grid_output_file_name": os.path.join(self.data_path, "fmc_grid.pkl")
            }
        }

        self.model = LfmcDryness(self.parameters, self.outputs)

    def set_date(self, when):
        ds = when.strftime('%Y%m%d')
        print("Setting date folder to: %s" % ds)
        self.model.set_netcdf_path(self.data_path + ds)

    async def run_main(self):
        print("Running the Matthews Model NOW!")
        try:
            self.model.set_mode("wet")
            self.model.run_model()
            self.model.set_mode("dry")
            self.model.run_model()
            PostProcessor.run_main()
        except OSError:
            print("Model failed.")
        finally:
            ncf = ""  # TODO

        return ncf

    # ShapeQuery
    async def get_shaped_resultcube(self, shape_query: ShapeQuery) -> xr.DataArray:

        sr = None
        fs = await asyncio.gather(*[self.dataset_files(when) for when in shape_query.temporal.dates()])

        # <-- Check we actually have datafiles
        fs = [pf for pf in fs if Path(pf).is_file()]

        if len(fs) > 0:
            print(fs)
            with xr.open_mfdataset(fs) as ds:
                print(ds)
                if "observations" in ds.dims:
                    sr = ds.squeeze("observations")

            return shape_query.apply_mask_to(sr)

        return xr.DataArray([])

    def netcdf_name_for_date(self, when):

        print("Making NCDF name for date: %s" % when)

        return "{}{}_{}{}".format(self.outputs["readings"]["path"],
                                  self.outputs["readings"]["prefix"],
                                  when.strftime("%Y%m%d"),
                                  self.outputs["readings"]["suffix"])

    def date_is_cached(self, when):

        # TODO -Swift Object Storage Checking

        file_path = Path(self.outputs["readings"]['path'])
        if not file_path.is_dir():
            os.makedirs(file_path)

        this_ncdf = self.netcdf_name_for_date(when)

        ok = Path(this_ncdf).is_file()
        print("\n--> Checking for existence of NetCDF, %s for %s: %s" %
              (this_ncdf, when.strftime("%d %m %Y"), ok))

        # TODO -if OK put the file into Swift Storage

        if ok:
            return this_ncdf
        else:
            return False

    async def get_shaped_timeseries(self, query: ShapeQuery) -> ModelResult:
        print(
            "\n--->>> Shape Query Called successfully on %s Model!! <<<---" % self.name)
        print("Spatial Component is: \n%s" % str(query.spatial))
        print("Temporal Component is: \n%s" % str(query.temporal))

        print("\nDerived LAT1: %s\nDerived LON1: %s\nDerived LAT2: %s\nDerived LON2: %s" %
              query.spatial.expanded(0.05))

        sr = await (self.get_shaped_resultcube(query))

        print(sr)
        print(sr.data)
        # Check our param exists in the shaped result set
        if len(sr.data) > 0:

            print(">> Crack a beer we got there!! <<")

            dps = [self.get_datapoint_for_param(b=sr.isel(time=t), param=self.outputs["readings"]["prefix"])
                   for t in range(0, len(sr["time"]))]
            return ModelResult(model_name=self.name, data_points=dps)
        else:
            return ModelResult(model_name=self.name, data_points=[])

    async def dataset_files(self, when):
        ok = self.date_is_cached(when)
        if ok:
            return ok
        else:
            return await self.do_compilation(when)

    async def do_compilation(self, when):
        self.set_date(when)
        run = await asyncio.gather(*[self.run_main()])
        return run

    async def get_shaped_timeseries(self, query: ShapeQuery) -> ModelResult:
        print(
            "\n--->>> Shape Query Called successfully on %s Model!! <<<---" % self.name)

        dps = []
        try:
            sr, weights = await (self.get_shaped_resultcube(query))
            sr.load()

            for r in sr['time']:
                t = r['time'].values
                o = sr.sel(time=t)
                p = self.outputs['readings']['prefix']
                df = o[p].to_dataframe()
                df = df[p]

                indices = np.where(np.logical_not(np.isnan(weights)))[0]
                wm = np.ma.average(sr[indices], weights=weights[indices])

                dps.append(DataPoint(observation_time=str(t).replace('.000000000', '.000Z'),
                                     value=np.nanmedian(df),
                                     mean=np.nanmean(df),
                                     weighted_mean=wm,
                                     minimum=np.nanmin(df),
                                     maximum=np.nanmax(df),
                                     deviation=np.nanstd(df),
                                     count=df.count()))
        except FileNotFoundError:
            logger.exception('Files not found for date range.')

        asyncio.sleep(1)

        return ModelResult(model_name=self.name, data_points=dps)
