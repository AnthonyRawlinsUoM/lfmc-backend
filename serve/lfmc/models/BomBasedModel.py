import asyncio
import glob
from abc import abstractmethod
import datetime as dt
import pandas as pd
import numpy as np
import xarray as xr
from pathlib2 import Path
import serve.lfmc.config.debug as dev
from serve.lfmc.models.Model import Model
from serve.lfmc.query import ShapeQuery
from serve.lfmc.query.GeoQuery import GeoQuery
from serve.lfmc.results.DataPoint import DataPoint
from serve.lfmc.results.MPEGFormatter import MPEGFormatter
from serve.lfmc.results.ModelResult import ModelResult
import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.debug("logger set to DEBUG")


class BomBasedModel(Model):

    @abstractmethod
    def netcdf_name_for_date(self, when):
        logger.debug("&&&&&&&&&&&&&&&&&&&&&&&&&&& called abstract ERGH!")
        return ['/tmp']  # DEBUG ONLY

    async def mpg(self, query: ShapeQuery):
        sr = await (self.get_shaped_resultcube(query))
        logger.debug(sr)
        mp4 = await (MPEGFormatter.format(
            sr, self.outputs["readings"]["prefix"]))
        asyncio.sleep(1)
        return mp4

    async def get_netcdf(self, query: ShapeQuery):
        sr = await (self.get_shaped_resultcube(query))
        return sr

    # ShapeQuery
    async def get_shaped_resultcube(self, shape_query: ShapeQuery) -> xr.DataArray:

        fs = set()
        for when in shape_query.temporal.dates():
            # logger.debug("Looking for files for: %s in '%s'",
            #             when.strftime('%d / %m / %Y'),
            #             self.netcdf_name_for_date(when))
            [fs.add(file) for file in self.netcdf_name_for_date(
                when) if Path(file).is_file()]

        [logger.debug('Found: %s', p) for p in fs]

        fl = list(fs)
        xr1 = xr.DataArray(())
        if dev.DEBUG:
            [logger.debug("\n--> Will load: %s" % f) for f in fl]

        # Load these files in date order overwriting older data with the newer
        if len(fl) > 0:
            fl.sort()
            xr1 = xr.open_dataset(fl.pop(0))
            while len(fl) > 1:
                xr2 = xr.open_dataset(fl.pop(0))
                # if dev.DEBUG:
                #     logger.debug("\n--> Loading BOM SFC TS by overwriting older data: %s" % fl[0])
                xr1 = self.load_by_overwrite(xr1, xr2)

            xr1.attrs['var_name'] = self.outputs["readings"]["prefix"]
            # xr1.to_netcdf(Model.path() + 'temp/latest_{}_query.nc'.format(self.name), format='NETCDF4')

            if dev.DEBUG:
                # Include forecasts!
                logger.debug(xr1)
                ts = xr1.sel(time=slice(
                    shape_query.temporal.start.strftime("%Y-%m-%d"), None))
            else:
                ts = xr1.sel(time=slice(shape_query.temporal.start.strftime("%Y-%m-%d"),
                                        shape_query.temporal.finish.strftime("%Y-%m-%d")))

            return ts
        else:
            raise FileNotFoundError('No data exists for that date range')

    def load_by_overwrite(self, xr1, xr2):
        ds1_start = xr1[self.outputs["readings"]
                        ["prefix"]].isel(time=0).time.values
        ds2_start = xr2[self.outputs["readings"]
                        ["prefix"]].isel(time=0).time.values
        ds1_subset = xr1.sel(time=slice(str(ds1_start), str(ds2_start)))
        return xr.concat([ds1_subset, xr2], dim='time')

    def consolidate_to_year_archive(self, year, file_name):

        y_begin = dt.datetime(year, 1, 1)
        y_end = dt.datetime(year, 12, 31)
        fl = []

        # # Don't even attempt if still in this year
        # # ie., must be a year in the past
        # if y_end.year > dt.datetime.now().year:
        #     return False

        for d in pd.date_range(y_begin, y_end):
            fl += [y + '/' + file_name for y in glob.glob(
                Model.path() + "Weather/{}*".format(d.strftime("%Y%m%d")))]

        minimal_file_list = list(set(fl))
        files = [f for f in minimal_file_list if Path(f).is_file()]

        file_list = list(files)

        if len(file_list) > 0:
            file_list.sort()
            xr1 = xr.open_dataset(file_list.pop(0))
            while len(file_list) > 1:
                xr2 = xr.open_dataset(file_list.pop(0))
                # if dev.DEBUG:
                #     logger.debug("\n--> Loading BOM SFC TS by overwriting older data: %s" % fl[0])
                xr1 = self.load_by_overwrite(xr1, xr2)

            xr1.attrs['var_name'] = self.outputs["readings"]["prefix"]

            logger.debug(xr1)

            if xr1['time'] is None:
                logger.debug('No temporal component to DataSet?!')
                return False
            else:
                # This needs refinement to extract days worth of records instead of actual time entries
                time_records = xr1.sel(time=str(year))

                xr1.to_netcdf(self.archive_name(year), format='NETCDF4')
                return True

                # if len(time_records) >= 365:
                #     # This could potentially give us 365 milliseconds/seconds/hours worth of data. TODO - Just days!!
                #     xr1.to_netcdf(self.archive_name(year), format='NETCDF4')
                #     return True
                # else:
                #     # Can't yet save the year as an archive it's incomplete
                #     logger.debug('Attempted to create an annual archive for %s,'
                #           'but the year (%d) is incomplete, containing just %d records' % (self.code,
                #                                                                            year,
                #                                                                            len(time_records)))
                #     return False
        else:
            return False

    def archive_name(self, year):
        """
        If using a glob it is possible to pass * to year to retrieve all nc fils for all years held.
        :param year:
        :return:
        """

        return '{}{}_{}.nc'.format(self.path,
                                   self.code,
                                   year)

    def netcdf_names_for_date(self, when, file_name):

        # First, check to see if an annual archive exists
        archival_file = self.archive_name(when.year)
        if Path(archival_file).is_file():
            return [archival_file]

        # Can we create a full years archive for this whole year?
        if self.consolidate_to_year_archive(when.year, file_name):
            return [archival_file]
        else:

            # Because some of the data is in 7 day observations,
            # we need to pad dates +/- 7 days to ensure we grab the correct nc files that might contain 'when'
            window_begin = when - dt.timedelta(7)
            window_end = when + dt.timedelta(7)
            cdf_list = []

            for d in pd.date_range(window_begin, window_end):
                # Uncompressed
                cdf_list += [p + "/" + file_name + ".gz" for p in
                             glob.glob(Model.path() + "Weather/{}*".format(d.strftime("%Y%m%d")))]
                # Compressed
                cdf_list += [p + "/" + file_name for p in
                             glob.glob(Model.path() + "Weather/{}*".format(d.strftime("%Y%m%d")))]

            short_list = list(set(cdf_list))
            # Assemble the individual components that contain that date range
            # Auto-magically accepts only files that actually exist
            return [f for f in short_list if Path(f).is_file()]

    def all_netcdfs(self):
        """
        Pattern matches potential paths where files could be stored to those that actually exist.
        Warning: Files outside this directory aren't indexed and won't get ingested.
        :return:
        """
        archives = [f for f in glob.glob(
            self.archive_name("*")) if Path(f).is_file()]
        return archives

    async def get_shaped_timeseries(self, query: ShapeQuery):
        logger.debug(
            "\n--->>> Shape Query Called successfully on %s Model!! <<<---" % self.name)
        sr = await (self.get_shaped_resultcube(query))
        sr.load()
        var = self.outputs['readings']['prefix']
        dps = []
        try:
            logger.debug('Trying to find datapoints.')

            geoQ = GeoQuery(query)
            df = geoQ.cast_fishnet({'init': 'EPSG:3111'}, sr[var])

        except FileNotFoundError:
            logger.debug('Files not found for date range.')
        except ValueError as ve:
            logger.debug(ve)
        except OSError as oe:
            logger.debug(oe)

        if len(df) == 0:
            logger.debug('Found no datapoints.')
            logger.debug(sr)

        asyncio.sleep(1)

        return df

    async def get_timeseries_results(self, query: ShapeQuery) -> ModelResult:
        df = await (self.get_shaped_resultcube(query))
        geoQ = GeoQuery(query)
        return ModelResult(model_name=self.name, data_points=geoQ.pull_fishnet(df))
