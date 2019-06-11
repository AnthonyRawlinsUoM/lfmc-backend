import glob
import os
import os.path
from urllib.error import URLError
import asyncio
import numpy as np

import datetime as dt
import xarray as xr
from pathlib2 import Path

from serve.lfmc.results.Abstracts import Abstracts
from serve.lfmc.results.Author import Author
from serve.lfmc.models.Model import Model
from serve.lfmc.models.ModelMetaData import ModelMetaData
from serve.lfmc.results.DataPoint import DataPoint
from serve.lfmc.results.MPEGFormatter import MPEGFormatter
from serve.lfmc.results.ModelResult import ModelResult
from serve.lfmc.query.ShapeQuery import ShapeQuery
from serve.lfmc.query.GeoQuery import GeoQuery
from serve.lfmc.query.SpatioTemporalQuery import SpatioTemporalQuery
import pickle
import regionmask

import matplotlib.pyplot as plt
import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.debug("logger set to DEBUG")

plt.switch_backend('agg')


class DeadFuelModel(Model):
    """ This is somewhat of an unusual case in that the files used for VP and T come as grid files and require some
     pre-processing to get them into properly projected NetCDF files.
    """

    def __init__(self):

        self.name = "dead_fuel"

        # TODO - Proper metadata!
        authors = [
            Author(name="Rachel Nolan", email="",
                   organisation="Hawkesbury Institute for the Environment, Western Sydney University"),
            Author(name="Víctor Resco de Dios", email="",
                   organisation="Hawkesbury Institute for the Environment, Western Sydney University"),
            Author(name="Matthias M. Boer", email="",
                   organisation="Hawkesbury Institute for the Environment, Western Sydney University"),
            Author(name="Gabriele Caccamo", email="",
                   organisation="Hawkesbury Institute for the Environment, Western Sydney University"),
            Author(name="Matthias M. Boer", email="",
                   organisation="Hawkesbury Institute for the Environment, Western Sydney University"),
            Author(name="Michael L. Goulden", email="",
                   organisation="Department of Earth System Science, University of California"),
            Author(name="Ross A. Bradstock", email="",
                   organisation="Centre for Environmental Risk Management of Bushfires, Centre for Sustainable Ecosystem Solutions, University of Wollongong")
        ]

        pub_date = dt.datetime(2015, 12, 9)
        abstract = Abstracts("Spatially explicit predictions of fuel moisture content are crucial for quantifying fire danger indices and as inputs \
        to fire behaviour models. Remotely sensed predictions of fuel moisture have typically focused on live fuels; but \
        regional estimates of dead fuel moisture have been less common. Here we develop and test the spatial application \
        of a recently developed dead fuel moisture model, which is based on the exponential decline of fine fuel moisture \
        with increasing vapour pressure deficit (D). We first compare the performance of two existing approaches to pre- \
        dict D from satellite observations. We then use remotely sensed D, as well as D estimated from gridded daily \
        weather observations, to predict dead fuel moisture. We calibrate and test the model at a woodland site in \
        South East Australia, and then test the model at a range of sites in South East Australia and Southern California \
        that vary in vegetation type, mean annual precipitation (129–1404 mm year −1 ) and leaf area index (0.1–5.7). \
        We found that D modelled from remotely sensed land surface temperature performed slightly better than a \
        model which also included total precipitable water (MAE b 1.16 kPa and 1.62 kPa respectively). D calculated \
        with observations from the Moderate Resolution Imaging Spectroradiometer (MODIS) on the Terra satellite \
        was under-predicted in areas with low leaf area index. Both D from remotely sensed data and gridded weather \
        station data were good predictors of the moisture content of dead suspended fuels at validation sites, with \
        mean absolute errors less than 3.9% and 6.0% respectively. The occurrence of data gaps in remotely sensed \
        time series presents an obstacle to this approach, and assimilated or extrapolated meteorological observations \
        may offer better continuity.")
        self.metadata = ModelMetaData(authors=authors,
                                      published_date=pub_date,
                                      fuel_types=["surface"],
                                      doi="http://dx.doi.org/10.1016/j.rse.2015.12.010",
                                      abstract=abstract)

        # Prefixes
        vapour_prefix = 'VP3pm'
        temp_prefix = 'Tmx'
        precipitation_prefix = 'P'
        dead_fuel_moisture_prefix = 'DFMC'

        self.ident = "Dead Fuels"
        self.code = "DFMC"
        self.path = os.path.abspath(Model.path() + 'Dead_FM') + '/'

        vapour_url = "http://www.bom.gov.au/web03/ncc/www/awap/vprp/vprph15/daily/grid/0.05/history/nat/"
        max_avg_temp_url = "http://www.bom.gov.au/web03/ncc/www/awap/temperature/maxave/daily/grid/0.05/history/nat/"
        precipitation_url = "http://www.bom.gov.au/web03/ncc/www/awap/rainfall/totals/daily/grid/0.05/history/nat/"

        vapour_path = self.path + vapour_prefix + "/"
        max_avg_temp_path = self.path + temp_prefix + "/"
        precipitation_path = self.path + precipitation_prefix + "/"

        self.tolerance = 0.06  # As a percentage accuracy

        self.parameters = {
            "vapour pressure": {
                "var": "VP3pm",
                "path": vapour_path,
                "url": vapour_url,
                "prefix": vapour_prefix,
                "suffix": ".grid",
                "dataset": ".grid.nc",
                "compression_suffix": ".Z"
            },
            "maximum average temperature": {
                "var": "T",
                "path": max_avg_temp_path,
                "url": max_avg_temp_url,
                "prefix": temp_prefix,
                "suffix": ".grid",
                "dataset": ".grid.nc",
                "compression_suffix": ".Z"
            },
            "precipitation": {
                "var": "P",
                "path": precipitation_path,
                "url": precipitation_url,
                "prefix": precipitation_prefix,
                "suffix": ".grid",
                "dataset": ".grid.nc",
                "compression_suffix": ".Z"
            }
        }

        self.outputs = {
            "type": "fuel moisture",
            "readings": {
                "path": self.path + dead_fuel_moisture_prefix + "/",
                "url": "",
                "prefix": dead_fuel_moisture_prefix,
                "suffix": ".nc",
            }
        }

        # self.storage_engine = LocalStorage(
        #     {"parameters": self.parameters, "outputs": self.outputs})

    def netcdf_name_for_date(self, when):

        archive_file = "{}{}_{}{}".format(self.path,
                                          self.outputs["readings"]["prefix"],
                                          when.strftime("%Y"),
                                          self.outputs["readings"]["suffix"])

        if Path(archive_file).is_file():
            return archive_file

        return "{}{}_{}{}".format(self.outputs["readings"]["path"],
                                  self.outputs["readings"]["prefix"],
                                  when.strftime("%Y%m%d"),
                                  self.outputs["readings"]["suffix"])

    def all_netcdfs(self):
        """
        Pattern matches potential paths where files could be stored to those that actually exist.
        Warning: Files outside this directory aren't indexed and won't get ingested.
        :return:
        """
        possibles = [p for p in glob.glob("{}{}_*{}".format(self.path,
                                                            self.outputs["readings"]["prefix"],
                                                            self.outputs["readings"]["suffix"]))]
        return [f for f in possibles if Path(f).is_file()]

    async def dataset_files(self, when):
        if self.date_is_cached(when):
            return self.netcdf_name_for_date(when)
        else:
            collection = await asyncio.gather(*[self.collect_parameter_data(param, when)
                                                for param in self.parameters])
            logger.debug(collection)
            collection = [x for x in collection if x is not None]
            if len(collection) > 0:
                return self.do_compilation(collection, when)
            else:
                logger.debug("Collected an empty list of files!")
                logger.debug(collection)
                return None

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
        sr = None
        fs = await asyncio.gather(*[self.dataset_files(when) for when in shape_query.temporal.dates()])
        fs = list(
            set([f for f in fs if (f is not None and Path(f).is_file())]))

        asyncio.sleep(1)
        if len(fs) > 0:
            logger.debug(fs)

            with xr.open_mfdataset(fs, concat_dim='time') as ds:

                if "observations" in ds.dims:
                    sr = ds.squeeze("observations")
                else:
                    sr = ds
                logger.debug(sr)
                sr = sr.sel(time=slice(shape_query.temporal.start.strftime("%Y-%m-%d"),
                                       shape_query.temporal.finish.strftime("%Y-%m-%d")))

            return sr
        else:
            logger.debug("No files available/gathered for that space/time.")
            return xr.DataArray([])

    # async def get_resultcube(self, query: SpatioTemporalQuery) -> xr.DataArray:
    #     """
    #     Does not guarantee a raster stack result.
    #     Quite possibly a jaggy edge.
    #     Essentially a subset of points only.
    #     """
    #
    #     sr = None
    #     fs = await asyncio.gather(*[self.dataset_files(when) for when in query.temporal.dates()])
    #     asyncio.sleep(1)
    #     if len(fs) > 0:
    #         with xr.open_mfdataset(fs) as ds:
    #             if "observations" in ds.dims:
    #                 ds = ds.squeeze("observations")
    #
    #             # expand coverage to tolerance
    #             # ensures single point returns at least 1 cell
    #             # also ensures ds slicing will work correctly
    #             lat1, lon1, lat2, lon2 = query.spatial.expanded(
    #                 0.05)  # <-- TODO - Remove magic number and get spatial pixel resolution from metadata
    #
    #             # restrict coverage to extents of ds
    #             lat1 = max(lat1, ds["latitude"].min())
    #             lon1 = max(lon1, ds["longitude"].min())
    #             lat2 = min(lat2, ds["latitude"].max())
    #             lon2 = min(lon2, ds["longitude"].max())
    #
    #             sr = ds.sel(latitude=slice(lat1, lat2),
    #                         longitude=slice(lon1, lon2))
    #             sr.load()
    #
    #     return sr

    # async def get_timeseries(self, query: SpatioTemporalQuery) -> ModelResult:
    #     """
    #     Essentially just time slicing the resultcube.
    #     DataPoint actually handles the creation of values from stats.
    #     :param query:
    #     :return:
    #     """
    #     logger.debug(
    #         "--->>> SpatioTemporal Query Called on %s Model!! <<<---" % self.name)
    #     sr = await (self.get_resultcube(query))
    #     sr.load()
    #     asyncio.sleep(1)
    #     dps = [self.get_datapoint_for_param(b=sr.isel(time=t), param="DFMC")
    #            for t in range(0, len(sr["time"]))]
    #     return ModelResult(model_name=self.name, data_points=dps)

    async def consolidate_year(self, y):
        with open(Model.path() + 'australia.pickle', 'rb') as pickled_australia:
            australia = pickle.load(pickled_australia)

        AUmask = regionmask.Regions_cls(
            'AUmask', [0], ['Australia'], ['AU'], [australia.polygon])

        for year in range(y, y + 1):
            with xr.open_mfdataset("%s%s_%s*" % (self.outputs['readings']['path'], self.outputs['readings']['prefix'], year), chunks={'time': 1}) as ds:
                ds['DFMC'] = ds['DFMC'].isel(observations=0, drop=True)
                dm = ds['DFMC'].isel(time=0)
                mask = AUmask.mask(dm['longitude'], dm['latitude'])
                mask_ma = np.ma.masked_invalid(mask)
                ds = ds.where(mask_ma == 0)
                logger.debug("--- Saving %s" % (year))
                ds.attrs = dict()
                ds.attrs['crs'] = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs "
                ds.attrs['var_name'] = 'DFMC'

                ds['time'].attrs['long_name'] = 'time'
                ds['time'].attrs['name'] = 'time'
                ds['time'].attrs['standard_name'] = 'time'

                ds['DFMC'].attrs['units'] = 'fullness'
                ds['DFMC'].attrs[
                    'long_name'] = 'Dead Fine Fuels Moisture Content - (Percentage wet over dry by weight)'
                ds['DFMC'].attrs['name'] = self.outputs['readings']['prefix']
                ds['DFMC'].attrs['standard_name'] = self.outputs['readings']['prefix']

                # logger.debug(ds)
                # logger.debug(ds[self.outputs['readings']['prefix']])
                # File is opened by itself, can't save because we're self locking
                temp = ds
                # close the handle first and then save

            tfile = "%s%s_%s.nc" % (self.path,
                                    self.outputs['readings']['prefix'],
                                    str(year))

            temp.to_netcdf(tfile + '.tmp')
            try:
                os.remove(tfile)
            except e:
                logger.error(e)
            os.rename(tfile + '.tmp', tfile)

        return True

    @staticmethod
    def do_conversion(file_name, param, when):
        """ Converts Arc Grid input files to NetCDF4 """
        y = when.strftime("%Y")
        m = when.strftime("%m")
        d = when.strftime("%d")
        logger.debug(
            "\n--> Processing data for: %s-%s-%s\n--> Converting: %s" % (d, m, y, file_name))

        nc_version = "%s.nc" % file_name
        arr = xr.open_rasterio("%s" % file_name)
        arr = arr.to_dataset(name="observations", dim=param["prefix"])
        arr = arr.rename({'y': 'latitude', 'x': 'longitude', 'band': 'time'})
        arr.coords['time'] = [dt.datetime(int(y), int(m), int(d))]
        arr.attrs['time:units'] = "Days since %s-%s-%s 00:00:00" % (y, m, d)
        arr.attrs['crs'] = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs "
        arr.attrs['created'] = "%s" % (dt.datetime.now().strftime("%d-%m-%Y"))
        arr.to_netcdf(nc_version, mode='w', format='NETCDF4')
        arr.close()

        return nc_version

    def do_compilation(self, param_datasets, when):
        DFMC_file = self.netcdf_name_for_date(when)

        y = when.strftime("%Y")
        m = when.strftime("%m")
        d = when.strftime("%d")

        tempfile = '/tmp/temp%s-%s-%s.nc' % (d, m, y)

        if len(param_datasets) > 0:

            # if len(param_datasets) == 1:
            #     logger.debug("Will open just: %s" % param_datasets)
            # elif len(param_datasets) > 1:

            logger.debug("\n----> Will open: %s" % f for f in param_datasets)

            with xr.open_mfdataset(param_datasets, concat_dim="observations") as ds:
                vp = ds["VP3pm"].isel(time=0)
                tmx = ds["Tmx"].isel(time=0)
                dfmc = DeadFuelModel.calculate(vp, tmx)
                dfmc = dfmc.expand_dims('time')
                logger.debug("Processing data for: %s-%s-%s" % (d, m, y))
                DFMC = dfmc.to_dataset('DFMC')
                DFMC.to_netcdf(tempfile, format='NETCDF4')
                logger.debug("\n------> Wrote: %s" % tempfile)
                logger.debug(DFMC)

            param_datasets.append(tempfile)

            with xr.open_mfdataset(tempfile) as combined:
                # DFMC.coords['time'] = [dt.datetime(int(y), int(m), int(d))]
                combined['DFMC'].attrs['DFMC:units'] = "Percentage wet over dry by weight."
                combined['DFMC'].attrs['long_name'] = "Dead Fuel Moisture Content"
                combined['DFMC'].attrs['time:units'] = "Days since %s-%s-%s 00:00:00" % (
                    y, m, d)
                combined['DFMC'].attrs['crs'] = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs "
                combined.attrs['created'] = "%s" % (
                    dt.datetime.now().strftime("%d-%m-%Y"))
                combined['DFMC'].attrs['output_frequency'] = "daily"
                combined.attrs['convention'] = "CF-1.4"
                combined.attrs['references'] = "#refs"
                combined.attrs['comment'] = "#comments"
                combined.attrs['var_name'] = self.outputs["readings"]["prefix"]

                logger.debug(combined)

                combined.to_netcdf(DFMC_file, mode='w', format='NETCDF4')
                combined.close()

            os.remove(tempfile)
        else:
            logger.debug("--> Can't open partial requirements!!! ")

        # Send file to SWIFT Storage here?
        asyncio.sleep(1)

        return DFMC_file

    async def collect_parameter_data(self, param, when):
        """ Collects input parameters for the model as determined by the metadata. """
        parameter_dataset_name = None

        # Only collect from the past!
        if when.date() < dt.datetime.today().date():
            param = self.parameters[param]
            file_path = Path(param['path'])
            if not file_path.is_dir():
                os.makedirs(file_path)

            parameter_dataset_name = file_path.joinpath(param['prefix'] + "_"
                                                        + param['dataset'])
            if parameter_dataset_name.is_file():
                return parameter_dataset_name
            else:
                data_file = file_path.joinpath(param['prefix'] + "_"
                                               + when.strftime("%Y%m%d")
                                               + param['suffix'])

                logger.debug(data_file)

                archive_file = Path(
                    str(data_file) + param['compression_suffix'])

                try:

                    if data_file.is_file():
                        parameter_dataset_name = self.do_conversion(
                            data_file, param, when)

                    elif not data_file.is_file() and archive_file.is_file():
                        logger.debug(
                            'Found an unexpanded archive: %s' % archive_file)
                        await self.do_expansion(archive_file)
                        parameter_dataset_name = self.do_conversion(
                            data_file, param, when)
                        # Remove the archive?

                    elif not data_file.is_file() and not archive_file.is_file():
                        date_string = when.strftime("%Y%m%d")
                        resource = date_string + date_string + \
                            param['suffix'] + param['compression_suffix']

                        if await self.do_download(param["url"], resource, archive_file):
                            if await self.do_expansion(archive_file):
                                parameter_dataset_name = self.do_conversion(
                                    data_file, param, when)

                except URLError as e:
                    logger.debug(e)
                    return None
                except FileNotFoundError as fnf:
                    logger.debug(fnf)
                    return None

        if Path(parameter_dataset_name).is_file():
            return parameter_dataset_name
        else:
            logger.debug("No parameters for that date.")
            return None

    @staticmethod
    def calculate(vp, t):
        """Short summary.

        Parameters
        ----------
        vp : type
            Description of parameter `vp`.
        t : type
            Description of parameter `t`.

        Returns
        -------
        type
            Description of returned object.

        """
        ea = vp * 0.1
        es = 0.6108 * np.exp(17.27 * t / (t + 237.3))
        d = np.clip(ea - es, None, 0)
        return 6.79 + (27.43 * np.exp(1.05 * d))

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

        if len(dps) == 0:
            logger.debug('Found no datapoints.')
            logger.debug(sr)

        asyncio.sleep(1)

        return ModelResult(model_name=self.name, data_points=dps)
