import asyncio
import glob

import gdal
import pandas as pd
import os
import os.path
import numpy as np
import pyproj
import requests
import xarray as xr
import datetime as dt
import json
import urllib
import ogr
import osr


from pathlib2 import Path
import lfmc.config.debug as dev
from lfmc.models.Model import Model
from lfmc.models.ModelMetaData import ModelMetaData
from lfmc.query.ShapeQuery import ShapeQuery
from lfmc.query.GeoQuery import GeoQuery
from lfmc.query.SpatioTemporalQuery import SpatioTemporalQuery
from lfmc.resource.SwiftStorage import SwiftStorage
from lfmc.results.Abstracts import Abstracts
from lfmc.results.Author import Author
from lfmc.results.DataPoint import DataPoint
from lfmc.results.ModelResult import ModelResult
import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.debug("logger set to DEBUG")


class LiveFuelModel(Model):

    def __init__(self):
        self.name = "live_fuel"

        # TODO - Proper metadata!
        authors = [
            Author(name="Rachel Nolan", email="test1@example.com",
                   organisation="Test Organisation"),
            Author(name="Victor Di Resco", email="test2@example.com",
                   organisation="Test Organisation")
        ]
        pub_date = dt.datetime(2015, 9, 9)

        # Which products from NASA
        product = "MOD09A1"
        version = "6"

        # AIO bounding box lower left longitude, lower left latitude, upper right longitude, upper right latitude.
        bbox = "108.0000,-45.0000,155.0000,-10.0000"

        self.modis_meta = product, version, bbox

        abstract = Abstracts("NYA")

        self.metadata = ModelMetaData(authors=authors,
                                      published_date=pub_date,
                                      fuel_types=["surface"],
                                      doi="http://dx.doi.org/10.1016/j.rse.2015.12.010",
                                      abstract=abstract)

        self.path = os.path.abspath(Model.path() + 'Live_FM') + '/'
        self.ident = "Live Fuels"
        self.code = "LFMC"
        self.parameters = {
            "surface relectance band 1": {
                "var": "sur_refl_1",
                "path": "",
                "url": "",
                "prefix": "SRB1",
                "suffix": ".hdf",
                "dataset": ".hdf",
                "compression_suffix": ".gz"
            },
            "surface relectance band 3": {
                "var": "sur_refl_3",
                "path": "",
                "url": "",
                "prefix": "SRB3",
                "suffix": ".hdf",
                "dataset": ".hdf",
                "compression_suffix": ".gz"
            },
            "surface relectance band 4": {
                "var": "sur_refl_4",
                "path": "",
                "url": "",
                "prefix": "SRB4",
                "suffix": ".hdf",
                "dataset": ".hdf",
                "compression_suffix": ".gz"
            }
        }
        self.outputs = {
            "type": "fuel moisture",
            "readings": {
                "path": "LFMC",
                "url": "LiveFM",
                "prefix": "LFMC",
                "suffix": ".nc",
            }
        }

        # self.storage_engine = SwiftStorage()
        # {"parameters": self.parameters, "outputs": self.outputs})

    # @deprecated
    # def check_for_netrc(self):
    #     cmdline("cat /home/arawlins/.netrc")
    #

    def netcdf_name_for_date(self, when):
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
        possibles = [p for p in glob.glob("{}{}_*{}".format(self.outputs["readings"]["path"],
                                                            self.outputs["readings"]["prefix"],
                                                            self.outputs["readings"]["suffix"]))]
        return [f for f in possibles if Path(f).is_file()]

    @staticmethod
    def used_granules():
        """ Generates a list of tuples describing HV coords for granules that are used
        to generate a MODIS composite covering Australia.
        """
        return [(h, v) for h in range(27, 31) for v in range(9, 13)]

    def is_acceptable_granule(self, granule):
        return self.get_hv(granule) in LiveFuelModel.used_granules()

    @staticmethod
    def hv_for_modis_granule(granule):
        """ Extracts HV grid coords from naming conventions of HDF-EOS file.
        Assumes input is a file name string conforming to EOS naming conventions."""

        parts = granule.split('.')
        hv_component = parts[2].split('v')
        h = int(hv_component[0].replace('h', ''))
        v = int(hv_component[1])
        return h, v

    def date_for_modis_granule(self, granule):
        """ Extracts the observation date from the naming conventions of a HDF-EOS file"""
        # unravel naming conventions
        parts = granule.split('.')
        # set the key for subgrouping to be the date of observation by parsing the Julian Date
        return dt.datetime.strptime((parts[1].replace('A', '')), '%Y%j')

    def get_hv(self, url):
        """ Parses a HDF_EOS URI to extract HV coords """
        uri_parts = url.split('/')
        return self.hv_for_modis_granule(uri_parts[-1])

    async def retrieve_earth_observation_data(self, url):
        """ Please note: Requires a valid .netrc file in users home directory! """

        # logger.debug(url)
        file_name = url.split('/')[-1]
        xml_name = file_name + '.xml'

        # LFMC Product as granules
        livefuel_name = self.fuel_name(file_name)
        livefuel_file = Path(
            self.outputs['readings']['path'] + "/" + livefuel_name)

        # MODIS source granules
        hdf_file = Path(self.path + "/modis/" + file_name)
        xml_file = Path(self.path + "/modis/" + xml_name)
        os.chdir(self.path)
        if (livefuel_file.is_file()):
            logger.debug("Have: %s", livefuel_name)
            return livefuel_name

        elif (not hdf_file.is_file()) or (os.path.getsize(hdf_file) == 0):
            # No local file either!
            logger.debug("[Downloading] %s" % file_name)
            # cmdline("curl -n -L -c cookiefile -b cookiefile %s --output %s" % (url, file_name))
            os.chdir('./modis')
            os.system(
                "wget -L --accept hdf --reject html --load-cookies=cookiefile --save-cookies=cookiefile %s -O %s" % (
                    url, file_name))
            os.chdir('..')
            asyncio.sleep(1)
            await self.convert_modis_granule_file_to_lfmc(str(hdf_file))
        elif hdf_file.is_file():
            # Local file now exists
            logger.debug('Converting MODIS to Fuel Granule...')
            # TODO -> Process the file and calc the Live FM here!
            await self.convert_modis_granule_file_to_lfmc(str(hdf_file))
        else:
            logger.debug('No HDF file!')

        # if not self.storage_engine.swift_check_lfmc(hdf5_name):
        #     logger.debug('Fuel Granule Not in Swift Storage...')
        #     # No LFMC Product for this granule
        #     if not self.storage_engine.swift_check_modis(file_name):
        #         logger.debug('MODIS Granule Not in Swift Storage...')
            # No Granule held in cloud
        #         if (not hdf_file.is_file()) or (os.path.getsize(hdf_file) == 0):
        #             # No local file either!
        #             logger.debug("[Downloading]" + file_name)
        #             # cmdline("curl -n -L -c cookiefile -b cookiefile %s --output %s" % (url, file_name))
        #             os.system(
        #                 "wget -L --accept hdf --reject html --load-cookies=cookiefile --save-cookies=cookiefile %s -O %s" % (
        #                     url, file_name))
        #             asyncio.sleep(1)
        #
        #         if hdf_file.is_file():
        #             # Local file now exists
        #             logger.debug('Converting MODIS to Fuel Granule...')
        #             # TODO -> Process the file and calc the Live FM here!
        #             xlfmc = self.convert_modis_granule_file_to_lfmc(hdf_file)
        #             # Upload the LFMC HDF5 file to swift API as well.
        #             logger.debug('Storing Fuel Granule in Swift...')
        #             self.storage_engine.swift_put_lfmc()
        #             # else:
        #             #     raise CalculationError('Processing LFMC for Granule: %s failed!' % (hdf_file))
        #
        #             # Make sure to save the original source
        #             if self.storage_engine.swift_put_modis(file_name):
        #                 os.remove(file_name)
        #     else:
        #         # MODIS Source exists but derived LFMC HDF5 does not!
        #         self.storage_engine.swift_get_modis(file_name)
        #
        #         # TODO -> Process the file and calc the Live FM here!\
        #         with self.convert_modis_granule_file_to_lfmc(hdf_file) as xlfmc:
        #             # Upload the LFMC HDF5 file to swift API as well.
        #             self.storage_engine.swift_put_lfmc(xlfmc)
        #
        #         # else:
        #         #     raise CalculationError('Processing LFMC for Granule: %s failed!' % (hdf_file))
        #
        #     logger.debug("[OK] %s" % (file_name))
        #
        #     if not self.storage_engine.swift_check_modis(xml_name):
        #         if (not xml_file.is_file()) or (os.path.getsize(xml_file) == 0):
        #             logger.debug("[Downloading] " + xml_name)
        #             os.system(
        #                 "wget -L --accept xml --reject html --load-cookies=cookiefile --save-cookies=cookiefile %s -O %s" % (
        #                     url, xml_name))
        #             # cmdline("curl -n -L -c cookiefile -b cookiefile %s --output %s" % (url+'.xml', xml_name))
        #         if xml_file.is_file():
        #             if self.storage_engine.swift_put_modis(xml_name):
        #                 os.remove(xml_name)
        #     logger.debug("[OK] %s" % (xml_name))
        #
        # else:
        #     # LFMC exists for this granule in Nectar Cloud already!
        #     logger.debug(
        #         'LFMC exists for this granule in Nectar Cloud already!')

        asyncio.sleep(1)
        if Path(livefuel_name).is_file():
            logger.debug('Validated %s' % livefuel_name)
        return livefuel_name

    # def group_queue_by_date(self, queue):
    #     grouped = {}
    #
    #     logger.debug('#### Deconstructing: %s', [e for e in queue])
    #
    #     # Sort the queue and group by date/granule HV coords
    #     for elem in queue:
    #         if type(elem) is list:
    #             logger.debug(
    #                 '#### Expected list of strings, got list of lists')
    #             for e in elem:
    #                 fname = e.split('/')[-1]
    #                 if fname.endswith('.hdf') or fname.endswith('.HDF'):
    #                     key = self.date_for_modis_granule(
    #                         fname).strftime('%Y-%m-%d')
    #                     grouped.setdefault(key, []).append(e)
    #         else:
    #             fname = elem.split('/')[-1]
    #             if fname.endswith('.hdf') or fname.endswith('.HDF'):
    #                 key = self.date_for_modis_granule(
    #                     fname).strftime('%Y-%m-%d')
    #                 grouped.setdefault(key, []).append(elem)
    #
    #     return grouped
        # return queue

    async def read_hdfeos_df_as_xarray(self, file_name, data_field_name):

        logger.debug('Called Read hdfeos df file_name on: %s' % file_name)

        grid_name = 'MOD_Grid_500m_Surface_Reflectance'
        gname = 'HDF4_EOS:EOS_GRID:"{0}":{1}:{2}'.format(file_name,
                                                         grid_name,
                                                         data_field_name)
        gdset = gdal.Open(file_name, gdal.GA_ReadOnly)

        subDatasets = gdset.GetSubDatasets()
        # this one is Band1
        dataset = gdal.Open(subDatasets[data_field_name][0])

        geotransform = dataset.GetGeoTransform()
        geoprojection = gdset.GetProjection()

        band = dataset.GetRasterBand(1)
        # logger.debug("Driver: {}/{}".format(dataset.GetDriver().ShortName,
        #                                     dataset.GetDriver().LongName))
        # logger.debug("Size is {} x {} x {}".format(dataset.RasterXSize,
        #                                            dataset.RasterYSize,
        #                                            dataset.RasterCount))
        # if geotransform:
        # logger.debug("Origin = ({}, {})".format(
        #     geotransform[0], geotransform[3]))
        # logger.debug("Pixel Size = ({}, {})".format(
        #     geotransform[1], geotransform[5]))

        inSRS_converter = osr.SpatialReference()  # makes an empty spatial ref object
        # populates the spatial ref object with our WKT SRS
        inSRS_converter.ImportFromWkt(geoprojection)
        # Exports an SRS ref as a Proj4 string usable by PyProj
        inSRS_forPyProj = inSRS_converter.ExportToProj4()

        # logger.debug(inSRS_forPyProj)

        fmttypes = {'Byte': 'B', 'UInt16': 'H', 'Int16': 'h', 'UInt32': 'I',
                    'Int32': 'i', 'Float32': 'f', 'Float64': 'd'}

        BandType = gdal.GetDataTypeName(band.DataType)

        data = band.ReadAsArray()
        x0, xinc, _, y0, _, yinc = geotransform
        nx, ny = (band.XSize, band.YSize)
        x = np.linspace(x0, x0 + xinc * nx, nx)
        y = np.linspace(y0, y0 + yinc * ny, ny)
        xv, yv = np.meshgrid(x, y)

        # Convert the grid back to lat/lons.
        sinu = pyproj.Proj(
            "+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs ")

        merc = pyproj.Proj(
            "+proj=merc +lon_0=0 +k=1 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs ")

        wgs84 = pyproj.Proj("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs")

        lon, lat = pyproj.transform(sinu, wgs84, xv, yv)

        # Read the attributes.
        meta = gdset.GetMetadata()
        # Now that the SINUSOIDAL projection has been regridded
        lon = lon[0, :band.XSize]
        #  lons can be single dimension index
        lat = lat[:band.YSize, 0]

        df = pd.DataFrame(data, index=lat, columns=lon)
        xrd = xr.DataArray(df)
        xrd.name = 'band'
        xrd = xrd.rename({'dim_0': 'lat'})
        xrd = xrd.rename({'dim_1': 'lon'})

        xrd.attrs['_FillValue'] = -28672
        xrd.attrs['crs'] = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs "
        ds = xr.decode_cf(xrd.to_dataset())

        # ds['band'].plot()
        # plt.show()

        return ds['band']

    async def convert_modis_granule_file_to_lfmc(self, fobj):
        """
        This method combines the bands to form the LFMC values IN THE GRANULE and adds the data to existing bands,
        along with appropriate metadata.

        For a single date (8-day window).
        :param fobjs:
        :return: An Xarray Dataset (in-memory)
        """
        logger.debug('Got call to convert: %s' % fobj)

        b1 = await self.read_hdfeos_df_as_xarray(fobj, 0)  # sur_refl_b01
        b3 = await self.read_hdfeos_df_as_xarray(fobj, 2)  # sur_refl_b03
        b4 = await self.read_hdfeos_df_as_xarray(fobj, 3)  # sur_refl_b04

        vari = ((b4 - b1) / (b4 + b1 - b3)).clip(-1, 1)

        # Calc spectral index
        vari_max = vari.max()
        vari_min = vari.min()
        vari_range = vari_max - vari_min
        rvari = (vari - vari_min / vari_range).clip(0, 1)  # SI
        data = np.reshape(np.array(52.51 ** (1.36 * rvari)),
                          b1.shape).astype(np.float64)

        # logger.debug(data.shape)

        # logger.debug(b1.coords)
        # logger.debug(b1.dims)

        # captured = b1.attrs['time']  #TODO <-- DEBUG THIS ATTRIBUTE is it correct?

        captured = self.date_for_modis_granule(str(fobj))

        xrd = xr.Dataset({'LFMC': (['time', 'lat', 'lon'], np.expand_dims(data, axis=0))},
                         coords={'lon': b1['lon'],
                                 'lat': b1['lat'],
                                 'time': pd.date_range(captured, periods=1)})

        # logger.debug(xrd)

        xrd.attrs['var_name'] = self.outputs["readings"]["prefix"]
        # xrd.attrs['var_name'] = "LFMC"
        xrd.attrs['created'] = "%s" % (
            dt.datetime.now().strftime("%d-%m-%Y"))
        xrd.attrs['crs'] = '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs '
        xrd['time:units'] = 'days since %s' % (
            captured.strftime("%Y-%m-%d"))
        xrd.load()
        name = self.fuel_name(fobj)
        xrd.to_netcdf(self.outputs['readings']['path']
                      + "/"
                      + name, mode='w', format='NETCDF4')

        logger.debug('Conversion of granule complete.')
        # TODO - auto ingest into geoserver!
        return name

    def fuel_name(self, granule):
        h, v = self.hv_for_modis_granule(granule)
        d = self.date_for_modis_granule(granule)
        name = "LFMC_h{}v{}_{}.nc".format(h, v, d.strftime("%Y%m%d"))
        return name

    @staticmethod
    def date_for_modis_granule(granule):
        """ Extracts the observation date from the naming conventions of a HDF-EOS file"""
        # unravel naming conventions
        parts = granule.split('.')

        # set the key for subgrouping to be the date of observation by parsing the Julian Date
        return dt.datetime.strptime((parts[1].replace('A', '')), '%Y%j')

    # ShapeQuery
    async def get_shaped_resultcube(self, shape_query: ShapeQuery) -> xr.DataArray:
        sr = None
        lat1, lon1, lat2, lon2 = shape_query.spatial.expanded(1.1)
        logger.debug('BL: %3.3f, %3.3f' % (lon1, lat1))
        logger.debug('TR: %3.3f, %3.3f' % (lon2, lat2))
        # Eg., "108.0000,-45.0000,155.0000,-10.0000"  # Bottom-left, top-right
        bbox = "%3.3f,%3.3f,%3.3f,%3.3f" % (lon1, lat1, lon2, lat2)
        logger.debug("%s" % bbox)
        collection = await asyncio.gather(*[self.dataset_files(when, bbox)
                                            for when in shape_query.temporal.dates()])

        logger.debug('Files to open are...')
        flat_list = list(
            set([item for sublist in collection for item in sublist]))
        fs = [self.outputs['readings']['path'] + "/" + f for f in flat_list if Path(
            self.outputs['readings']['path'] + "/" + f).is_file()]
        logger.debug(fs)

        asyncio.sleep(1)
        if len(fs) > 0:
            with xr.open_mfdataset(fs) as ds:
                if "observations" in ds.dims:
                    sr = ds.squeeze("observations")
                else:
                    sr = ds
                # sr = sr.sel(time=slice(shape_query.temporal.start.strftime("%Y-%m-%d"),
                #                        shape_query.temporal.finish.strftime("%Y-%m-%d")))

            return sr
        else:
            logger.debug("No files available/gathered for that space/time.")
            return xr.DataArray([])

    async def dataset_files(self, when, bbox):
        """
        Uses USGS service to match spatiotemporal query to granules required.
        converts each granule name to LFMC name
        """
        product, version, obbox = self.modis_meta
        dfiles = []

        rurl = "https://lpdaacsvc.cr.usgs.gov/services/inventory?product=" + product + "&version=" + \
               version + "&bbox=" + bbox + "&date=" + \
               when.strftime('%Y-%m-%d') + "&output=text"

        inventory = await asyncio.gather(*[self.get_inventory_for_request(rurl)])

        asyncio.sleep(1)
        # Convert inventory to fuel_names
        dfiles = [(self.fuel_name(g.split('/')[-1]), g)
                  for sl in inventory for g in sl]

        all_ok = await asyncio.gather(*[self.retrieve_earth_observation_data(
            v) for k, v in dfiles if not Path(k).is_file()])

        logger.debug(dfiles)
        return [k for k, v in dfiles]

    def unique_from_nestedlist(self, inventory):
        unique_data = []
        if type(inventory) is list:
            for i in inventory:
                if type(i) is list:
                    unique_row = self.unique_from_nestedlist(i)
                    [unique_data.insert(0, a)
                     for a in unique_row if a not in unique_data]
                else:
                    unique_data.insert(0, i)
        return sorted(unique_data)

    async def collect_granules(self, when):
        r = self.build_inventory_request_url(when)
        logger.debug('### Request URL for Inventory: \n')
        inventory = await asyncio.gather(*[self.get_inventory_for_request(r)])
        logger.debug('### Inventory to retrieve: \n')
        [logger.debug(i) for i in inventory]
        logger.debug('-' * 80)
        collected = []

        os.chdir(self.path)  # ????????

        if len(inventory) > 0:
            # Check the indexed files and don't replicate work!
            # Also check the current download queue to see if the granule is currently being downloaded.
            # split the queue by task status
            # grouped_by_date = self.group_queue_by_date(inventory)
            # for urls in list(grouped_by_date.values()):

            inventory = self.unique_from_nestedlist(inventory)
            for url in inventory:
                rok = await self.retrieve_earth_observation_data(url)
                collected.append(rok)
            return collected
        else:
            logger.debug('Collecting nothing!')
            return []

    async def get_inventory_for_request(self, url_string):
        logger.debug('Getting %s' % url_string)
        r = requests.get(url_string)
        queue = []
        if r.status_code == 200:
            granules = r.text.split('\n')
            for line in granules:
                if len(line) > 0 and self.is_acceptable_granule(line):
                    queue.append(line)
        else:
            raise (
                "[Error] Can't continue. Didn't receive what we expected from USGS / NASA.")
        return queue

    def build_inventory_request_url(self, when):
        """
        Uses USGS LPDAAC inventory service to select files.
        Gathers entirety of Australia rather than using query BBOX.
        """
        product, version, bbox = self.modis_meta

        rurl = "https://lpdaacsvc.cr.usgs.gov/services/inventory?product=" + product + "&version=" + \
               version + "&bbox=" + bbox + "&date=" + \
            when.strftime('%Y-%m-%d') + "&output=text"

        return rurl

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
            dps = geoQ.cast_fishnet({'init': 'EPSG:3577'}, sr[var])
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
