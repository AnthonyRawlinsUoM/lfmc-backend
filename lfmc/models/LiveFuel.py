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

        # Reprojected Granule
        base = os.path.splitext(file_name)[0]
        reprojd = base + '.nc'
        reprojd_filepath = Path(self.path + "/projd/" + reprojd)

        # MODIS source granules
        hdf_file = Path(self.path + "/modis/" + file_name)
        xml_file = Path(self.path + "/modis/" + xml_name)
        os.chdir(self.path)

        if (livefuel_file.is_file()):
            # Have Fuel NetCDF4 File
            logger.debug(
                "*** STEP 4 *** Seems we already have: %s", livefuel_name)

        elif (reprojd_filepath.is_file()):
            logger.debug(
                "*** STEP 3 *** Reprojection exists, now shifting to LiveFuel: %s", livefuel_name)
            os.rename(reprojd_filepath, livefuel_file)

        elif hdf_file.is_file():
            # Have MODIS Granule file
            logger.debug(
                '*** STEP 2 *** Converting MODIS to Fuel Granule by reprojection.')
            # TODO -> Process the file and calc the Live FM here!
            await self.convert_modis_granule_file_to_lfmc(str(hdf_file))
            os.rename(reprojd_filepath, livefuel_file)

        elif (not hdf_file.is_file()) or (os.path.getsize(hdf_file) == 0):
            # No local file either!
            # Get the MODIS Granule
            logger.debug("*** STEP 1 *** [Downloading] %s" % file_name)
            # cmdline("curl -n -L -c cookiefile -b cookiefile %s --output %s" % (url, file_name))
            os.chdir('./modis')
            os.system(
                "wget -L --accept hdf --reject html --load-cookies=cookiefile --save-cookies=cookiefile %s -O %s" % (
                    url, file_name))
            os.chdir('..')
            asyncio.sleep(1)
            logger.debug(
                '*** STEP 2 *** Converting MODIS to Fuel Granule by reprojection.')
            await self.convert_modis_granule_file_to_lfmc(str(hdf_file))
            logger.debug(
                "*** STEP 3 *** Reprojection now exists, now shifting to LiveFuel: %s", livefuel_name)
            os.rename(reprojd_filepath, livefuel_file)
            logger.debug("*** STEP 4 *** Seems we now have: %s", livefuel_name)
        else:
            logger.debug('No HDF file!')

        asyncio.sleep(1)
        if Path(livefuel_file).is_file():
            logger.debug('Validated that we have: %s' % livefuel_name)
        else:
            logger.debug('Collection failed for: %s!' % livefuel_name)

        return livefuel_name

    async def convert_modis_granule_file_to_lfmc(self, fobj):

        logger.debug('Got call to convert: %s' % fobj)
        captured = self.date_for_modis_granule(str(fobj))
        data = {
            'thefile': fobj
        }
        r = requests.post(
            url='http://transformr.landscapefuelmoisture.bushfirebehaviour.net.au/modis_to_ncdf', data=data)
        name = fobj.split('.')[-1] + '.nc'

        if r.status_code == 200:
            logger.debug('Got response from TransformR:\n%s\n' % r.text)
            name = r.text
            xrd = xr.open_dataset(self.path + '/projd/' + r.text)
            logger.debug(xrd)
            xrd.to_netcdf(
                self.outputs['readings']['path'] + '/' + fuel_name(fobj), format='NETCDF4')
            os.remove(self.path + '/projd/' + r.text)
            # os.remove(self.path + '/modis/' + fobj)
            logger.debug('Conversion of granule complete.')
        else:
            logger.debug('Got Error from TransformR: %s', r.text)

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
        lat1, lon1, lat2, lon2 = shape_query.spatial.expanded(1.0)
        # logger.debug('BL: %3.3f, %3.3f' % (lon1, lat1))
        # logger.debug('TR: %3.3f, %3.3f' % (lon2, lat2))
        # Eg., "108.0000,-45.0000,155.0000,-10.0000"  # Bottom-left, top-right
        bbox = "%3.3f,%3.3f,%3.3f,%3.3f" % (lon1, lat1, lon2, lat2)
        # logger.debug("%s" % bbox)

        collection = await self.dataset_files(shape_query.temporal.start, shape_query.temporal.finish, bbox)

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
            return sr
        else:
            logger.debug("No files available/gathered for that space/time.")
            return xr.DataArray([])

    async def dataset_files(self, start, finish, bbox):
        """
        Uses USGS service to match spatiotemporal query to granules required.
        converts each granule name to LFMC name
        """
        product, version, obbox = self.modis_meta
        dfiles = []

        rurl = "https://lpdaacsvc.cr.usgs.gov/services/inventory?product=" \
            + product \
            + "&version=" \
            + version \
            + "&bbox=" \
            + bbox \
            + "&date=" \
            + start.strftime('%Y-%m-%d') \
            + ',' \
            + finish.strftime('%Y-%m-%d') \
            + "&output=text"

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
            return await asyncio.gather(*[collected.append(self.retrieve_earth_observation_data(url)) for url in self.unique_from_nestedlist(inventory)])
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
