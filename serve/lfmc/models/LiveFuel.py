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

from dateutil.parser import parse


import serve.lfmc.config.debug as dev
from serve.lfmc.models.Model import Model
from serve.lfmc.models.ModelMetaData import ModelMetaData
from serve.lfmc.query.ShapeQuery import ShapeQuery
from serve.lfmc.query.GeoQuery import GeoQuery
from serve.lfmc.query.SpatioTemporalQuery import SpatioTemporalQuery
from serve.lfmc.resource.SwiftStorage import SwiftStorage
from serve.lfmc.results.Abstracts import Abstracts
from serve.lfmc.results.Author import Author
from serve.lfmc.results.DataPoint import DataPoint
from serve.lfmc.results.ModelResult import ModelResult
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
        self.outputs = {
            "type": "fuel moisture",
            "readings": {
                "path": "LFMC",
                "url": "LiveFM",
                "prefix": "LFMC",
                "suffix": ".nc",
            }
        }

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

    async def get_inventory_for_request(self, url_string):
        logger.debug('Getting %s' % url_string)
        r = requests.get(url_string)
        queue = []
        if r.status_code == requests.codes.ok:
            granules = r.text.split('\n')
            for line in granules:
                if len(line) > 0 and self.is_acceptable_granule(line):
                    queue.append(line)
        else:
            raise (
                "[Error] Can't continue. Didn't receive what we expected from USGS / NASA.")
        return queue

    def get_hv(self, url):
        """ Parses a HDF_EOS URI to extract HV coords """
        uri_parts = url.split('/')
        return self.hv_for_modis_granule(uri_parts[-1])

    def is_acceptable_granule(self, granule):
        """ Generates a list of tuples describing HV coords for granules that are used
        to generate a MODIS composite covering Australia. Returns list of granules that match.
        """
        return self.get_hv(granule) in [(h, v) for h in range(27, 31) for v in range(9, 13)]

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

    def which_hvs_for_query(self, bbox):
        rurl = 'https://lpdaacsvc.cr.usgs.gov/services/inventory?product=MOD09A1&version=6&bbox=' + \
            bbox + '&date=2013-01-02,2013-04-05&output=text'
        queue = []

        r = requests.get(rurl)
        if r.status_code == requests.codes.ok:
            granules = r.text.split('\n')
            for line in granules:
                if len(line) > 0:  # and self.is_acceptable_granule(line):
                    h, v = hv_for_modis_granule(line.split('/')[-1])
                    queue.append("h%sv%s" % (h, v))
        else:
            print('Failed')

        return list(set(queue))

    def which_archival_years_for_daterange(self, start, finish):
        years = int(finish.year - start.year) + 1
        return [int(start.year) + i for i in range(0, years)]

    def netcdf_name_for_date_and_granule(self, when, hv):
        return "{}{}__h{}v{}_{}{}".format(self.outputs["readings"]["path"],
                                          self.outputs["readings"]["prefix"],
                                          hv,
                                          when.strftime("%Y"),
                                          self.outputs["readings"]["suffix"])

    def fuel_name(self, granule):
        h, v = self.hv_for_modis_granule(granule)
        d = self.date_for_modis_granule(granule)
        name = "LFMC_h{}v{}_{}.nc".format(h, v, d.strftime("%Y%m%d"))
        return name

    async def dataset_files(self, start, finish, bbox):
        """
        Uses USGS service to match spatiotemporal query to granules required.
        converts each granule name to LFMC name
        """
        granules = []

        # Generate names of hypothetical archives
        for when in self.which_archival_years_for_daterange(start, finish):
            for hv in self.which_hvs_for_query(bbox):
                granules.append(
                    self.netcdf_name_for_date_and_granule(when, hv))

        # Test for the existence of these archives

        missing = [m for m in granules if not Path(m).is_file()]

        if len(missing) > 0:
            logger.debug('Some granules are missing.')
            logger.debug('Gathering missing granules.')

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

            # all_ok = await asyncio.gather(*[self.retrieve_earth_observation_data(
            #     v) for k, v in dfiles if not Path(k).is_file()])

            logger.debug(dfiles)
            return [k for k, v in dfiles if Path(k).is_file()]
        else:
            return granules

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
