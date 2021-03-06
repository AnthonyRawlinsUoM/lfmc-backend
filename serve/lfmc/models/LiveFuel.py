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
#from serve.lfmc.models.LiveScraper import LiveScraper

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
                "path": self.path,
                "url": "LiveFM",
                "prefix": "LFMC",
                "suffix": ".nc",
            }
        }

        # self.scraper = LiveScraper(self.path)s


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
        possibles = [p for p in glob.glob("{}{}_*{}".format(self.path,
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

    def hv_for_modis_granule(self, granule):
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
            logger.debug('200 OK')
            granules = r.text.split('\n')
            for line in granules:
                if len(line) > 0:  # and self.is_acceptable_granule(line):
                    h, v = self.hv_for_modis_granule(line.split('/')[-1])
                    queue.append("h%sv%s" % (h, v))
        else:
            logger.debug('Failed')

        return list(set(queue))

    def which_archival_years_for_daterange(self, start, finish):
        years = int(finish.year - start.year) + 1
        return [int(start.year) + i for i in range(0, years)]

    def netcdf_name_for_date_and_granule(self, when, hv):
        name = "{}{}_{}_{}{}".format(self.outputs["readings"]["path"],
                                     self.outputs["readings"]["prefix"],
                                     when,
                                     hv,
                                     self.outputs["readings"]["suffix"])
        logger.debug(name)
        return name

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
        [logger.debug(g) for g in list(set(granules))]

        missing = [m for m in list(set(granules)) if not Path(m).is_file()]

        if len(missing) > 0:
            logger.debug('Some granules are missing:')
            logger.debug(missing)
            logger.debug('Gathering missing granules...')

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
            logger.debug('Granules are OK.')
            return granules

    # ShapeQuery

    async def get_shaped_resultcube(self, shape_query: ShapeQuery) -> xr.DataArray:
        sr = None
        lat1, lon1, lat2, lon2 = shape_query.spatial.expanded(0.1)
        # logger.debug('BL: %3.3f, %3.3f' % (lon1, lat1))
        # logger.debug('TR: %3.3f, %3.3f' % (lon2, lat2))
        # Eg., "108.0000,-45.0000,155.0000,-10.0000"  # Bottom-left, top-right
        bbox = "%3.3f,%3.3f,%3.3f,%3.3f" % (lon1, lat1, lon2, lat2)
        logger.debug("BBOX is: %s" % bbox)

        searcher = ['/FuelModels/Live_FM/LFMC_{}_*.nc'.format(year) for year in list(
            range(shape_query.temporal.start.year, shape_query.temporal.finish.year + 1))]

        search_res = []
        for searching in searcher:
            [search_res.append(s) for s in glob.glob(searching)]

        collection = list(set(search_res))

        logger.debug('Files to open are...')
        logger.debug(collection)

        strs = []

        for c in collection:
            ds = xr.open_dataset(c)
            s_t_r = ds['lfmc'].sel(lat=slice(lat2, lat1), lon=slice(lon1, lon2), time=slice(
                shape_query.temporal.start, shape_query.temporal.finish))
            strs.append(s_t_r)

        dsa = xr.merge(strs)
        dsa['LFMC'] = dsa['lfmc']

        return dsa
