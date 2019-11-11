import datetime
import requests as r
import os
import xarray as xr
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import geopandas as gpd
import geoviews as gv
import geoviews.feature as gf
import xarray as xr
import requests
from cartopy import crs
from pathlib import Path
import subprocess
import queue
import threading
from glob import glob as glob
from multiprocessing import Pool
from tabulate import tabulate

import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.debug("logger set to DEBUG")

from serve.lfmc.models.LiveFuel import LiveFuelModel

class LiveScraper:
    def __init__(self, path):
        self.path = path

    def data(self, fpath, granule, total_req, suffix):
        have = len(glob(fpath + '*' + granule + '*' + suffix))
        logger.debug("Have %s days (%3.2f%%), of data for %s." %
                     (have, (have / total_req * 100), granule))

    def percentage(self, fpath, granule, total_req, suffix):
        have = len(glob(fpath + '*' + granule + '*' + suffix))
        return (have / total_req * 100)


    def coord_from_granule(self, granule):
        parts = granule.split('v')
        h = parts[0].replace('h', '')
        v = parts[-1]
        return int(h), int(v)


    def formatted_grid(self, fpath, dataframe, granules, suffix):
        for granule in granules:
            h, v = coord_from_granule(granule)
            total = granules_in_file_len(required, granule)
            perc = "%3.2f%%" % percentage(fpath, granule, total, suffix)
            dataframe.at[v, h] = perc
        return dataframe


    def get_standard_set(self):
        logger.debug('Finding coordinate system references for granules.')

        with open('./FuelModels/Live_FM/modis/MODIS-minmax.txt', 'r') as req:
            first_granules = [line for line in req if '.A2000049' in line]

        logger.debug("[Found: %s of 25] expected granules." % len(first_granules))

        standard_coord_set = sorted(['./FuelModels/Live_FM/projd/' + r.split(
            '/')[-1].rstrip().replace('.hdf', '.nc') for r in first_granules])

        logger.debug("\nChecking the base coordinate system files...")
        for f in standard_coord_set:
            if not Path(f).is_file():
                logger.debug('[Missing] %s' % str(f))
            else:
                logger.debug('[OK] %s' % str(f))

        return standard_coord_set


    def homogenize_granule(self, current_granule, label, standard_set):
        '''
        Homogenize the coords
        There are tiny fluctuations in the precision of the lat/lon coordinates from gdal.
        For each file in a granule HV space, maintain the same coordinate system.
        '''

        logger.debug(
            '\nHomogenizing the coordinate space of granule: %s.' % (label))
        standardized_lats, standardized_lons = get_standard_coords(
            label, standard_set)

        fsize = len(current_granule)
        # Standardize the coord system
        for c, i in enumerate(current_granule):

            lfmc_path = i.replace('projd', 'lfmc')
            if not Path(lfmc_path).is_file():
                with xr.open_dataset(i) as ds:
                    ds['vari'].coords['lat'].data = standardized_lats.data
                    ds['vari'].coords['lon'].data = standardized_lons.data
                    ds.to_netcdf(i + '.tmp', mode='w', format='NETCDF4')
                    logger.debug("[%3.0f/%3.0f] %s" % (c, fsize, i))
                os.rename(i + '.tmp', i)
            else:
                logger.debug("[Skipping] %s %s exists." % (i, lfmc_path))

        logger.debug('HOMOGENIZATION of Granule: %s complete.' % (label))


    def get_standard_coords(self, label, standard_set):

        df = [ss for ss in standard_set if label in ss]

        if len(df) == 0:
            raise Error(
                'Standard coord data file for granule %s not found.' % label)
        elif len(df) > 1:
            logger.debug(df)
            raise Error('Too many files for standard coord data file.')
        else:
            with xr.open_dataset(df[0]) as ds:
                standardized_lats = ds['vari'].coords['lat']
                standardized_lons = ds['vari'].coords['lon']

            return standardized_lats, standardized_lons


    def derive_minmax(self, fourteen_years_data, label):
        logger.debug('Calculating MINMAX for Granule: %s' % (label))

        # Open everything (because of dask, this is lightweight)
        with xr.open_mfdataset(fourteen_years_data, chunks={'time': 1}, concat_dim='time') as ds:
            minmax_set = ds['vari'].sel(time=slice('2000-01-01', '2015-01-01'))
            logger.debug(minmax_set)

            # Minimum VARI over 14 years
            min_vari_g = './FuelModels/Live_FM/min_vari_' + label + '.nc'
            minmax_set.min(dim='time').to_netcdf(
                min_vari_g, mode='w', format='NETCDF4')

            # Maximum VARI over 14 years
            max_vari_g = './FuelModels/Live_FM/max_vari_' + label + '.nc'
            minmax_set.max(dim='time').to_netcdf(
                max_vari_g, mode='w', format='NETCDF4')
        logger.debug('MINMAX for Granule: %s complete.' % (label))


    def get_granule_from_file(self, fname):
        return str(fname).split('/')[-1].split('.')[-4]


    def derive_lfmc(self, collection, label):
        # Assumes sorted and homogenous granule collection...
        logger.debug(78 * "*", "\n")
        logger.debug('[%s] Deriving LFMC values' % label)
        logger.debug(78 * "*", "\n")
        max_vari_g = './FuelModels/Live_FM/max_vari_' + label + '.nc'
        min_vari_g = './FuelModels/Live_FM/min_vari_' + label + '.nc'

        with xr.open_dataset(max_vari_g) as max_vari_ds:
            MAX_VARI = max_vari_ds['vari']

        with xr.open_dataset(min_vari_g) as min_vari_ds:
            MIN_VARI = min_vari_ds['vari']

        MAX_MINUS_MIN_VARI = MAX_VARI.data - MIN_VARI.data

        for f in collection:
            lfmc_path = f.replace('projd', 'lfmc')
            if not Path(lfmc_path).is_file():
                with xr.open_dataset(f) as ds:
                    # Duplicate the variable (we'll update the data next step)
                    ds['lfmc'] = ds['vari']
                    RVARI = (ds['vari'].data - MIN_VARI.data) / MAX_MINUS_MIN_VARI
                    # clamp between 0-1
                    RVARI = np.clip(RVARI, 0, 1)
                    ds['lfmc'].data = 52.51**(1.36 * RVARI)
                    ds['lfmc'].attrs['granule'] = label
                    ds['lfmc'].to_netcdf(lfmc_path, mode='w', format='NETCDF4')
                    logger.debug('Wrote: %s' % (lfmc_path))
            else:
                logger.debug("[File already exists.] %s" % (lfmc_path))

        logger.debug(78 * "*", "\n")
        logger.debug('[%s] LFMC calculations complete.' % label)
        logger.debug(78 * "*", "\n")


    def consolidate(self, ds, label):
        for year in range(2000, 2020):
            start = str(year) + '-01-01'
            end = str(year + 1) + '-01-01'
            fpath = './FuelModels/Live_FM/LFMC_' + str(year) + '_' + label + '.nc'
            if not Path(fpath).is_file():
                ds['lfmc'].sel(time=slice(start, end)).to_netcdf(
                    fpath, mode='w', format='NETCDF4')
                logger.debug('Wrote: ' + fpath)
        logger.debug('Done')


    def file_len(self, fname):
        with open(fname) as f:
            for i, l in enumerate(f):
                pass
        return i + 1


    def generate_query(self):
        product = "MOD09A1"
        version = "6"
        # AIO bounding box lower left longitude, lower left latitude, upper right longitude, upper right latitude.
        bbox = "108.0000,-45.0000,155.0000,-10.0000"
        modis_meta = product, version, bbox
        """
        Uses USGS service to match spatiotemporal query to granules required.
        converts each granule name to LFMC name
        """
        product, version, obbox = modis_meta
        dfiles = []

        rurl = "https://lpdaacsvc.cr.usgs.gov/services/inventory?product=" \
            + product \
            + "&version=" \
            + version \
            + "&bbox=" \
            + bbox \
            + "&date=" \
            + "2015-01-01" \
            + ',' \
            + "2019-10-10" \
            + "&output=text"

        path = './FuelModels/Live_FM/modis/MODIS-todate.txt'

        return path, rurl


    def get_usgs_list():
        try:
            path, rurl = generate_query()
            p = subprocess.run(
                ['curl', rurl, '-f', '-o', str(path)], shell=False, check=True)

        except e:
            msg = '500 - An unspecified error has occurred.\n'
            if hasattr(e, 'reason'):
                msg += 'We failed to reach a server.\n'
                msg += 'Reason: %s\n' % e.reason
            if hasattr(e, 'code'):
                msg += 'The server could not fulfill the request.\n'
                msg += 'Error code: %s\n' % e.code

        logger.debug('\n----> Download complete.\n')


    def granules_in_file_len(fname, test):
        a = 0
        with open(fname) as f:
            for i, l in enumerate(f):
                if test in l:
                    a += 1
        return a + 1


    def year_up_to_date(year, label):
        firstjan = datetime.datetime(year, 1, 1).strftime('%Y-%m-%d')
        today = datetime.datetime.now().strftime('%Y-%m-%d')

        """
         * Uses USGS service to match spatiotemporal query to granules required.
         * converts each granule name to LFMC name
        """

        url = "https://lpdaacsvc.cr.usgs.gov/services/inventory?product=" + product + "&version=" + \
            version + "&bbox=" + bbox + "&date=" + firstjan + ',' + today + "&output=text"
        path = self.path + "modis/"

        res = r.get(url)

        outfile = './FuelModels/Live_FM/modis/granules_ytd.txt'

        if res.status_code == requests.codes.ok:

            with open(outfile, 'w') as out:
                [out.write("%s" % g) for g in res.text]
                logger.debug('Wrote %s' % outfile)
            return res.text
        else:
            return []


    def generate_processing_bins():
        missing = []
        unprojected = []
        incomplete = []

        projd = './FuelModels/Live_FM/projd/'
        modis = './FuelModels/Live_FM/modis/'
        lfmc = './FuelModels/Live_FM/lfmc/'
        path = './FuelModels/Live_FM/modis/MODIS-minmax.txt'

        with open(path, 'r') as f:
            for l in f:
                fname = str(l).split('/')[-1].rstrip()

                if not Path(modis).joinpath(fname).is_file():
                    missing.append(l)

                if not Path(projd).joinpath(fname.replace('.hdf', '.nc')).is_file():
                    unprojected.append(Path(modis).joinpath(fname))

                if not Path(lfmc).joinpath(fname.replace('.hdf', '.nc')).is_file():
                    incomplete.append(Path(projd).joinpath(
                        fname.replace('.hdf', '.nc')))

        path = './FuelModels/Live_FM/modis/MODIS-todate.txt'
        with open(path, 'r') as f:
            for l in f:
                fname = str(l).split('/')[-1].rstrip()

                if not Path(modis).joinpath(fname).is_file():
                    missing.append(l)

                if not Path(projd).joinpath(fname.replace('.hdf', '.nc')).is_file():
                    unprojected.append(Path(modis).joinpath(fname))

                if not Path(lfmc).joinpath(fname.replace('.hdf', '.nc')).is_file():
                    incomplete.append(Path(projd).joinpath(
                        fname.replace('.hdf', '.nc')))

        logger.debug("%4.0f are missing." % len(missing))
        logger.debug("%4.0f are unprojected." % len(unprojected))
        logger.debug("%4.0f are incomplete" % len(incomplete))

        return missing, unprojected, incomplete


    def granulize():
        unprojected = []

        with open('./FuelModels/Live_FM/modis/MODIS-todate.txt', 'r') as req:
            [unprojected.append(line) for line in req]

        with open('./FuelModels/Live_FM/modis/MODIS-minmax.txt', 'r') as req:
            [unprojected.append(line) for line in req]

        logger.debug(len(unprojected))
        labels = list(set([get_granule_from_file(line) for line in unprojected]))

        fname = './FuelModels/Live_FM/granules.txt'
        with open(fname, 'w') as f:
            [f.write("%s\n" % n) for n in labels]
        logger.debug("Wrote: %s" % fname)
        logger.debug(sorted(labels))
        missing, unprojected, incomplete = generate_processing_bins()

        un = [str(x).rstrip() for x in unprojected]

        for label in sorted(labels):
            logger.debug("Doing %s" % label)
            grouped = [str(x).split('/')[-1] for x in un if label in x]
            fname = './FuelModels/Live_FM/' + label + '.txt'
            with open(fname, 'w') as f:
                [f.write("%s\n" % n) for n in grouped]
            logger.debug("Wrote %s" % fname)
        logger.debug("Done.")


    def minmax_exists(label):
        max_vari_g = './FuelModels/Live_FM/max_vari_' + label + '.nc'
        min_vari_g = './FuelModels/Live_FM/min_vari_' + label + '.nc'
        return Path(min_vari_g).is_file() and Path(max_vari_g).is_file()


    def do_work(labels):
        for label in labels:

            if not minmax_exists(label):
                with open('./FuelModels/Live_FM/modis/MODIS-minmax.txt', 'r') as req:
                    minmax_files = [line for line in req if label in line]
                    minmax_files_list = ['./FuelModels/Live_FM/projd/' + v.split(
                        '/')[-1].rstrip().replace('hdf', 'nc') for v in minmax_files]

                missing = preflight(minmax_files_list)
                if type(missing) == list:
                    with Pool(6) as pool:
                        pool.map(transformR, missing)
                        pool.close()
                        pool.join()

                # Same coords for all
                homogenize_granule(minmax_files_list, label, standard_set)

                # Get MIN & MAX VARI from 14 year period
                derive_minmax(minmax_files_list, label)

            with open('./FuelModels/Live_FM/modis/granules_ytd.txt', 'r') as req:
                rest = [line for line in req if label in line]
                rest_list = ['./FuelModels/Live_FM/projd/' +
                             v.split('/')[-1].rstrip().replace('hdf', 'nc') for v in rest]

            missing = preflight(rest_list)
            if type(missing) == list:
                with Pool(6) as pool:
                    pool.map(transformR, missing)
                    pool.close()
                    pool.join()

            # Same coords for all
            homogenize_granule(rest_list, label, standard_set)

            # Get the LFMC for ALL files for this granule label
            derive_lfmc(rest_list, label)

            with xr.open_mfdataset(glob(self.path + "lfmc/*." + label + ".*.nc")) as ds:
                consolidate(ds, label)


    def preflight(required):
        missing = [l for l in required if not Path(l).is_file()]
    #     [logger.debug("[Missing] %s" % (m)) for m in missing]
        return [m.split('/')[-1].replace('.nc', '.hdf') for m in missing]


    def transformR(file):
        logger.debug('Transforming %s' % file)
        r = requests.post("http://transformr:8000/modis_to_ncdf", data={
            "thefile": file
        })
        return r.text


    def years(label='h29v12'):
        with xr.open_mfdataset(glob(self.path + "lfmc/*." + label + ".*.nc")) as ds:
            consolidate(ds, label)
        logger.debug('Done.')
