import fiona
import geojson
import shapely
import pyproj
import pandas as pd
import numpy as np
import xarray as xr
import geopandas as gp
from rtree import index
from shapely.ops import polygonize, unary_union
from shapely.wkt import dumps, loads
from shapely.geometry import LineString, MultiLineString, Polygon, MultiPolygon, mapping, shape
from cartopy import crs as ccrs

from tabulate import tabulate

from serve.lfmc.results.DataPoint import DataPoint
from serve.lfmc.query.ShapeQuery import ShapeQuery

import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.debug("logger set to DEBUG")


class GeoQuery(ShapeQuery):

    # nswgrid = pyproj.Proj("+init=EPSG:")
    epsg3857 = pyproj.Proj("+init=EPSG:3857")
    australian = pyproj.Proj("+init=EPSG:3577")
    gda94 = pyproj.Proj("+init=EPSG:3112")  # Geoscience Australia Lambert
    vicgrid94 = pyproj.Proj("+init=EPSG:3111")
    wgs84 = pyproj.Proj("+init=EPSG:4326")

    def __init__(self, shape_query):
        self.query = shape_query
        self.idx = None

    def build_index(self, grids):
        self.idx = index.Index()
        logger.debug('Building Index()')
        for pos, poly in enumerate(grids):
            self.idx.insert(pos, poly.bounds)

    def pull_fishnet(self, results: gp.GeoDataFrame):

        moisture = results[['moisture_content']].values

        indices = ~pd.isnull(moisture)
        non_nans = (indices).sum(1)

        if len(non_nans) == 0:
            logger.debug(
                'All moisture values are NaN. No datapoints to gather.')
            logger.debug(
                tabulate(results[['moisture_content', 'weight']]))
        else:
            logger.debug(
                'Found {} cells with moisture.'.format(len(non_nans)))

            if results[['weight']].values[indices].sum() == 0:
                raise ValueError('Cell weights total zero.')
            else:
                # Used for GeoJSON via __geo_interface__
                # Confirmed Proj can be like... {'init': 'epsg:3857'} or... wgs84
                # areaGDF = gp.GeoDataFrame(results, crs=projection)
                area_weighted_average_mc = np.average(
                    moisture[indices], weights=results[['weight']].values[indices])
                mean_mc = np.nanmean(moisture)
                min_mc = np.nanmin(moisture)
                max_mc = np.nanmax(moisture)
                median_mc = np.nanmedian(moisture)
                std_mc = np.nanstd(moisture)
                median_mc = np.nanmedian(moisture)
                count_mc = len(moisture)  # Count NaN cells too??

                shape_stats.append((t, area_weighted_average_mc,
                                    mean_mc, min_mc, max_mc, std_mc, median_mc, count_mc))

        logger.debug('Done gathering stats over time.')
        final_stats = pd.DataFrame(shape_stats, columns=[
                                   'time', 'area_weighted_average_mc', 'mean_mc', 'min_mc', 'max_mc', 'std_mc', 'median_mc', 'count_mc'])
        final_stats = final_stats.set_index('time', inplace=True)

        logger.debug(tabulate(final_stats))

        dps = []
        logger.debug('Type of final_stats is: %s' % type(final_stats))
        logger.debug(tabulate(final_stats))
        logger.debug(final_stats.to_dataframe())

        for row in final_stats.to_dataframe().itertuples(index=True, name='Pandas'):
            # logger.debug(row.Index.isoformat().replace('.000000000', '.000Z'))
            dps.append(DataPoint(observation_time=row.Index.isoformat() + '.000Z',
                                 value=row.median_mc,
                                 mean=row.mean_mc,
                                 weighted_mean=row.area_weighted_average_mc,
                                 minimum=row.min_mc,
                                 maximum=row.max_mc,
                                 deviation=row.std_mc,
                                 median=row.median_mc,
                                 count=row.count_mc))
        return dps

    def cast_fishnet(self, projection, df) -> gp.GeoDataFrame:
        """ dataframe from dataframe  """

        logger.debug('Called cast fishnet.')
        cell_size = 1.0  # Default: 5 seconds

        # Get the largest nearest bounding box
        # (essentially just adding a buffer around the selection)
        bottom, left, top, right = self.query.spatio_temporal_query.spatial.expanded(
            cell_size)

        logger.debug("\nB: %s\n L: %s\n T: %s\n R: %s\n" %
                     (bottom, left, top, right))

        if 'latitude' in df.coords:
            lats = df['latitude'].where(df['latitude'] <= top, drop=True).where(
                df['latitude'] >= bottom, drop=True).values
            lons = df['longitude'].where(df['longitude'] <= right, drop=True).where(
                df['longitude'] >= left, drop=True).values
            # logger.debug(lons)
            # logger.debug(lats)
        elif 'lat' in df.coords:
            lats = df['lat'].where(df['lat'] <= top, drop=True).where(
                df['lat'] >= bottom, drop=True).values
            lons = df['lon'].where(df['lon'] <= right, drop=True).where(
                df['lon'] >= left, drop=True).values
            # logger.debug(lons)
            # logger.debug(lats)
        else:
            logger.debug("Can't determine coordinate naming conventions.")
        # lons = np.arange(140.9500, 149.9800, 0.05)
        # lats = np.arange(-34.0000, -39.1500, -0.05)

        # logger.debug(df)

        xl = len(lons) + 1
        yl = len(lats) + 1
        x = np.linspace(0, xl, xl)
        y = np.linspace(0, yl, yl)

        # logger.debug('Selections X Length: %s' % len(x))
        # logger.debug('DF lon range: %s to %s' %
        #              (df['lon'].min(), df['lon'].max()))
        # logger.debug('Selections Y Length: %s' % len(y))
        # logger.debug('DF lon range: %s to %s' %
        #              (df['lat'].min(), df['lat'].max()))

        hlines = [((lon1, lai), (lon2, lai))
                  for lon1, lon2 in zip(lons[:-1], lons[1:]) for lai in lats]
        vlines = [((loi, lat1), (loi, lat2))
                  for lat1, lat2 in zip(lats[:-1], lats[1:]) for loi in lons]

        # -|--|--|-
        # -|--|--|-
        # -|--|--|-
        # -|--|--|-
        # -|--|--|-

        grids = list(polygonize(MultiLineString(hlines + vlines)))

        # |-----------|
        # |--|--|--|--|
        # |--|--|--|--|
        # |--|--|--|--|
        # |--|--|--|--|
        # |--|--|--|--|
        # |-----------|

        # Do only once
        if self.idx is None:
            self.build_index(grids)

        weights = [[[pos, grids[pos], grids[pos].intersection(self.query.selections[i]),
                     (grids[pos].intersection(self.query.selections[i]).area / grids[pos].area)]
                    for pos in self.idx.intersection(self.query.selections[i].bounds)]
                   for i in range(0, len(self.query.selections))]

        agg_weights = dict()
        agg_bounds = dict()
        agg_geom = dict()

        # Build up from all the tiny pieces
        for w in weights:
            for pos, cell, sect, weight in w:
                if weight > 0:
                    agg_weights[pos] = agg_weights.get(pos, 0) + weight
                    agg_bounds[pos] = cell.bounds
                    agg_geom[pos] = unary_union(
                        [agg_geom.get(pos, Polygon()), sect])

        shape_stats = []
        dataframesList = []
        for t in sorted(df['time'].values):

            logger.debug('Doing timeslice...')

            final_weights = dict()
            mcs = dict()
            # Loop through the aggregated weightings for each cell
            for pos in agg_weights:
                weight = agg_weights[pos]
                left, bottom, right, top = agg_bounds[pos]
                if weight > 0:
                    if 'latitude' in df.coords:
                        cells = df.sel(time=t).sel(latitude=bottom,
                                                   longitude=left).values.flatten()
                    elif 'lat' in df.coords:
                        cells = df.sel(time=t).sel(lat=bottom,
                                                   lon=left).values.flatten()
                    final_weights[pos] = weight
                    mcs[pos] = cells[0]

            data = [t, [mcs[pos], final_weights[pos], agg_geom[pos].wkt]
                    for pos in final_weights]

            if len(data) > 0:
                # Create a dataframe of the results
                results = gp.GeoDataFrame(
                    data, columns=['time', 'moisture_content', 'weight', 'geometry'])
                dataframesList.append(results)

        rdf = gpd.GeoDataFrame(
            pd.concat(dataframesList, ignore_index=True), crs=dataframesList[0].crs)

        return rdf
