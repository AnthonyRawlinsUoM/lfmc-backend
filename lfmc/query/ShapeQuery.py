from lfmc.query.Query import Query, QuerySchema
from marshmallow import Schema, fields
from lfmc.query.SpatioTemporalQuery import SpatioTemporalQuery, SpatioTemporalQuerySchema
from lfmc.query.TemporalQuery import TemporalQuery, TemporalQuerySchema
from lfmc.query.SpatialQuery import SpatialQuery, SpatialQuerySchema
import pickle
import geopandas as gp
import numpy as np
import pandas as pd
import xarray as xr
import regionmask
import json
import geojson
# import glob
# import time
import cv2
from numpy import asarray
from scipy.spatial import ConvexHull
# import cartopy.feature as cfeature
import cartopy.crs as ccrs
import shapely
# from shapely.wkt import dumps, loads
from shapely.geometry import Polygon, mapping, shape
# from shapely import affinity
from affine import Affine

# import fiona
# from fiona.crs import from_epsg
#
# import rasterio
# import rasterio.mask
# from rasterio import features
from rasterio.features import rasterize
import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.debug("logger set to DEBUG")


class ShapeQuery(SpatialQuery, TemporalQuery):

    def __init__(self, start, finish, geo_json, weighted=False):

        self.weighted = weighted

        if type(geo_json) == type(str()):
            # try:
            self.geo_json = geojson.loads(geo_json)
            # except:
            #     try:
            #         # Url Encoded?
            #         self.geo_json = geojson.loads(urldecode(geojson))
            #     except:
            #         pass
        elif type(geo_json) == type(dict()):
            self.geo_json = geojson.loads(geojson.dumps(geo_json))

        elif type(geo_json) == geojson.feature.FeatureCollection:
            self.geo_json = geo_json

        else:
            logger.debug("Couldn't parse GeoJSON.")
            # logger.debug("We got:", type(geo_json))
            raise ValueError("Couldn't parse GeoJSON: " + geo_json)

        lon1 = 180
        lon2 = -180
        lat1 = 90
        lat2 = -90

        # A regionmask Object
        # A list of the Shapely.Polygons

        selections = list()
        count = 0
        numbers = []
        names = []
        abbrevs = []

        for p in self.geo_json["features"]:

            min_lon, min_lat, max_lon, max_lat = self.bbox(p)

            lat1 = min(min_lat, lat1)
            lat2 = max(max_lat, lat2)
            lon1 = min(min_lon, lon1)
            lon2 = max(max_lon, lon2)

            logger.debug("Found a Feature.")
            if p["geometry"]["type"] == "Polygon" or p["geometry"]["type"] == "MultiPolygon":
                logger.debug("Found Polygon/MultiPolygon #%s" % count)
                s = shape(p["geometry"])
                selections.append(s)
                numbers.append(count)
                names.append("Selection_%s" % count)
                abbrevs.append("SEL_%s" % count)
                count += 1

        logger.debug("Making Region Mask with %s Polygons." % count)
        logger.debug("numbers: %s" % numbers)
        logger.debug("names: %s" % names)
        logger.debug("abbrevs: %s" % abbrevs)
        logger.debug("selections: %s" % selections)

        self.rmask = regionmask.Regions_cls(
            0, numbers, names, abbrevs, selections)

        self.selections = selections
        logger.debug(["%s" % sel for sel in selections])

        # Do once and store
        # self.mask = self.get_super_sampled_mask()  # TODO - remove default creation of mask and require setting the transform according to dataset projection

        # hull = ShapeQuery.get_query_hull(self.rmask)
        # lat1, lon2, lat2, lon1 = hull.bounds

        self.spatio_temporal_query = SpatioTemporalQuery(
            lat1, lon1, lat2, lon2, start, finish)
        self.temporal = self.spatio_temporal_query.temporal
        self.spatial = self.spatio_temporal_query.spatial
        self.transform = [0.05, 0.0, 111.975, 0.0, -0.05, -9.974999999999994]
        self.schema = ShapeQuerySchema()

    def explode(self, coords):
        """Explode a GeoJSON geometry's coordinates object and yield coordinate tuples.
        As long as the input is conforming, the type of the geometry doesn't matter."""
        for e in coords:
            if isinstance(e, (float, int)):
                yield coords
                break
            else:
                for f in self.explode(e):
                    yield f

    def bbox(self, f):
        x, y = zip(*list(self.explode(f['geometry']['coordinates'])))
        return min(x), min(y), max(x), max(y)

    def apply_transform(self, transform, out_shape):
        self.mask = self.get_super_sampled_mask(transform, out_shape)

    def get_selections(self):
        return self.selections

    def weighted(self):
        return self.weighted

    def spatio_temporal_query(self):
        return self.spatio_temporal_query

    def geo_json(self):
        return self.geo_json

    def logResponse(self):
        self.temporal.logResponse()
        self.spatial.logResponse()

    @staticmethod
    def get_bbox(poly: shapely.geometry.Polygon):
        return list((poly.bounds[0], poly.bounds[2], poly.bounds[1], poly.bounds[3]))

    @staticmethod
    def get_query_hull(mask):
        points = np.concatenate(mask.coords, axis=0)
        if not np.any(np.isnan(points)):
            hull = ConvexHull(points)
            return shapely.geometry.Polygon([hull.points[vertex] for vertex in hull.vertices])
        else:
            logger.debug('HullError: Mask coords contain Nans.')
            logger.debug('Points were: \n%s\n' % (list(points)))
            raise ValueError(
                'Cannot make a hull around points that contain NaNs')
            return None

    @staticmethod
    def get_corners(poly: shapely.geometry.Polygon):
        bb = ShapeQuery.get_bbox(poly)
        return [(bb[0], bb[2]), (bb[0], bb[3]), (bb[1], bb[3]), (bb[1], bb[2])]

    def get_buffered_coords(self, poly, kilometers):
        return self.transform_to_meters(poly).buffer(kilometers)

    def transform_to_meters(self, poly):
        # TODO
        affine = Affine(*self.transform)
        new_points = [~affine * point for point in asarray(poly.exterior)]
        #     logger.debug(new_points)
        return shapely.geometry.Polygon(new_points)

    def transform_to_latlong(self, poly):
        # TODO
        affine = Affine(*self.transform)
        return shapely.geometry.Polygon([affine * (point) for point in asarray(poly.exterior)])

    def as_buffered(self, poly, kilometers):
        return self.transform_to_latlong(self.get_buffered_coords(poly, kilometers))

    def get_super_sampled_mask(self, transform=None,
                               data_shape=(691, 886)):

        if transform is not None:
            scaled_transform = transform
        else:
            scaled_transform = self.transform

        scaled_transform[0] /= 10
        scaled_transform[4] /= 10
        mask_shape = (10 * data_shape[0], 10 * data_shape[1])
        rparams = dict(
            transform=scaled_transform,
            out_shape=mask_shape
        )
        # selection rasterization using rasterio!
        raster = rasterize(self.selections, **rparams)
        out_shape = (data_shape[1], data_shape[0])
        # Use OpenCV to interpolate back to the correct dimensions
        # This effectively 'super-samples' the mask
        resized = cv2.resize(raster.astype(np.float32), out_shape,
                             interpolation=cv2.INTER_AREA)
        return resized

    def apply_mask_to(self, result_cube: xr.DataArray) -> (xr.DataArray, xr.DataArray):
        rc = result_cube[result_cube.attrs['var_name']].isel(time=0)
        s = rc.shape
        pickled_aussie = open('./FuelModels/australia.pickle', 'rb')
        australia = pickle.load(pickled_aussie)
        numbers = [0]
        name = ['Australia']
        abbrev = ['AU']
        AUmask = regionmask.Regions_cls(
            'AUmask', numbers, name, abbrev, [australia.polygon])
        au_scaled_mask = AUmask.mask(rc['longitude'], rc['latitude'])
        au_masked = np.ma.masked_invalid(au_scaled_mask)
        result_cube = result_cube.where(au_masked == 0)
        mask = self.get_super_sampled_mask(transform=None, data_shape=s)
        result_cube['mask'] = xr.DataArray(mask, coords=[result_cube['latitude'].data, result_cube['longitude'].data],
                                           dims=['latitude', 'longitude'])
        return result_cube, result_cube['mask']


class ShapeQuerySchema(SpatioTemporalQuerySchema):
    spatio_temporal = fields.Nested(SpatioTemporalQuerySchema)
    geo_json = fields.String()
