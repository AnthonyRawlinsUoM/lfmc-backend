from pathlib2 import Path
import json
import fiona
import fiona.crs
from fiona.crs import from_epsg
import sys
from pyproj import Proj, transform
import functools
import geojson
import pyproj
import shapely.geometry
import shapely.ops

omit = ['SHAPE_AREA', 'SHAPE_LEN']


class Conversion:

    @staticmethod
    def convert(shp):

        # Translate the path for this Docker Container's Volume context
        # shp = shp.replace('/mnt/queries/', '/FuelModels/queries/')
        shp = '/FuelModels/queries/' + shp

        geoj = shp.replace('.shp', '.json')

        if Path(geoj).is_file():
            contents = open(geoj, 'r')
            return json.loads(contents.read())

        if not Path(shp).is_file():
            raise FileNotFoundError("File not found.")
        else:
            with fiona.open(shp, 'r') as source:
                original = Proj(source.crs)
                destination = Proj(init='EPSG:4326')
                with fiona.open(
                        geoj,
                        'w',
                        driver='GeoJSON',
                        crs=fiona.crs.from_epsg(4326),
                        schema=source.schema.copy()) as sink:
                    for feat in source:
                        # sink.write(feat)
                        out_linear_ring = []
                        for point in feat['geometry']['coordinates'][0]:
                            long, lat = point
                            x, y = transform(original, destination, long, lat)
                            out_linear_ring.append((x, y))
                        feat['geometry']['coordinates'] = [out_linear_ring]
                        sink.write(feat)
        # Remove the default WGS84 crs to avoid GeoJSON import error on UI

        return geoj

    @staticmethod
    def convert_this(shp):
        shp = '/FuelModels/queries/' + shp
        geoj = shp.replace('.shp', '.json')

        if Path(geoj).is_file():
            contents = open(geoj, 'r')
            return json.loads(contents.read())

        if not Path(shp).is_file():
            raise FileNotFoundError("File not found.")
        else:
            with fiona.open(shp) as source:
                # Use the recipe from the Shapely documentation:
                # http://toblerity.org/shapely/manual.html
                project = functools.partial(pyproj.transform,
                                            pyproj.Proj(**source.crs),
                                            pyproj.Proj(init='epsg:4326'))

                features = []
                for f in source:
                    shape = shapely.geometry.shape(f['geometry'])
                    projected_shape = shapely.ops.transform(project, shape)

                    # Remove the properties we don't want
                    props = f['properties']  # props is a reference
                    for k in omit:
                        if k in props:
                            del props[k]

                    feature = geojson.Feature(id=f['id'],
                                              geometry=projected_shape,
                                              properties=props)
                    features.append(feature)

            fc = geojson.FeatureCollection(features)

            with open(geoj, 'w') as f:
                f.write(geojson.dumps(fc))

        return geojson.dumps(fc)


if __name__ == '__main__':
    Conversion.convert(sys.argv[1])
