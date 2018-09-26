from lfmc.query.Query import Query, QuerySchema
from marshmallow import Schema, fields
import numpy as np
import math


class SpatialQuery(Query):
    """ SpatialQuery is a Bounding box defined by the NW/SE corners of a rectangular selection
    """

    def __init__(self, lat1, lon1, lat2, lon2):
        """Short summary.

        Parameters
        ----------
        lat1 : type
            Description of parameter `lat1`.
        lon1 : type
            Description of parameter `lon1`.
        lat2 : type
            Description of parameter `lat2`.
        lon2 : type
            Description of parameter `lon2`.

        Returns
        -------
        type
            Description of returned object.

        """

        self.lat1 = lat1
        self.lon1 = lon1
        self.lat2 = lat2
        self.lon2 = lon2

        self.schema = SpatialQuerySchema()

    def restricted(self, tolerance):
        lat1 = SpatialQuery.round_up(np.float64(self.lat1), tolerance)
        lon1 = SpatialQuery.round_up(np.float64(self.lon1), tolerance)
        lat2 = SpatialQuery.round_down(np.float64(self.lat2), tolerance)
        lon2 = SpatialQuery.round_down(np.float64(self.lon2), tolerance)
        return lat1, lon1, lat2, lon2

    def nearest(self, tolerance):
        lat1 = SpatialQuery.round_nearest(np.float64(self.lat1), tolerance)
        lon1 = SpatialQuery.round_nearest(np.float64(self.lon1), tolerance)
        lat2 = SpatialQuery.round_nearest(np.float64(self.lat2), tolerance)
        lon2 = SpatialQuery.round_nearest(np.float64(self.lon2), tolerance)
        return lat1, lon1, lat2, lon2

    def expanded(self, tolerance):
        lat1 = SpatialQuery.round_down(np.float64(self.lat1), tolerance)
        lon1 = SpatialQuery.round_down(np.float64(self.lon1), tolerance)
        lat2 = SpatialQuery.round_up(np.float64(self.lat2), tolerance)
        lon2 = SpatialQuery.round_up(np.float64(self.lon2), tolerance)
        return lat1, lon1, lat2, lon2

    @staticmethod
    def round_nearest(x, a):
        return round(round(x / a) * a, -int(math.floor(math.log10(a))))

    @staticmethod
    def round_down(x, a):
        return math.floor(x / a) * a

    @staticmethod
    def round_up(x, a):
        return math.ceil(x / a) * a

    pass


class SpatialQuerySchema(QuerySchema):
    lat1 = fields.Decimal(places=8, as_string=True)
    lon1 = fields.Decimal(places=8, as_string=True)
    lat2 = fields.Decimal(places=8, as_string=True)
    lon2 = fields.Decimal(places=8, as_string=True)


# sq = SpatialQuery(-10, 110, -45, 155)
# schema = SpatialQuerySchema()
# data, errors = schema.dump(sq)
#
# data
