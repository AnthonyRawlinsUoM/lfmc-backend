from datetime import datetime as dt
from marshmallow import Schema, fields
import pandas as pd
from lfmc.query.Query import Query, QuerySchema
from dateutil.parser import parse


class TemporalQuery(Query):
    """
      TemporalQuery is a date range. All data in the temporal range is returned regardless of resolution.
    """

    def __init__(self, start, finish):
        self.start = parse(start)
        self.finish = parse(finish)

        self.schema = TemporalQuerySchema()

    def dates(self):
        return pd.date_range(self.start, self.finish)


class TemporalQuerySchema(QuerySchema):
    start = fields.Date()
    finish = fields.Date()
