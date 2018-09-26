from datetime import datetime as dt
from marshmallow import Schema, fields


class Query:
    def __init__(self):
        self.request_time = dt.now()
        self.status = 'active'
        self.response_time = dt.now()

    def logResponse(self):
        self.status = 'complete'
        self.response_time = dt.now()

    def cancel(self):
        self.status = 'cancelled'
        self.response_time = dt.now()

    def is_complete(self):
        if self.status == 'active':
            return True
        else:
            return False

    pass


class QuerySchema(Schema):
    request_time = fields.DateTime()
    response_time = fields.DateTime()
    status = fields.String()
