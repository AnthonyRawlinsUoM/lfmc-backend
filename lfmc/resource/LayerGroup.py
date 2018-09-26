from marshmallow import Schema, fields


class LayerGroup:
    def __init(self, name, url):
        self.name = name
        self.url = url


class LayerGroupSchema(Schema):
    name = fields.String()
    url = fields.String()
