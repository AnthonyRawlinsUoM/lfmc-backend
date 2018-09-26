from marshmallow import Schema, fields


class CoverageStore:
    """
    See: http://docs.geoserver.org/latest/en/api/#/1.0.0/coveragestores.yaml

    {
      "coverageStore": {
        "name": "nyc",
        "url": "file:/path/to/file.tiff"
      }
    }
    """
    def __init(self, name, url):
        self.name = name
        self.url = url


class CoverageStoreSchema(Schema):
    name = fields.String()
    url = fields.String()