import json
from marshmallow import Schema, fields
from lfmc.results.DataPoint import DataPoint, DataPointSchema


class ModelResult:
    def __init__(self, model_name: fields.String(), data_points: [DataPoint]):
        """Short summary.

        Parameters
        ----------
        model_name : type
                        Description of parameter `model_name`.
        data_points : type
                        Description of parameter `data_points`.
        Returns
        -------
        type
                        Description of returned object.
        """
        # An Array (1D) of DataPoints
        self.series = data_points
        self.name = model_name

    def __str__(self):
        schema = ModelResultSchema()
        return schema.dumps(self)


class ModelResultSchema(Schema):
    name = fields.String()
    series = fields.Nested(DataPointSchema, many=True)
