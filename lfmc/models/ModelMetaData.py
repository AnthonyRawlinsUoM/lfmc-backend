from marshmallow import Schema, fields

from lfmc.results.Abstracts import AbstractSchema
from lfmc.results.Author import AuthorSchema


class ModelMetaData:
    def __init__(self, authors, abstract, published_date, fuel_types, doi):
        """Short summary.

        Parameters
        ----------
        authors : type
                Description of parameter `authors`.
        published_date : type
                Description of parameter `published_date`.
        fuel_types : type
                Description of parameter `fuel_types`.

        Returns
        -------
        type
                Description of returned object.

        """
        self.authors = authors
        self.abstract = abstract
        self.published_date = published_date
        self.fuel_types = fuel_types
        self.doi = doi


class ModelMetaDataSchema(Schema):
    authors = fields.Nested(AuthorSchema, many=True)
    abstract = fields.Nested(AbstractSchema, many=False)
    published_date = fields.Date()
    fuel_types = fields.String()
    doi = fields.String()
