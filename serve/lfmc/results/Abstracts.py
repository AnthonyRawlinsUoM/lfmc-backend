from marshmallow import Schema, fields


class Abstracts:
    def __init__(self, abstract: str):
        self.abstract = ' '.join(abstract.split())


class AbstractSchema(Schema):
    abstract = fields.String()
