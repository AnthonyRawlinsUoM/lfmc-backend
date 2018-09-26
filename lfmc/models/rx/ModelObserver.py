import rx
from marshmallow import Schema, fields
from rx import Observable, Observer
from lfmc.models.Model import Model, ModelSchema
from lfmc.results.ModelResult import ModelResultSchema, ModelResult


class ModelObserver(Observer):
    """

    """
    def __init__(self):
        self.schema = ModelResultSchema()

    def on_next(self, mr):
        if isinstance(mr, ModelResult):
            print("Yes, have ModelResult:")
            print(self.schema.dumps(mr))
        else:
            print(mr)
        pass

    def on_error(self, error):
        print("Error: %s" % error)
        pass

    def on_completed(self):
        print("Complete.")
        pass
