import rx
from marshmallow import Schema, fields
from rx import Observable, Observer
from serve.lfmc.models.Model import Model, ModelSchema
from serve.lfmc.results.ModelResult import ModelResultSchema, ModelResult


class ModelObserver(Observer):
    """

    """
    def __init__(self):
        self.schema = ModelResultSchema()

    def on_next(self, mr):
        if isinstance(mr, ModelResult):
            logger.debug("Yes, have ModelResult:")
            logger.debug(self.schema.dumps(mr))
        else:
            logger.debug(mr)
        pass

    def on_error(self, error):
        logger.debug("Error: %s" % error)
        pass

    def on_completed(self):
        logger.debug("Complete.")
        pass
