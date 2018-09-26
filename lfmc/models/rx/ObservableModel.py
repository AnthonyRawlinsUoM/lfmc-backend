import rx
from rx import Observable, Observer
from lfmc.config import debug as dev

import datetime as dt
from marshmallow import Schema, fields

from lfmc.results.DataPoint import DataPoint
from lfmc.results.ModelResult import ModelResult


class ObservableModel(Observable):
    name = "ObservableModel"

    def __init__(self):
        pass

    def subscribe(self, observer):
        if dev.DEBUG:
            dps = []
            for i in range(3):
                dps.append(DataPoint(observation_time=dt.date(2018, 5, 17),
                                     value=i,
                                     mean=i,
                                     minimum=i,
                                     maximum=i,
                                     deviation=0
                                     ))
                observer.on_next(ModelResult(model_name='test', data_points=dps))
            observer.on_completed()
        else:
            dps = []

        pass


class ObservableModelSchema(Schema):
    name = fields.String()
