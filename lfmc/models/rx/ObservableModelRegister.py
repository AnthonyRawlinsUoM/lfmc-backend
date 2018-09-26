import rx
from rx import Observable
from lfmc.config import debug as dev
from lfmc.models.Model import Model
from lfmc.query import ShapeQuery


class ObservableModelRegister(Observable):
    def __init__(self):
        self.sources = []
        pass

    # def apply_shape_query(self, query: ShapeQuery):
        # for m in self.models:
        #     self.sources.append(Observable.create(m.get_shaped_timeseries(query)))
        # observer.on_completed()

    def subscribe(self, observer):
        for model in self.models:
            model.subscribe(observer)
        pass