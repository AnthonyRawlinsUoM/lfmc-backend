from serve.lfmc.models.rx import ModelObserver, ObservableModelRegister
from serve.lfmc.models.rx.ObservableModel import ObservableModel
from serve.lfmc.models.rx import RegisterObserver
source = ObservableModel()
mo = ModelObserver()
source.subscribe(mo)

omr = ObservableModelRegister()
ro = RegisterObserver()
omr.subscribe(ro)
