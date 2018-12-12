from serve.lfmc.models.ModelRegister import ModelRegister
from serve.lfmc.models.rx.RegisterObserver import RegisterObserver

mr = ModelRegister()
ro = RegisterObserver()

mr.subscribe(ro)