from lfmc.models.ModelRegister import ModelRegister
from lfmc.models.rx.RegisterObserver import RegisterObserver

mr = ModelRegister()
ro = RegisterObserver()

mr.subscribe(ro)