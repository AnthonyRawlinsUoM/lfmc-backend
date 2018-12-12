from rx import Observable


# class ModelAdaptor(Observable):
class ModelAdaptor:
    def __init__(self, model, **adapted_methods):
        super().__init__()
        self.model = model

        for key, value in adapted_methods.items():
            func = getattr(self.model, value)
            self.__setattr__(key, func)

        self.initialised = True

    def __getattr__(self, attr):
        return getattr(self.model, attr)

    def __setattr__(self, key, value):
        if not self.initialised:
            super().__setattr__(key, value)
        else:
            setattr(self.model, key, value)
            # self.notify_observer(key=key, value=value)
