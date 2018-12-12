class Filter:
    def __init__(self, model_subset):
        self.models = model_subset
        
    def get_models(self):
        return self.models