from lfmc.query.Query import Query


class RequestMonitor:
    class __RequestMonitor:
        def __init__(self):
            self.request_collection = []

        def __str__(self):
            return repr(self)

        def log_request(self, query: Query):
            self.request_collection.append(query)

        def all_requests(self):
            return {"all_requests": [r.schema.dumps(r) for r in self.request_collection]}

        def open_requests(self):
            return {"open_requests": [r.schema.dumps(r) for r in self.request_collection if not r.is_complete()]}

        def completed_requests(self):
            return {"completed_requests": [r.schema.dumps(r) for r in self.request_collection if r.is_complete()]}

    instance = None

    def __init__(self):
        if not RequestMonitor.instance:
            RequestMonitor.instance = RequestMonitor.__RequestMonitor()

    def __getattr__(self, name):
        return getattr(self.instance, name)

