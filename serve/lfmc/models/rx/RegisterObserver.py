from rx import Observer


class RegisterObserver(Observer):
    def on_next(self, value):
        print(value)
        pass

    def on_error(self, error):
        print("Error: %s" % error)
        pass

    def on_completed(self):
        print("Complete")
        pass