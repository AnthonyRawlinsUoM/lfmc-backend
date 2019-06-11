from rx import Observer


class RegisterObserver(Observer):
    def on_next(self, value):
        logger.debug(value)
        pass

    def on_error(self, error):
        logger.debug("Error: %s" % error)
        pass

    def on_completed(self):
        logger.debug("Complete")
        pass