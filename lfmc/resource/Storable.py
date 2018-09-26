from abc import abstractmethod


class Storable:

    @abstractmethod
    def object_exists(self, obj):
        return True

    @abstractmethod
    def store_object(self, obj):
        return True

    @abstractmethod
    def list_objects(self, path):
        return []

    # TODO - Object Deletion

    @abstractmethod
    def path_exists(self, path):
        return False

    @abstractmethod
    def file_exists(self, path):
        return False
