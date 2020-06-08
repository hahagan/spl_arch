from abc import abstractmethod, ABCMeta


class AbstractStream(metaclass=ABCMeta):
    @abstractmethod
    def get(self, size=1):
        pass

    @abstractmethod
    def push(self, event):
        pass


class StreamException(Exception):
    pass


class StreamFinishException(StreamException):
    pass
