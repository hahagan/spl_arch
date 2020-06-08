from spl_arch.stream.base_stream import BaseStream
import queue
from spl_arch.stream.stream_exception import StreamFinishException


class LocalMemoryQueue(BaseStream):
    def __init__(self, name, size=1):
        super().__init__(name)
        self._queue = queue.Queue(size)
        self._finish_in = False

    def pull(self, size=1):
        event = self._queue.get()
        if isinstance(event, StreamFinishException):
            raise event

        return event

    def push(self, event=None):
        if self.finish_in:
            raise StreamFinishException
        self._queue.put(event)

    @property
    def finish_in(self):
        return self._finish_in

    @finish_in.setter
    def finish_in(self, v):
        self.push(StreamFinishException("finish"))
        self._finish_in = True
