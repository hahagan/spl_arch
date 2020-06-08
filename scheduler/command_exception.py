class CommandException(Exception):
    def __init__(self, lock):
        self._msg = None
        self._lock = lock

    def get(self):
        return self._msg

    def set(self, msg: str):
        self._msg = msg
        if self._lock.locked():
            self._lock.release()

    def __str__(self):
        return self.get()[0]
