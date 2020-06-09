from threading import Thread
from spl_arch.scheduler.scheduler import Scheduler


class DemoScheduler(Scheduler):
    INIT = 0
    RUNNING = 1
    FINISH = 2
    ERROR = -1

    def __init__(self, name, exception_lock, command_exception, commands=None):
        super().__init__(name=name)
        self._commands = commands
        self._status = self.INIT
        self._exception_lock = exception_lock
        self._command_exception = command_exception

    def schedule(self, cmd_ops):
        self._commands, threads = cmd_ops, []
        self._commands[0].calculate()  # search first exec

        for cmd in self._commands[1:]:  # exec others in parallel
            threads.append(Thread(target=cmd.calculate))  # consider using threads
            threads[-1].start()

        self._status = self.RUNNING

        while True:
            self._exception_lock.acquire()

            if self._command_exception.get() is not None:
                self._status = self.ERROR
                print(self._command_exception.get())
                break

            finish = 0
            for thread in threads:
                if not thread.is_alive():
                    finish += 1

            if finish == len(threads):  # all cmds had processed
                self._status = self.FINISH
                break

    @property
    def status(self):
        return self._status
