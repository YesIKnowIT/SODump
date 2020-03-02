import signal

class ProcessManager:
    def __init__(self, *workers):
        self.workers = list(workers)

        self.started = []
        self._install_handler()

    def _install_handler(self):
        def killall(*args):
            self.terminate()
            sigint(*args)

        sigint = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, killall)

    def terminate(self):
        started = self.started
        while started:
            worker = started.pop()
            worker.terminate()

    def start(self):
        for worker in self.workers:
            worker.start()
            self.started.append(worker)

    def __getitem__(self, index):
        return self.workers[index]

