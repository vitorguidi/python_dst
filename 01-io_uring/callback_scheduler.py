
import dataclasses

@dataclasses.dataclass
class CQE:
    pass

@dataclasses.dataclass
class SQE:
    pass

@dataclasses.dataclass
class Task:
    pass

class IOUring:
    def __init__(self):
        self._cqe_queue = []
        self._sqe_queue = []

    def enqueue_cqe(self, cqe: CQE):
        self._cqe_queue.append(cqe)

    def dequeue_cqe(self):
        if not self._cqe_queue:
            return None
        return self._cqe_queue.pop(0)

    def enqueue_sqe(self, sqe: SQE):
        self._sqe_queue.append(sqe)

    def dequeue_sqe(self):
        if not self._sqe_queue:
            return None
        return self._sqe_queue.pop(0)

    def is_empty(self):
        return not self._sqe_queue and not self._cqe_queue

class Scheduler:
    """Juggles tasks, completion (CQE) and submission (SQE) events"""
    def __init__(self, io_uring: IOUring):
        self._tasks = []
        self._io_uring = io_uring

    def add_task(self, task):
        self._tasks.append(task)

    def run(self):
        while self._tasks or not self._io_uring.is_empty():
            cqe = self._io_uring.dequeue_cqe()
            if cqe:
                #spawn a task to enqueue
                pass
            sqe = self._io_uring.dequeue_sqe()
            if sqe:
                #spawn a task to generate the CQE
                pass
            task = self._tasks.pop(0)
            if task:
                # do work
                try:
                    print(task.send(None))
                    self._tasks.append(task)
                except StopIteration:
                    continue
io_uring = IOUring()
sched = Scheduler(io_uring)
def fun():
    yield 2


t = fun()
sched.add_task(t)
sched.run()


#PROBLEMA: Como tratar um CQE quando ele exige RPC?

# coro: yield cqe

# sched:
# cqe = coro.send(val)
# task = Task(handler(req))
# callback = lambda x : io_uring.enqueue_cqe(x)
# task.callback = callback
