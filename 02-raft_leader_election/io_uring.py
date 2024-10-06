import dataclasses

@dataclasses.dataclass
class CQE:
    data: any
    callback: any #function that enqueues the correct task with result

@dataclasses.dataclass
class SQE:
    data: any
    callback: callable

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
