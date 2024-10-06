from io_uring import IOUring, SQE, CQE
from dispatcher import Dispatcher
from rpc import Request, Response
from task import Task
from commons import Clock, RNG
import dataclasses
import heapq

@dataclasses.dataclass
class TaskEntry:
    task: Task
    task_id: int
    dequeue_time: any
    argument: any

    def __str__(self) -> str:
        return f'[task={self.task._name} - tid={self.task_id} - dequeue_time={self.dequeue_time} - arg={self.argument}]'

    def __repr__(self) -> str:
        return self.__str__()

    def __lt__(self, other):
        """Custom comparison function for the min heap."""
        if self.dequeue_time == other.dequeue_time:
            return self.task_id < other.task_id
        return self.dequeue_time < other.dequeue_time

class Scheduler:
    """Juggles tasks, completion (CQE) and submission (SQE) events"""
    def __init__(self, io_uring: IOUring, dispatcher: Dispatcher, clock: Clock, rng: RNG, task_random_delay: int, rpc_random_delay):
        self._tasks = []
        self._io_uring = io_uring
        self._dispatcher = dispatcher
        self._clock = clock
        self._task_id = 0
        self._rng = rng
        self._task_random_delay = task_random_delay
        self._rpc_random_delay = rpc_random_delay

    def add_task(self, task, arg, delay):
        dequeue_time = self._clock.get_time() + self._rng.generate(delay)
        entry = TaskEntry(task=task, argument=arg, task_id=self._task_id, dequeue_time=dequeue_time)
        heapq.heappush(self._tasks, entry)
        self._task_id += 1

    def tick(self):
        self._tick_tasks()
        self._tick_iouring()
        self._clock.tick()

    def _tick_iouring(self):
        if self._io_uring.is_empty():
            return
        cqe = self._io_uring.dequeue_cqe()
        if cqe:
        # add task with result back to task queue
            cqe.callback(cqe.data)
        sqe = self._io_uring.dequeue_sqe()
        if sqe:
        # Spawn a task that will create a CQE whose callback gives data to originator coro
            req = sqe.data
            assert isinstance(req, Request)
            # Bind this NOW, otherwise the value of sqe or task will change
            # Once the coroutine leaves and comes back to this stack
            def enqueue_cqe_callback_factory(sqe_callback):
                return lambda x : self._io_uring.enqueue_cqe(CQE(x, sqe_callback))
            callback = enqueue_cqe_callback_factory(sqe.callback) 
            assert sqe is not None
            target_node = req.node_to
            rpc_name = req.rpc_name
            handler = self._dispatcher.get_handler_coro(req.rpc_name, req.node_to)
            io_task = Task(handler, callback, f'handle_{rpc_name}_rpc on node {target_node}')
            self.add_task(io_task, req, self._rpc_random_delay)

    def _tick_tasks(self):
        if not self._tasks:
            return
        if self._tasks[0].dequeue_time > self._clock.get_time():
            return
        task_entry = self._tasks.pop(0)
        assert type(task_entry) == TaskEntry
        task = task_entry.task
        arg = task_entry.argument
        assert type(task) == Task

        print(f'dequeued task = {task_entry}, at time {self._clock.get_time()}')
        try:
            result = task.run(arg)
            self._clock.tick()
            # We only allow nodes to request RPCs, launch tasks or 
            # return a value
            if isinstance(result, Request):
                # I need to bind current task value NOW otherwise this will bind
                # to another value when coroutine stack dances around
                def cqe_to_original_task_callback_factory(task):
                    # this is equivalent to the trip back of the rpc
                    return lambda cqe_data : self.add_task(task, cqe_data, self._rpc_random_delay)
                # enqueue IO request, with the proper callback to resume once CQE gets popped
                sqe = SQE(result, cqe_to_original_task_callback_factory(task))
                self._io_uring.enqueue_sqe(sqe)
            elif isinstance(result, Task):
                def return_value_to_task_callback_factory(requester_task):
                    return lambda retval : self.add_task(requester_task, retval, self._task_random_delay)
                result._callback = return_value_to_task_callback_factory(task)
                self.add_task(result, None, self._task_random_delay)
            else:
                # Every coroutine that finishes is an RPC handler
                # This is a sanity check
                # Other coroutines are the raft loops, which should never stop
                self.add_task(task, None, self._task_random_delay)
        except StopIteration:
            task.callback()

if __name__ == '__main__':
    def dummy_coro():
        yield
        yield 2
    t1 = TaskEntry(task=Task(dummy_coro(), None, 'random task 1'), task_id = 0, dequeue_time=0, argument=None)
    t2 = TaskEntry(task=Task(dummy_coro(), None, 'random task 2'), task_id = 1, dequeue_time=0, argument=None)
    t3 = TaskEntry(task=Task(dummy_coro(), None, 'random task 3'), task_id = 2, dequeue_time=1, argument=None)
    assert t1 < t2
    assert t2 < t3
    print(t3)