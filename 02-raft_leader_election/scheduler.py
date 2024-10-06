from io_uring import IOUring, SQE, CQE
from dispatcher import Dispatcher
from rpc import Request, Response
from task import Task
from commons import Clock, RNG

class Scheduler:
    """Juggles tasks, completion (CQE) and submission (SQE) events"""
    def __init__(self, io_uring: IOUring, dispatcher: Dispatcher, clock: Clock):
        self._tasks = []
        self._io_uring = io_uring
        self._dispatcher = dispatcher
        self._clock = clock


    def add_task(self, task):
        self._tasks.append(task)

    # Assumption: we only yield to ask for syscalls, and always return a value
    # in the end. Coroutines are a narrower set than generators
    def run(self):
        while self._tasks or not self._io_uring.is_empty():
            [task, arg] = self._tasks.pop(0)
            assert type(task) == Task
            print(f'dequeued task = {task._name}')
            try:
                result = task.run(arg)
                self._clock.tick()
                print(f'got result {result} running task {task._name}')
                # We only allow nodes to request RPCs, launch tasks or 
                # return a value
                if isinstance(result, Request):
                    print(f'got request{result}')
                    # I need to bind current task value NOW otherwise this will bind
                    # to another value when coroutine stack dances around
                    def cqe_to_original_task_callback_factory(task):
                        return lambda cqe_data : self.add_task([task, cqe_data])
                    # enqueue IO request, with the proper callback to resume once CQE gets popped
                    sqe = SQE(result, cqe_to_original_task_callback_factory(task))
                    print(f'enqueuing {sqe} for task {task._name} as CQE')
                    self._io_uring.enqueue_sqe(sqe)
                elif isinstance(result, Task):
                    def return_value_to_task_callback_factory(requester_task):
                        return lambda retval : self.add_task([requester_task, retval])
                    result._callback = return_value_to_task_callback_factory(task)
                    self.add_task([result, None])
                else:
                    print(f'Adding task back to the scheduler: {task._name}')
                    # Every coroutine that finishes is an RPC handler
                    # This is a sanity check
                    # Other coroutines are the raft loops, which should never stop
                    self.add_task([task, None])
            except StopIteration:
                task.callback()
            cqe = self._io_uring.dequeue_cqe()
            if cqe:
            # add task with result back to task queue
                cqe.callback(cqe.data)
            sqe = self._io_uring.dequeue_sqe()
            if sqe:
                print(f'dequeued sqe {sqe}')
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
                print(f'Enqueing IO task for SQE {sqe}')
                self.add_task([io_task, req])
                print(f'Enqueued task {io_task._name}')
