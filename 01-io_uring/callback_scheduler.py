
import dataclasses
from enum import Enum

@dataclasses.dataclass
class CQE:
    data: any
    callback: any #function that enqueues the correct task with result

@dataclasses.dataclass
class SQE:
    data: any
    callback: callable

class Node:
    def __init__(self, node_id: int, node_count: int):
        self._node_id = node_id
        self._node_count = node_count

    def handle_simple_rpc(self):
        yield #coroutine starting protocol
        yield self._node_id

    def handle_complex_rpc(self):
        yield #coroutine starting protocol
        count = 0
        print(f'running complex rpc in node {self._node_id}')
        for node in range(self._node_count):
            request = {'rpc': 'handle_simple_rpc', 'target_node': node}
            print(f'node {self._node_id} yield req {request}')
            yield SQE(data=request, callback=None)
            count += 1
        print(f'exiting complex rpc for node {self._node_id}')

class Task:
    def __init__(self, coro, callback, name):
        self._name = name
        self._coro = coro
        next(coro)
        self._last_returned_value = None
        self._callback = callback
    
    def run(self, arg):
        result = self._coro.send(arg)
        self._last_returned_value = result
        print(f'running task whose coro is {self._coro}, got result {result}')
        return result
    
    def callback(self):
        if self._callback:
            assert self._last_returned_value is not None
            print(f'self.callback = {self._callback}, retval = {self._last_returned_value}')
            self._callback(self._last_returned_value)

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
    def __init__(self, io_uring: IOUring, nodes: any):
        self._tasks = []
        self._io_uring = io_uring
        self._nodes = nodes
        self._rpc_mapping = {}
        for node in nodes:
            self._rpc_mapping[node._node_id] = {
                'handle_simple_rpc': node.handle_simple_rpc,
            }

    def add_task(self, task):
        self._tasks.append(task)

    # Hypothesis: we only yield to ask for syscalls
    def run(self):
        while self._tasks or not self._io_uring.is_empty():
            # weave things with callback = lambda x : self._cqe.enqueue(x)
            [task, arg] = self._tasks.pop(0)
            assert type(task) == Task
            print(f'task = {task._coro}')
            try:
                result = task.run(arg)
                print(f'task coro = {task._coro}, arg = {arg}')
                if type(result) == SQE:
                    print('result = sqe')
                    # I need to bind current task value NOW otherwise this will bind
                    # to another value when coroutine stack dances around
                    def cqe_to_original_task_callback_factory(task):
                        return lambda cqe_data : self.add_task([task, cqe_data])
                    # enqueue IO request, with the proper callback to resume once CQE gets popped
                    result.callback = cqe_to_original_task_callback_factory(task)
                    # Bug = enqueing simple rpc instead of complex rpc, so inf loop
                    print(f'sqe to be enqueued = {result}')
                    self._io_uring.enqueue_sqe(result)
                else:
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
                def enqueue_cqe_callback_factory(sqe_callback):
                    return lambda x : self._io_uring.enqueue_cqe(CQE(x, sqe_callback))
                callback = enqueue_cqe_callback_factory(sqe.callback) 
                assert sqe is not None
                print(f'req = {req}')
                handler = self._rpc_mapping[req['target_node']]['handle_simple_rpc']()
                tgt = req['target_node']
                io_task = Task(handler, callback, f'handle_simple_rpc_node_{tgt}')
                self.add_task([io_task, req])

io_uring = IOUring()

nr_nodes = 3
nodes = []
for i in range(nr_nodes):
    nodes.append(Node(i, nr_nodes))
sched = Scheduler(io_uring, nodes)

sched.add_task( [Task(nodes[0].handle_complex_rpc(), None, 'complex_rpc_node_0'), None] )
sched.add_task( [Task(nodes[1].handle_complex_rpc(), None, 'complex_rpc_node_1'), None] )

sched.run()


#PROBLEMA: Como tratar um CQE quando ele exige RPC?

# coro: yield cqe

# sched:
# cqe = coro.send(val)
# task = Task(handler(req))
# callback = lambda x : io_uring.enqueue_cqe(x)
# task.callback = callback
# callback sempre vai ser uma lambda que resume a coro com resultado