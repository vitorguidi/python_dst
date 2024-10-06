from commons import Clock, RNG
from raft import RaftNode
from scheduler import Scheduler
from io_uring import IOUring
from dispatcher import Dispatcher
from task import Task

nr_nodes = 3
clock = Clock()
rng = RNG(0)
iouring = IOUring()
nodes = []
for i in range(nr_nodes):
    node = RaftNode(i, clock, nr_nodes)
    nodes.append(node)
dispatcher = Dispatcher(nodes)
sched = Scheduler(iouring, dispatcher, clock)
for node in nodes:
    sched.add_task([Task(node.raft_loop(), None, f'node_{node._node_id}_raft_loop)'), None])
sched.run()