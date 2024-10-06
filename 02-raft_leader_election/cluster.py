from commons import Clock, RNG
from raft import RaftNode, RaftOracle
from dispatcher import Dispatcher
from scheduler import Scheduler
from io_uring import IOUring
from task import Task
from oracle import InvariantViolationException

class Cluster:
    def __init__(self, nr_nodes, seed, task_delay_time, rpc_delay_time):
        self._seed = seed
        self._nr_nodes = nr_nodes
        self._task_delay_time = task_delay_time
        self._rpc_delay_time = rpc_delay_time
        self._clock = Clock()
        self._rng = RNG(seed)
        self._nodes = []
        for i in range(nr_nodes):
            node = RaftNode(i, self._clock, nr_nodes)
            self._nodes.append(node)
        dispatcher = Dispatcher(self._nodes)
        self._oracle = RaftOracle(self._nodes)
        self._sched = Scheduler(IOUring(), dispatcher, self._clock, self._rng, task_delay_time, rpc_delay_time)
        for node in self._nodes:
            self._sched.add_task(Task(node.raft_loop(), None, f'node_{node._node_id}_raft_loop)'), None, task_delay_time)

    def start(self, tick_limit = None):
        tick = 0
        while True:
            if tick_limit is not None and tick > tick_limit:
                break
            self._sched.tick()
            invariant_violations = self._oracle.assert_invariants()
            if invariant_violations != []:
                raise InvariantViolationException(invariant_violations, tick, self._seed, self._nr_nodes, self._task_delay_time, self._rpc_delay_time)
            tick+=1

if __name__ == '__main__':
    # for i in range(1000):
    #     raft_cluster = Cluster(3, i, 5, 20)
    #     raft_cluster.start(tick_limit = 10000)
    raft_cluster = Cluster(3, 141, 5, 20)
    raft_cluster.start(tick_limit = 10000)
