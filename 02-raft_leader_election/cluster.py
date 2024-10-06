from commons import Clock, RNG
from raft import RaftNode, RaftOracle
from dispatcher import Dispatcher
from scheduler import Scheduler
from io_uring import IOUring
from task import Task

class Cluster:
    def __init__(self, nr_nodes, seed):
        self._clock = Clock()
        self._rng = RNG(seed)
        self._nodes = []
        for i in range(nr_nodes):
            node = RaftNode(i, self._clock, nr_nodes)
            self._nodes.append(node)
        dispatcher = Dispatcher(self._nodes)
        self._oracle = RaftOracle(self._nodes)
        self._sched = Scheduler(IOUring(), dispatcher, self._clock)
        for node in self._nodes:
            self._sched.add_task([Task(node.raft_loop(), None, f'node_{node._node_id}_raft_loop)'), None])

    def start(self, tick_limit = None):
        tick = 0
        while True:
            if tick_limit is not None and tick > tick_limit:
                break
            self._sched.tick()
            invariant_violations = self._oracle.assert_invariants()
            if invariant_violations != []:
                print('On the round {tick} of simulation, the following invariant violations happened:')
                for violation in invariant_violations:
                    print(violation)
                break
            tick+=1

if __name__ == '__main__':
    raft_cluster = Cluster(3, 0)
    raft_cluster.start(tick_limit = 1000000)