class Oracle:
    def assert_invariants(self, nodes):
        raise NotImplemented
    
class InvariantViolationException(Exception):
    def __init__(self, violations, round, seed, nr_nodes, task_delay, rpc_delay):
        message = f'Simulation failed on round {round} (seed={seed}, nr_nodes={nr_nodes}, task_delay={task_delay}, rpc_delay={rpc_delay}) due to the following invariant violations: {violations}'
        super().__init__(message)

if __name__ == '__main__':
    raise InvariantViolationException(['batata', 'banana'], 10, 0, 0, 0, 0)