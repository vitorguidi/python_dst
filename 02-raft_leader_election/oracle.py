class Oracle:
    def assert_invariants(self, nodes):
        raise NotImplemented
    
class InvariantViolationException(Exception):
    def __init__(self, violations, round):
        message = f'Simulation failed on round {round} due to the following invariant violations: {violations}'
        super().__init__(message)

if __name__ == '__main__':
    raise InvariantViolationException(['batata', 'banana'], 10)