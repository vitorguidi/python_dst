from rpc import AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse, Request, Response
from commons import Clock, RNG
from enum import Enum
from task import Task
from oracle import Oracle
import functools

class RaftState(Enum):
    FOLLOWER=0
    CANDIDATE=1
    LEADER=2
    

class RaftNode:
    def __init__(self, node_id: int, clock: Clock, nr_nodes: int):
        self._clock = clock
        self._node_id = node_id
        self._state = RaftState.FOLLOWER
        self._term = 0
        self._voted_for = -1
        self._last_rpc_time = 0
        self._election_timeout = 10
        self._nr_nodes = nr_nodes

    def _log(self, message):
        identifier = f'[{self._node_id}][{self._state}][{self._term}] '
        print(f'{identifier}{message}')

    def _become_follower(self, term: int):
        self._term = term
        self._nr_votes = 0
        self._voted_for = -1
        self._state = RaftState.FOLLOWER
        self._log(f'node {self._node_id} is becoming a follower on term {self._term}')

    def _become_candidate(self):
        self._term += 1
        self._voted_for = -1
        self._nr_votes = 0
        self._last_rpc_time = self._clock.get_time()
        self._state = RaftState.CANDIDATE
        self._log(f'node {self._node_id} is becoming a candidate on term {self._term}')

    def _become_leader(self):
        self._state = RaftState.LEADER

    def handle_leader(self):
        yield
        self._log('handling_leader')
        for target_node in range(self._nr_nodes):
            request = AppendEntriesRequest(node_from=self._node_id, node_to=target_node, leader_id=self._node_id, term=self._term)
            response = yield request
            assert type(response) == AppendEntriesResponse
            if response.term > self._term:
                self._become_follower(response.term)

    def handle_follower(self):
        yield #Coro initialization protocol
        self._log('handling follower')
        if self._clock.get_time() >= self._last_rpc_time + self._election_timeout:
            self._log(f'becoming candidate on node {self._node_id}')
            self._become_candidate()
        return None  #For callbacks to work we always return something

    def handle_candidate(self):
        yield
        self._log('handling candidate')

        self._log(f'node {self._node_id} is starting election on term {self._term}')



        self._voted_for = self._node_id
        self._nr_votes = 1
        self._log(f'node {self._node_id} voted for iteself on term {self._term}, now has {self._nr_votes} votes')


        for target_node in range(self._nr_nodes):
            if target_node == self._node_id:
                continue
            request = RequestVoteRequest(
                node_from=self._node_id,
                node_to = target_node,
                term = self._term,
                candidate_id=self._node_id
            )
            response = yield request
            assert type(response) == RequestVoteResponse
            if response.term > self._term:
                self._become_follower(response.term)
                return None
            if response.vote_granted and response.term == self._term:
                self._log(f'Node {self._node_id} received a vote from node {target_node} on term {self._term}')
                self._nr_votes += 1
            
        if self._nr_votes > self._nr_nodes/2:
            self._become_leader()
            self._log(f'Node {self._node_id} became leader on term {self._term}, with {self._nr_votes} votes')
        yield None

    def raft_loop(self):
        # vitorguidi@Vitors-Air 02-raft_leader_election % grep "\[RaftState.FOLLOWER\]" logs.txt | grep "handling candidate"
        # [2][RaftState.FOLLOWER][67] handling candidate
        # [0][RaftState.FOLLOWER][72] handling candidate
        # RaftState out of sync with the handler coroutine, why?
        yield
        while True:
            self._log(f'doing one round of raft on node {self._node_id}')
            if self._state == RaftState.LEADER:
                leader_coro = self.handle_leader()
                yield Task(leader_coro, None, f'node_{self._node_id }_leader_coro')
            elif self._state == RaftState.FOLLOWER:
                follower_coro = self.handle_follower()
                yield Task(follower_coro, None, f'node_{self._node_id }_follower_coro')
            else:
                assert self._state == RaftState.CANDIDATE
                candidate_coro = self.handle_candidate()
                yield Task(candidate_coro, None, f'node_{self._node_id }_candidate_coro')

    def handle_append_entries_rpc(self):
        request = yield # Coro protocol
        assert isinstance(request, AppendEntriesRequest)
        response = AppendEntriesResponse(node_from=request.node_to, node_to=request.node_from, term=self._term)
        response.node_to = request.node_from
        response.node_from = self._node_id
        self._last_rpc_time = self._clock.get_time()

        response.term = self._term
        if request.term > self._term:
            self._become_follower(request.term)
            self._term = request.term
        yield response

    def handle_request_vote_rpc(self):
        request = yield
        self._log(type(request))
        assert isinstance(request, RequestVoteRequest)
        response = RequestVoteResponse(node_from=request.node_to, node_to=request.node_from, term=self._term, vote_granted=False)
        self._last_rpc_time = self._clock.get_time()

        if request.term > self._term:
            prev_term = self._term
            self._become_follower(request.term)
            self._voted_for = request.candidate_id
            response.term = self._term
            self._log(f'node {self._node_id} granted vote to node {request.node_from} on term {self._term}, after stepping down from round {prev_term}')
            response.vote_granted = True
        elif request.term < self._term:
            self._log(f'node {self._node_id} denied vote to node {request.node_from} on term {self._term}, because it was on smaller term {request.term}')
            response.vote_granted = False
        elif self._voted_for == -1 and self._state == RaftState.FOLLOWER:
            self._log(f'node {self._node_id} granted vote to node {request.node_from} on term {self._term}, because it still did not vote this term')
            self._voted_for = request.candidate_id
            response.vote_granted = True
        else:
            response.vote_granted = False
        
        response.term = self._term
        yield response

class RaftOracle(Oracle):
    def __init__(self, nodes):
        self._nodes = nodes
        self._leader_history = {}
        self._assertion_failures = []

    def assert_invariants(self):
        self._assert_election_safety_invariant()
        return self._assertion_failures

    def _assert_election_safety_invariant(self):
        for node in self._nodes:
            node_term = node._term
            node_id = node._node_id
            node_state = node._state
            if node_state != RaftState.LEADER:
                continue
            if not node_term in self._leader_history:
                self._leader_history[node_term] = node_id
                continue
            term_leader = self._leader_history[node_term]
            if term_leader != node_id:
                self._assertion_failures.append(f'Node {node_id} considers itself leader on term {node_term}, but so does node {term_leader}. The election safety invariant does not hold.')
