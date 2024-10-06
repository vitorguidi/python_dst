from rpc import AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse, Request, Response
from commons import Clock, RNG
from enum import Enum
from task import Task

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

    def _become_follower(self, term: int):
        self._term = term
        self._voted_for = -1
        self._state = RaftState.FOLLOWER

    def _become_candidate(self):
        self._term += 1
        self._voted_for = -1
        self._last_rpc_time = self._clock.get_time()
        self._state = RaftState.CANDIDATE

    def _become_leader(self):
        self._state = RaftState.LEADER

    def handle_leader(self):
        yield
        print('handling_leader')
        for target_node in range(self._nr_nodes):
            request = AppendEntriesRequest(node_from=self._node_id, node_to=target_node, leader_id=self._node_id, term=self._term)
            response = yield request
            assert type(response) == AppendEntriesResponse
            if response.term > self._term:
                self._become_follower(response.term)

    def handle_follower(self):
        yield #Coro initialization protocol
        print('handling follower')
        if self._clock.get_time() >= self._last_rpc_time + self._election_timeout:
            print(f'becoming candidate on node {self._node_id}')
            self._become_candidate()
        return None  #For callbacks to work we always return something

    def handle_candidate(self):
        yield
        print('handling candidate')
        if self._clock.get_time() >= self._last_rpc_time + self._election_timeout:
            self._become_candidate()
            return None

        self._voted_for = self._node_id
        nr_votes = 0

        print(f'node {self._node_id} is starting election')

        for target_node in range(self._nr_nodes):
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
            if response.vote_granted:
                nr_votes += 1
            
        if nr_votes > self._nr_nodes/2:
            self._become_leader()
            print(f'Node {self._node_id} became leader on term {self._term}')
        return None

    def raft_loop(self):
        yield
        while True:
            print(f'doing one round of raft on node {self._node_id}')
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
        print(type(request))
        assert isinstance(request, RequestVoteRequest)
        response = RequestVoteResponse(node_from=request.node_to, node_to=request.node_from, term=self._term, vote_granted=False)
        self._last_rpc_time = self._clock.get_time()
        if request.term > self._term:
            self._become_follower(request.term)
            self._voted_for = request.candidate_id
            response.term = request.term
            response.vote_granted = True
        elif request.term < self._term:
            response.vote_granted = False        
        elif self._voted_for in [-1, request.candidate_id]:
            self._voted_for = request.candidate_id
            response.vote_granted = True
        else:
            response.vote_granted = False

        response.term = self._term
        yield response
