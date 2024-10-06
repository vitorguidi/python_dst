from dataclasses import dataclass

@dataclass
class Request:
    node_from: int
    node_to: int

@dataclass
class Response:
    node_from: int
    node_to: int

@dataclass
class AppendEntriesRequest(Request):
    term: int
    leader_id: int
    rpc_name: str = 'append_entries'

@dataclass
class AppendEntriesResponse(Response):
    term: int
    rpc_name: str = 'append_entries'

@dataclass
class RequestVoteRequest(Request):
    term: int
    candidate_id: int
    rpc_name: str = 'request_vote'

@dataclass
class RequestVoteResponse(Response):
    term: int
    vote_granted: bool
    rpc_name: str = 'request_vote'


if __name__ == '__main__':
    req = AppendEntriesRequest(0,1,{},1,0)
    assert isinstance(req, Request)
    assert isinstance(req, AppendEntriesRequest)
    assert req.rpc_name == 'append_entries'
    resp = AppendEntriesResponse(0,0,{},0)
    assert isinstance(resp, Response)
    assert isinstance(resp, AppendEntriesResponse)
    assert resp.rpc_name == 'append_entries'
