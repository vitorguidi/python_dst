from dataclasses import dataclass
import random

@dataclass
class Request:
    node_id: int
    type: str
    key: int
    val: int

truth = {}

def server(node_id):
    data = {}
    while True:
        print('server waiting for request')
        request = yield
        print(f'server got request = {request}')
        if request.type == 'read':
            result = data.get(request.key, None)
            print(f'server sending read result for key {request.key}: {result}')
            yield result
        elif request.type == 'write':
            data[request.key] = request.val
            truth[request.key] = request.val
            print(f'server wrote data[{request.key}]={request.val}')
            yield request.val
        else:
            raise Exception(f'expected read or write op, got request = {request}')


def client():
    global truth
    while True:
        key = random.randint(0,10)
        val = random.randint(0,10)
        op = random.randint(0,10)
        if op % 2 == 0:
            print(f'client requesting op = read, for key = {key}')
            result = yield Request(node_id=0, type='read', key=key, val = None)
            print(f'client got read result for key {key} = {result}')
            assert truth.get(key, None) == result, f'expected {truth.get(key, None)}, got {result}'
        else:
            print(f'client requesting op = put, for key = {key}, val = {val}')
            result = yield Request(node_id=0, type='write', key=key, val=val)
            print(f'client got assured that server wrote {key} -> {result}')




class Scheduler:

    def __init__(self):
        self._tasks = []
        self._servers = {}
        for i in range(1):
            self._servers[i] = server(i)
            self._servers[i].send(None)


    def add_task(self, gen):
        self._tasks.append(gen)

    def run(self):
        while self._tasks:
            [task, val] = self._tasks.pop()
            try:
                req = task.send(val)
                assert type(req) == Request
                server = self._servers[req.node_id]
                result = server.send(req)
                # Dodgy loc, wont reach the next yield if I do not next here
                # To get past the yield
                next(server)
                self.add_task([task, result])
            except StopIteration:
                pass


sched = Scheduler()
cl = client()
sched.add_task([cl, None])
sched.run()