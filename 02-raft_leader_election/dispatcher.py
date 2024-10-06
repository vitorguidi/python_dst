class Dispatcher:
    """Generates a dispatcher object, to facilitate the scheduler in handling RPCs"""
    def __init__(self, nodes):
        dispatcher = {}
        for index, node in enumerate(nodes):
            for method_name in dir(node):
                if method_name.startswith("handle_") and method_name.endswith("_rpc"):
                    rpc_name = method_name[7:-4]  # Extract the name between "handle_" and "_rpc"
                    if not rpc_name in dispatcher:
                        dispatcher[rpc_name] = {}
                    dispatcher[rpc_name][index] = getattr(node, method_name)  # Call the generator method
        self._dispatcher = dispatcher

    def get_handler_coro(self, rpc_name, node_id):
        """Generates a fresh coroutine that handles the specific rpc on the given node"""
        if rpc_name not in self._dispatcher:
            raise Exception(f'RPC not registered in the dispatcher: {rpc_name}')
        if node_id not in self._dispatcher[rpc_name]:
            raise Exception(f'Node not registered in the cluster: {node_id}')
        return self._dispatcher[rpc_name][node_id]()


if __name__ == '__main__':
    class DummyNode:
        def __init__(self, id):
            self._id = id

        def handle_some_rpc(self):
            yield
            yield f'some_{self._id}'

        def handle_another_rpc(self):
            yield
            yield f'another_{self._id}'

    nr_nodes = 10
    nodes = [DummyNode(i) for i in range(nr_nodes)]
    dispatcher = Dispatcher(nodes)
    for i in range(nr_nodes):
        for rpc_name in ['some', 'another']:
            coro = dispatcher.get_handler_coro(rpc_name, i)
            next(coro)
            assert coro.send(None) == f'{rpc_name}_{i}'