class Task:
    def __init__(self, coro, callback, name):
        self._name = name
        self._coro = coro
        next(coro)
        self._last_returned_value = None
        self._callback = callback
    
    def run(self, arg):
        result = self._coro.send(arg)
        self._last_returned_value = result
        print(f'running task {self._name} with arg {arg}, got result {result}')
        return result
    
    def callback(self):
        if self._callback:
            print(f'running callback for task {self._name}, retval = {self._last_returned_value}')
            self._callback(self._last_returned_value)
