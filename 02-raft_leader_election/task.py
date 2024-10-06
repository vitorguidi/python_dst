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
        return result
    
    def callback(self):
        if self._callback:
            self._callback(self._last_returned_value)
