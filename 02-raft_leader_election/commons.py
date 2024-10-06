import random

class Clock:
    def __init__(self):
        self._time = 0
    
    def tick(self):
        self._time += 1

    def get_time(self):
        return self._time
    
    def set_time(self, time):
        self._time = time
    
class RNG:
    def __init__(self, seed: int):
        random.seed(seed)

    def generate(self, delay):
        return random.randint(0, delay)