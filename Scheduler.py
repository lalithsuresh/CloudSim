from SimPy.Simulation import *
from AbstractResource import *

class Scheduler(AbstractResource):
    def __init__(self, name="Scheduler", queue_max_size=10):
        AbstractResource.__init__(self, capacity=1, name=name, queue_max=queue_max_size)
