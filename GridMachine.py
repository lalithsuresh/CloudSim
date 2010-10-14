from SimPy.Simulation import *
from AbstractResource import *

class GridMachine(AbstractResource):
    def __init__(self, number, queue_max_size=10, factor=1):
        AbstractResource.__init__(self, capacity=1, name="M"+str(number)+':'+str(factor),
                            queue_max=queue_max_size)
        self.factor = factor
