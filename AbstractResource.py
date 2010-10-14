from SimPy.Simulation import *

class AbstractResource(Resource):

    def __init__(self, capacity=1, name="MyResource", queue_max=-1):
        Resource.__init__(self, capacity=capacity, name=name, monitored=False)
        self.queue_max = queue_max
        self.monitors = {}
        self.monitors['B'] = Monitor(self.name + '-B')
        self.monitors['T'] = Monitor(self.name + '-T')
        self.monitors['N'] = Monitor(self.name + '-N')
        self.N = 0
        self.monitors['N'].observe(0)
        self.A = 0
        self.C = 0

    def change_n(self, value):
        self.N += value
        self.monitors['N'].observe(self.N)

    def get_queue_size(self):
        return len(self.waitQ)

    def get_queue_available_space(self):
        if self.queue_max == -1:
            return -1
        return self.queue_max - self.get_queue_size()

    def can_enqueue_an_element(self):
        return self.get_queue_available_space() == -1 or \
                self.get_queue_available_space() > 0
