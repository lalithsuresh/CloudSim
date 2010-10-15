from SimPy.Simulation import *
from AbstractResource import *

class CloudMachine(Process):
    def __init__(self, number, queue_max_size=10, factor=1):
        Process.__init__(self, name="M"+str(number)+':'+str(factor))
        self.factor = factor

    # The below needs to be extended to account for
    # memory, swap in-outs and processors itself
    def execute_task (self, task):
        machine_queue_start = now()
        # yield request,self,choosen_machine
        machine_queue = now() - machine_queue_start
        start = now()
        yield hold,self, float(task.duration)/self.factor
        # yield release,self,choosen_machine
        now1 = now()
        machine_service_time = now1 - start

        # Fix later
        # self.scenario.total_leaving_tasks += 1
        leaving_time = now()
        # change_total_tasks(self.scenario, -1)
