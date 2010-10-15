from SimPy.Simulation import *
from AbstractResource import *

class Scheduler(Process):
    def __init__(self, scenario, name="Scheduler", queue_max_size=10):
        Process.__init__ (self, name=name)
        self.scenario = scenario

        #XXX: Implement max queue size
        self.queue = []
        self.running = 0

    def enqueue (self, job):
        self.queue.append (job) 
        if (len(self.queue) == 1):
            reactivate (self)

    def schedule (self):

        while (1):

          if (len(self.queue) == 0):
              yield passivate, self

          yield hold,self,self.scenario.scheduler_hold_time
          choosen_machine = self.scenario.schedule_algorithm (self.scenario.machineList, self.scenario)

          activate (choosen_machine, choosen_machine.execute_task (self.queue.pop(0)))
