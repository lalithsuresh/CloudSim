from SimPy.Simulation import *
from AbstractResource import *

class Scheduler(Process):
    def __init__(self, scenario, name="Scheduler", queue_max_size=10):
        Process.__init__ (self, name=name)
        self.scenario = scenario

        #XXX: Implement max queue size
        self.jobqueue = []
        self.running = 0

    def enqueue (self, job):
        self.jobqueue.append (job) 

        # Only run the Scheduler thread
        # if we have jobs to schedule
        if (len(self.jobqueue) == 1):
            reactivate (self)

    def schedule (self):

        while (1):

          # If our job queue is empty,
          # go into idle state
          if (len(self.jobqueue) == 0):
              yield passivate, self

          yield hold,self,self.scenario.scheduler_hold_time

          choosen_machine = self.scenario.schedule_algorithm (self.scenario.machineList, self.scenario)

          # Pass on job to a machine for execution
          choosen_machine.addJob(self.jobqueue.pop(0))
          #activate (choosen_machine, choosen_machine.execute_task (self.jobqueue.pop(0)))
