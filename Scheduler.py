from SimPy.Simulation import *
from AbstractResource import *
from Task import *

class Scheduler(Process):
    def __init__(self, scenario, name="Scheduler", queue_max_size=10):
        Process.__init__ (self, name=name)
        self.scenario = scenario

        #XXX: Implement max queue size
        self.queue = []
        self.running = 0

    def enqueue (self, Task):
        self.queue.append (Task) 
        if (len(self.queue) == 1):
            reactivate (self)

    def schedule (self):

        i = 0
        while (1):

          if (len(self.queue) == 0):
              yield passivate, self

          # Scheduler queue
          scheduler_queue_start = now ()

          # Replace the yield-requests with processor requests
          scheduler_queue = now () - scheduler_queue_start

          start = now ()
          yield hold,self,self.scenario.scheduler_hold_time
          choosen_machine = self.scenario.schedule_algorithm(self.scenario.machineList, self.scenario)

          now1 = now()
          scheduler_service_time = now1 - start

          # The below has been commented out because choosen_machine
          # has been converted from AbstractResource to Process.
          # This should be changed to the part where the scheduler
          # probes the machine to see if it can handle another Job.

          # if(not choosen_machine.can_enqueue_an_element()):
          #    change_total_tasks(self.scenario, -1)
          #    self.scenario.total_task_drops += 1
          #    return 
        
          print "Scheduler enqueued someone at:", now()
          activate (choosen_machine, choosen_machine.execute_task (self.queue.pop(0)))
