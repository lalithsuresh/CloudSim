import sys
from SimPy.Simulation import *
import random
from GlobalVars import *
from Scheduler import *
from GridMachine import *

class CloudSimScenario:
    def __init__(self):
        
        # Parameters
        self.grid_description = [(GRID_SIZE, 1)] # All machines have the same performance
        self.schedule_algorithm = DEFAULT_SCHEDULING_ALG
        self.task_distribution = [("simple", SIMPLE_TASK_PROB, SIMPLE_TASK_TIME),
                                     ("normal", NORMAL_TASK_PROB, NORMAL_TASK_TIME),
                                     ("complex", COMPLEX_TASK_PROB, COMPLEX_TASK_TIME)]
        self.task_distribution = [("simple", SIMPLE_TASK_PROB, SIMPLE_TASK_TIME)]
        self.scheduler_queue_size = SCHEDULER_MAX_QUEUE_SIZE
        self.machine_queue_size = MACHINE_MAX_QUEUE_SIZE
        self.task_class_selector_seed = random.randint(11, 9999)
        self.task_duration_seed = random.randint(11, 9999)
        self.task_arrival_seed = random.randint(11, 9999)
        self.task_arrival_mean = 10.0
        self.scheduler_hold_time = SCHEDULER_HOLD_TIME
        self.sim_time = DEFAULT_SIMULATION_TIME
        
        self.total_tasks = 0
        self.total_arriving_tasks = 0
        self.total_leaving_tasks = 0
        self.total_task_drops = 0
        
        self.initiated = False
        self.monitors = {}
        
    def init_objects(self):
        if self.initiated:
            raise Exception("Objects already initiated")
        else:
            self.initiated = True
        #objects
        self.scheduler = Scheduler(queue_max_size=self.scheduler_queue_size)
        self.machineList = []
        count = 0
        for m in self.grid_description:
            for n in range(m[0]):
                self.machineList.append(GridMachine(count, self.machine_queue_size, m[1]))
                count += 1
        
        # Monitors
        self.monitors['N'] = Monitor("N") # Number of clients
        self.monitors['T'] = Monitor("T") # Service time
