import sys
from SimPy.Simulation import *
import random
from SchedulingAlgos import *
from Scheduler import *
from CloudMachine import *

DEFAULT_CONF_FILE = "scheduler.conf"

algorithms_map = {'random':random_schedule,
                  'round_robin':round_robin}

class CloudSimScenario:
    def __init__(self, confPath):

        conf = open(confPath, 'r')

        #Single task or web mode (W or S)
        self.mode = self.nextProperty(conf)
        #Scheduling interval (0.01 - 1 seconds)
        self.sch_interval = float(self.nextProperty(conf))
        #Worker node speed (200 - 400 instructions/second)
        self.wn_speed = int(self.nextProperty(conf))
        #Worker node memory size (2 - 8 GB)
        self.wn_mem = int(self.nextProperty(conf))*1024
        #Worker node swapping cost (2 - 10 instructions/GB)
        self.wn_swap = float(self.nextProperty(conf))/1024
        #Worker quantum (0.01 - 0.5 seconds)
        self.wn_quantum = float(self.nextProperty(conf))
        #Worker node startup time (120 - 600 seconds)
        self.wn_startup = int(self.nextProperty(conf))
        #Worker node scheduler notification time (1-5 instructions)
        self.wn_notification = int(self.nextProperty(conf))
        #Worker node cost (euros/hour);
        self.wn_cost = float(self.nextProperty(conf))
        #Initial number of worker nodes
        self.initial_machines = int(self.nextProperty(conf))
        #Simulation time
        self.sim_time = int(self.nextProperty(conf))
        #Scheduling algorithm (either random or round robin)
        self.schedule_algorithm = algorithms_map[self.nextProperty(conf)]

        self.machines = []
        self.genId = 0

        self.initiated = False
        self.monitors = {}
    
    def nextProperty(self, file):
        line = file.readline()
        while(line != '' and line.startswith("#")):
          line = file.readline()
        return line.strip('\n')

    def init_objects(self):
        if self.initiated:
            raise Exception("Objects already initiated")
        else:
            self.initiated = True
        #objects
        self.scheduler = Scheduler(self)
        for m_id in range(self.initial_machines):
          self.createMachine(1)

    def createMachine(self, started=0):
        machine = CloudMachine(self.genId, self, started)
        self.machines.append(machine)
        self.genId += 1
        return machine

    def addMonitor (self, name):
        try: 
            self.monitors[name]
            print 'Error: Monitor name "%s" is already being used' % (name)
            sys.exit (-1)
        except KeyError:
            self.monitors[name] = Monitor ()
