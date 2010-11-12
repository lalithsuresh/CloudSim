import sys
from SimPy.Simulation import *
from SimPy.SimPlot import *
import random
from SchedulingAlgos import *
from Scheduler import *

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
        self.algoName = self.nextProperty(conf)
        self.schedule_algorithm = algorithms_map[self.algoName]
        #Random seed
        self.seed = int(self.nextProperty(conf))
        random.seed(self.seed)

        self.initiated = False
        self.monitors = {}
        self.monitorPlots = {}
        self.monitorFunctions = {}

    def plotLine (self,monitor):
        SimPlot().plotLine (monitor).mainloop()

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
        
        # Activate scheduler
        activate(self.scheduler, self.scheduler.run())

    def finish_objects(self):
        if not self.initiated:
            raise Exception("Objects not initiated")
        else:
            self.initiated = False
        
        #stop scheduler
        self.scheduler.stop()

    def addMonitor (self, name):
        try: 
            self.monitors[name]
            print 'Error: Monitor name "%s" is already being used' % (name)
            sys.exit (-1)
        except KeyError:
            self.monitors[name] = Monitor (name = name)

        return self.monitors[name]

    def addMonitorPlot (self, name, monitor):
        try:
            self.monitorPlots[name]
            print 'Error: Plot function already defined for monitor %s' % (name)
            sys.exit (-1)
        except KeyError:
            self.monitorPlots[name] = monitor

    def addMonitorFunction (self, name, fn):
        try:
            self.monitorFunctions[name]
            print 'Error: Monitor function already defined for monitor %s' % (name)
            sys.exit (-1)
        except KeyError:
            self.monitorFunctions [name] = fn
            
    def executeMonitorFunctions (self):
        self.printSep()
        print "- Calculating monitor functions"
        for each in self.monitorFunctions:
            print "%s\t:\t %s" % (each, self.monitorFunctions[each]())

    def executeMonitorPlots (self):
        self.printSep()
        print "- Generating graph files"
        for each in self.monitorPlots:
            fileName = each + " - Seed " + str(self.seed) + ".ps"
            plot = SimPlot ()
            pl = plot.plotLine (self.monitorPlots[each], color="blue",width=2)
            pl.postscr(fileName)
            print "Graph file created\t:\t " + fileName

    def printSep(self):
        print "-----------------------------------------"
