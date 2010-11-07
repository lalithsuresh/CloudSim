# Python modules
import sys
import random

# Simpy modules
from SimPy.Simulation import *
from SimPy.SimPlot import *

# CloudSim modules
from Scenario import *
from Scheduler import *
from SchedulingAlgos import *
from TaskGenerator import *
from Task import *
from AbstractResource import *
from CloudMachine import *

#Used in round robin scheduler
currentMachine = 0

inputFile = ''

arguments = [
             ('--input', 'input file', 'input file path',  ),
             ('--conf', 'simulation conf file', 'config file path', )
             ]


def usage():
    string = 'USAGE:\n'
    global arguments
    for arg in arguments:
        string = string + ('Argument: %s (%s) - type: %s\n' % arg)
        
    string += '\nArguments are passed as follow: simgrid.py --argument-name value'
    return string

def parse_args():
    import sys
    if len(sys.argv) == 1:
        print usage()
        sys.exit()

    args = sys.argv[1:]
    if (len(args) < 4):
        print usage()
        sys.exit()
    
    scenario = None

    index = 0
    while index < len(args):
        if args[index] == '--conf':
            conf = args[index + 1]
            scenario = CloudSimScenario(conf)
            index += 2 
        elif args[index] == '--input':
            global inputFile
            inputFile = args[index + 1]
            index += 2
        else:
            raise Exception, 'Unknown option:' + str(args[index])

    return scenario

def run(scenario, verbose=True):
    scenario.init_objects()
    initialize()
    
    # XXX: Works only for a single input line.
    # XXX: Need to implement batch processing.
    global inputFile
    inputFile = open (inputFile, 'r')
    # Remove trailing endline characters
    temp = map (lambda x : x.strip (), inputFile.readlines ())
    params = []
    for each in temp:
      y = each.split()
      # Remove commas from each input except the last
      params.append(map(lambda x: x[:-1], y[:-1]) + [y[-1]]) 
    taskGenerator = TaskGenerator (scenario, params)
    activate (taskGenerator, taskGenerator.run(scenario.sim_time))

    #Activate initial machines
    for machine in scenario.machines:
        activate(machine, machine.start())
    
    simulate(until=scenario.sim_time)
    
    return scenario, now()

def main():
    scenario = parse_args()
    return run(scenario)
    
if __name__ == '__main__':
    main()
