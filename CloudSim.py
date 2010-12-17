# Python modules
import sys
import math

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

totalJobs = 0

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
    print "--------------- CloudSim ----------------"
    print "-----------------------------------------"
    initialize()
    scenario.init_objects()
    print "- Scenario initiated"

    print_initial_data(scenario)

    init_task_generators(scenario)

    scenario.printSep()
    print "- Running Simulation"
    simulate(until=scenario.sim_time)
    print "- Simulation complete"

    scenario.finish_objects()

    print_result(scenario)

    scenario.executeMonitorPlots()

    return scenario, now()

def init_task_generators(scenario):
    global inputFile
    global totalJobs
    inputFile = open (inputFile, 'r')
    # Remove trailing endline characters
    temp = map (lambda x : x.strip (), inputFile.readlines ())
    params = []
    for each in temp:
        y = each.split()
        # Remove commas from each input element except the last
        params = (map(lambda x: x[:-1], y[:-1]) + [y[-1]]) 
        taskGenerator = TaskGenerator (scenario, [params])  # Generate one TaskGenerator per input line
        totalJobs += taskGenerator.numJobs()
        activate (taskGenerator, taskGenerator.run(scenario.sim_time))

    scenario.remainingTasks = len(temp)

    scenario.printSep()
    print "- Task Generators created"
    print "%s\t:\t %s" % ("Jobs to be generated", str(totalJobs))


def print_initial_data(scenario):
    scenario.printSep()
    print "- Initial data:"
    print "%s\t:\t %s" % ("Execution mode",str(scenario.mode))
    print "%s\t:\t %s" % ("Random seed",str(scenario.seed))
    print "%s\t:\t %s" % ("Scheduling algorithm",str(scenario.algoName))
    print "%s\t:\t %d%%" % ("Acceptable waste",scenario.acceptableWaste)
    print "%s\t:\t %s" % ("Started workers", str(scenario.initial_machines))
    print "%s\t:\t %s" % ("Simulation time", str(scenario.sim_time))

def print_result(scenario):
 
    allMachines = scenario.scheduler.activeMachines+scenario.scheduler.destroyedMachines

    # Calculate total execution time,
    # wasted time, CPU time and total cost
    cpuTime = 0
    wastedTime = 0
    wastedSwStartup = 0
    wastedPart = 0
    totalCost = 0
    paidTime = 0
    for machine in allMachines:
        cpuTime += machine.getExecutionTime()
        paidTime += machine.getPaidTime()
        wastedTime += machine.getWastedTime()
        wastedSwStartup += machine.getWastedSwapAndStartup()
        wastedPart += machine.getWastedPartialHours()
        totalCost += machine.getExecutionCost()

    jobRTs = scenario.monitors["jobRT"]
    taskRTs = scenario.monitors["taskRT"]

    completedJobs = scenario.scheduler.completedJobs

    scenario.printSep()
    print "- Simulation results:"
    print "%s\t:\t %d (%.2f%%)" % ("Completed jobs", completedJobs, (completedJobs/totalJobs)*100)
    print "%s\t:\t %.2fs" % ("Total execution time", now())
    print "%s\t:\t %.2fs" % ("Total CPU time used",cpuTime)
    print "%s\t:\t %.2fs" % ("Total CPU time paid", paidTime)
    print "%s\t:\t %.2fs" % ("Total unused paid time",wastedTime)
    print "-- %s\t:\t %.2fs (%.2f%%)" % ("Swap and startup:",wastedSwStartup, (wastedSwStartup/wastedTime)*100)
    print "-- %s\t:\t %.2fs (%.2f%%)" % ("Unused partial hour:",wastedPart, (wastedPart/wastedTime)*100)
    print "%s\t:\t %.2f%%" % ("Percentage of waste", (wastedTime/paidTime)*100)
    print "%s\t:\t $%.2f" % ("Total cost", totalCost)
    print "%s\t:\t %.2fs" % ("Average job response time", jobRTs.mean())
    print "%s\t:\t %.2fs" % ("Job response time std deviation", math.sqrt(jobRTs.var()))
    #print "%s\t:\t %ss" % ("Average task response time", str(taskRTs.mean()))
    #print "%s\t:\t %s" % ("Task response time std deviation", str(math.sqrt(taskRTs.var())))

def main():
    scenario = parse_args()
    return run(scenario)
    
if __name__ == '__main__':
    main()
