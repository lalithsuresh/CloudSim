# Python modules
import sys

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
    print "- Simulation complete at %d" %(now())

    scenario.finish_objects()

    print_result(scenario)

    scenario.executeMonitorPlots()

    return scenario, now()

def init_task_generators(scenario):
    global inputFile
    inputFile = open (inputFile, 'r')
    # Remove trailing endline characters
    temp = map (lambda x : x.strip (), inputFile.readlines ())
    params = []
    totalJobs = 0
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
    print "%s\t:\t %s" % ("Random seed",str(scenario.seed))
    print "%s\t:\t %s" % ("Scheduling algorithm",str(scenario.algoName))
    print "%s\t:\t %s" % ("Started workers", str(scenario.initial_machines))
    print "%s\t:\t %s" % ("Simulation time", str(scenario.sim_time))

def print_result(scenario):
 
    jobsRT = scenario.scheduler.jobsRT
    tasksRT = scenario.scheduler.tasksRT

    # Calculate job response time average
    # and standard deviation
    avgJobRT = 0
    jobRTStdDev = 0
    if(len(jobsRT) > 0):
        avgJobRT = sum(jobsRT)/len(jobsRT)
        summation = 0
        for rt in jobsRT:
            summation += (avgJobRT - rt)**2
        jobRTStdDev = math.sqrt(summation/len(jobsRT)-1)

    # Calculate job response time average
    # and standard deviation
    avgTaskRT = 0
    taskRTStdDev = 0
    if(len(tasksRT) > 0):
        avgTaskRT = sum(tasksRT)/len(tasksRT)
        for rt in scenario.scheduler.tasksRT:
            summation += (avgTaskRT - rt)**2
        taskRTStdDev = math.sqrt(summation/len(tasksRT)-1)

    allMachines = scenario.scheduler.activeMachines+scenario.scheduler.destroyedMachines

    # Calculate total execution time,
    # wasted time, CPU time and total cost
    totalTime = 0
    wastedTime = 0
    cpuTime = 0
    for machine in allMachines:
        totalTime += machine.getExecutionTime()
        wastedTime += machine.getWastedTime()
        cpuTime += machine.getCPUTime()

    totalCost = totalTime*scenario.wn_cost

    scenario.printSep()
    print "- Simulation results:"
    print "%s\t:\t %s" % ("Total execution time", str(totalTime))
    print "%s\t:\t %s" % ("Total CPU time used",str(cpuTime))
    print "%s\t:\t $%s" % ("Total cost",str(totalCost))
    print "%s\t:\t $%s" % ("Total unused paid time",str(wastedTime))
    print "%s\t:\t %s" % ("Average job response time", str(avgJobRT))
    print "%s\t:\t %s" % ("Job response time std deviation", str(jobRTStdDev))
    print "%s\t:\t %s" % ("Average task response time", str(avgTaskRT))
    print "%s\t:\t %s" % ("Task response time std deviation", str(taskRTStdDev))

def main():
    scenario = parse_args()
    return run(scenario)
    
if __name__ == '__main__':
    main()
