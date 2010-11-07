from SimPy.Simulation import *
from Task import *
from Job import *

class TaskGenerator(Process):

    def __init__(self, scenario, input_parameters):

        Process.__init__(self)
        self.scenario = scenario

        # Following are input parameters to the task generator.
        # Stores a list of input parameter tuples
        self.task_parameters = map(lambda x:tuple(x[0]) + tuple (map(lambda y:int(y), x[1:])), input_parameters)

        # XXX: Keep seeds modifiable later on!
        self.cpuRandomObject = random.Random (1)
        self.memRandomObject = random.Random (2)
        
        # This list of tasks is submitted to the scheduler
        self.tasklist = []
    
    def generate_tasks (self):

        for each in self.task_parameters:

            mode,\
            startingTaskId,\
            numJobs,\
            rateOfJobGeneration,\
            startingJobId,\
            lowInstrBound,\
            highInstrBound,\
            lowMemBound,\
            highMemBound = each

            if (mode == 'W'):
                print "Not developed yet"
                exit (-1)
            elif (mode == 'S'):
                # Create a set of jobs for the task
                joblist = []
           
                print "Low mem " + str(lowMemBound) + ". High mem " + str(highMemBound)

                for jobId in xrange (startingJobId, startingJobId + numJobs):
                    name = "Job%s-%s" % (jobId, startingTaskId)
                    reqInstr = int (self.cpuRandomObject.uniform (lowInstrBound, highInstrBound))
                    reqMem = int (self.memRandomObject.uniform (lowMemBound, highMemBound))
                    joblist.append (Job (name, reqInstr, reqMem, startingTaskId, jobId, self.scenario))
             
                task = Task ("Task" + str(startingTaskId), startingTaskId, joblist, self.scenario)
                self.tasklist.append (task)

    def run(self, finish):

        # Populate the task list
        self.generate_tasks()
       
        activate (self.scenario.scheduler, self.scenario.scheduler.run())

        times = len(self.tasklist)
        for i in xrange(times):
            mode,\
            startingTaskId,\
            numJobs,\
            rateOfJobGeneration,\
            startingJobId,\
            lowInstrBound,\
            highInstrBound,\
            lowMemBound,\
            highMemBound = self.task_parameters[i]

            while (numJobs):
                jobsToBeAdded = []
                try:
                    for i in xrange (rateOfJobGeneration):
                        jobsToBeAdded.append (self.tasklist[0].joblist.pop(0))
                except IndexError:
                    self.tasklist.pop(0)
                    for i in xrange (len(jobsToBeAdded) - rateOfJobGeneration):
                        jobsToBeAdded.append (self.tasklist[0].joblist.pop(0))

                for each in jobsToBeAdded:
                    self.scenario.scheduler.addJob(each)
                    numJobs -= 1

                yield hold, self, 1
                print "Current time: " + str(now())
