from SimPy.Simulation import *
from Task import *
from Job import *

class TaskGenerator(Process):

    def __init__(self, scenario, input_parameters):

        Process.__init__(self)
        self.scenario = scenario

        # Following are input parameters to the task generator
        self.mode = input_parameters[0]
        self.startingTaskId,\
        self.numJobs,\
        self.rateOfJobGeneration,\
        self.startingJobId,\
        self.lowInstrBound,\
        self.highInstrBound,\
        self.lowMemBound,\
        self.highMemBound = map (lambda x : int (x), input_parameters[1:])

        # XXX: Keep seeds modifiable later on!
        self.cpuRandomObject = random.Random (1)
        self.memRandomObject = random.Random (2)
        
        # This list of tasks is submitted to the scheduler
        self.tasklist = []
    
    def generate_tasks (self):

        if (self.mode == 'W'):
            print "Not developed yet"
            exit (-1)
        elif (self.mode == 'S'):
            # Create a set of jobs for the task
            joblist = []
            
            for jobId in xrange (self.startingJobId, self.startingJobId + self.numJobs):
                name = "Job%s-%s" % (jobId, self.startingTaskId)
                reqInstr = int (self.cpuRandomObject.uniform (self.lowInstrBound, self.highInstrBound))
                reqMem = int (self.memRandomObject.uniform (self.lowMemBound, self.highMemBound))
                joblist.append (Job (name, reqInstr, reqMem, self.startingTaskId, jobId, self.scenario))
             
            task = Task ("Task" + str(self.startingTaskId), self.startingTaskId, joblist, self.scenario)
            self.tasklist.append (task)

    def run(self, finish):

        # Populate the task list
        self.generate_tasks()

        activate (self.scenario.scheduler, self.scenario.scheduler.schedule ())
        while (self.numJobs):
            jobsToBeQueued = []
            try:
                for i in xrange (self.rateOfJobGeneration):
                    jobsToBeQueued.append (self.tasklist[0].joblist.pop(0))
            except IndexError:
                self.tasklist.pop(0)
                for i in xrange (len(jobsToBeQueued) - self.rateOfJobGeneration):
                    jobsToBeQueueda.append (self.tasklist[0].joblist.pop(0))

            for each in jobsToBeQueued:
                self.scenario.scheduler.enqueue (each)
                self.numJobs -= 1

            yield hold, self, 1
