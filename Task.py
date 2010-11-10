from SimPy.Simulation import *

class Task(Process):
    def __init__(self, name, taskId, joblist):
        Process.__init__(self, name=name)
        self.taskId = taskId
        self.joblist = joblist
        self.numjobs = len(joblist)
