from SimPy.Simulation import *

class Job(Process):
    def __init__ (self, name, instructions, memory, taskId, numjobs, jobId):
        Process.__init__(self, name=name)
        self.size = instructions
        self.req_mem = memory
        self.taskId = taskId
        self.numjobs = numjobs
        self.jobId = jobId
        self.finished = False
        self.workerId = None
        self.startTime = None
