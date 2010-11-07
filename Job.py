from SimPy.Simulation import *

class Job(Process):
    def __init__ (self, name, instructions, memory, taskId, numjobs, jobId, scenario):
        Process.__init__(self, name=name)
        self.size = instructions
        self.req_mem = memory
        self.taskId = taskId
        self.numjobs = numjobs
        self.jobId = jobId
        self.scenario = scenario
        self.finished = 0
        self.worker = None
