from SimPy.Simulation import *

class Job(Process):
    def __init__ (self, name, instructions, memory, taskId, jobId, scenario):
        Process.__init__(self, name=name)
        self.size = instructions
        self.req_mem = memory
        self.taskId = taskId
        self.jobId = jobId
        self.scenario = scenario
        self.finished = 0
