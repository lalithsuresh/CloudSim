from SimPy.Simulation import *

class Job(Process):
    def __init__ (self, name, duration, memory, taskId, jobId, scenario):
        Process.__init__(self, name=name)
        self.duration = duration
        # self.req_instructions = instructions
        self.req_mem = memory
        self.taskId = taskId
        self.jobId = jobId
        self.scenario = scenario
