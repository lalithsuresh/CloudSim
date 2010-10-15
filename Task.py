from SimPy.Simulation import *

def change_total_tasks(scenario, value):
    scenario.total_tasks += value

class Task(Process):
    def __init__(self, name, taskId, joblist, scenario):
        Process.__init__(self, name=name)
        self.scenario = scenario
        self.taskId = taskId
        self.joblist = joblist
        self.numjobs = len(joblist)
