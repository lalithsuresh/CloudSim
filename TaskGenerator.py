from SimPy.Simulation import *
from Task import *

class TaskGenerator(Process):

    def __init__(self, scenario):

        Process.__init__(self)
        self.scenario = scenario
        self.distribution = scenario.task_distribution
        self.n = 0
        self.random_class = random.Random(scenario.task_class_selector_seed)
        self.random_duration = random.Random(scenario.task_duration_seed)
        self.random_arrival = random.Random(scenario.task_arrival_seed)

    def select_task_class(self):
        value = self.random_class.uniform(0,1)
        total = 0.0
        for dist in self.distribution:
            total += dist[1]
            if value <= total:
                return dist

        return self.distribution[-1]

    #def select_task_duration(self, task_class):
    #    return self.random_duration.uniform(task_class[2], task_class[3])
    def select_task_duration(self, task_class):
        return self.random_duration.expovariate(task_class[2])

    def get_task(self):
        task_class = self.select_task_class()
        name = "Task(%s)-%d" % (task_class[0], self.n)
        self.n += 1
        return Task(name, self.select_task_duration(task_class), self.scenario)

    def run(self, finish):

        while now() < finish:
          yield hold,self,self.random_arrival.expovariate(self.scenario.task_arrival_mean)
          c = self.get_task()
          self.scenario.total_arriving_tasks += 1
          change_total_tasks(self.scenario, 1)
          activate(c,c.go())
