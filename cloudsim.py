import sys
from SimPy.Simulation import *
from SimPy.SimPlot import *
import random
from GlobalVars import *
from Scenario import CloudSimScenario


def change_total_tasks(scenario, value):
    scenario.total_tasks += value
    scenario.monitors['N'].observe(scenario.total_tasks)

class MyResource(Resource):
    
    def __init__(self, capacity=1, name="MyResource", queue_max=-1):
        Resource.__init__(self, capacity=capacity, name=name, monitored=False)
        self.queue_max = queue_max
        self.monitors = {}
        self.monitors['B'] = Monitor(self.name + '-B')
        self.monitors['T'] = Monitor(self.name + '-T')
        self.monitors['N'] = Monitor(self.name + '-N')
        self.N = 0
        self.monitors['N'].observe(0)
        self.A = 0
        self.C = 0
    
    def change_n(self, value):
        self.N += value
        self.monitors['N'].observe(self.N)
    
    def get_queue_size(self):
        return len(self.waitQ)
    
    def get_queue_available_space(self):
        if self.queue_max == -1:
            return -1
        return self.queue_max - self.get_queue_size()
    
    def can_enqueue_an_element(self):
        return self.get_queue_available_space() == -1 or \
                self.get_queue_available_space() > 0

class Scheduler(MyResource):
    def __init__(self, name="Scheduler", queue_max_size=10):
        MyResource.__init__(self, capacity=1, name=name, queue_max=queue_max_size)

class GridMachine(MyResource):
    def __init__(self, number, queue_max_size=10, factor=1):
        MyResource.__init__(self, capacity=1, name="M"+str(number)+':'+str(factor),
                            queue_max=queue_max_size)
        self.factor = factor
    
class Task(Process):
    def __init__(self, name, duration, scenario):
        Process.__init__(self, name=name)
        self.duration=duration
        self.scenario = scenario
        
    def go(self):

        self.scenario.scheduler.A += 1
        entry_time = now()
        if(not self.scenario.scheduler.can_enqueue_an_element()):
            change_total_tasks(self.scenario, -1)
            self.scenario.total_task_drops += 1
            return
        
        #Escalonando tarefa
        
        #Fila do escalonador
        scheduler_queue_start = now()
        self.scenario.scheduler.change_n(1)
        yield request,self,self.scenario.scheduler
        scheduler_queue = now() - scheduler_queue_start
        
        #Dentro do escalonador
        start = now()
        yield hold,self,self.scenario.scheduler_hold_time
        choosen_machine = self.scenario.schedule_algorithm(self.scenario.machineList, self.scenario)
        yield release,self,self.scenario.scheduler
        self.scenario.scheduler.change_n(-1)
        self.scenario.scheduler.C += 1
        now1=now()
        scheduler_service_time=now1 - start
        
        self.scenario.scheduler.monitors['B'].observe(scheduler_service_time)
        self.scenario.scheduler.monitors['T'].observe(scheduler_service_time + scheduler_queue)
        
        choosen_machine.A += 1
        if(not choosen_machine.can_enqueue_an_element()):
            change_total_tasks(self.scenario, -1)
            self.scenario.total_task_drops += 1
            return
        
        #Executando tarefa
        machine_queue_start = now()
        choosen_machine.change_n(1)
        yield request,self,choosen_machine
        machine_queue = now() - machine_queue_start
        start = now()
        yield hold,self, float(self.duration)/choosen_machine.factor
        yield release,self,choosen_machine
        choosen_machine.C += 1
        choosen_machine.change_n(-1)
        now1 = now()
        machine_service_time = now1 - start
        choosen_machine.monitors['B'].observe(machine_service_time)
        choosen_machine.monitors['T'].observe(machine_service_time + machine_queue)
        
        
        self.scenario.total_leaving_tasks += 1
        leaving_time = now()
        self.scenario.monitors['T'].observe(leaving_time - entry_time)
        change_total_tasks(self.scenario, -1)

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
          

currentMachine=0


arguments = [
             ('--grid-size', 'number of machines in the grid', 'integer', ),
             ('--scheduling-algorithm', 'scheduling algorithm', 'one of random, least_full, round_robin, fill_queue', ),
             ('--scheduler-qs', 'scheduler queue size', 'integer', ),
             ('--machine-qs', 'machine queue size', 'integer',  ),
             ('--scheduler-ht', 'scheduler hold time', 'float', ),
             ('--task-class-seed', 'seed used to do task class selection', 'integer'),
             ('--task-duration-seed', 'seed used to generate task duration', 'integer'),
             ('--task-arrival-seed', 'seed used to generate task arrivals', 'integer'),
             ('--task-arrival-mean', 'Mean task arrival rate', 'float'),
             ('--task-class', 'defines a task class, 4 values are needed: name probability time', 'string float int')
             ]

algorithms_map = {'random':random_schedule,
                  'least_full':least_full,
                  'round_robin':round_robin,
                  'fill_queue':fill_queue}

def usage():
    string = 'USAGE:\n'
    global arguments
    for arg in arguments:
        string = string + ('Argument: %s (%s) - type: %s\n' % arg)
        
    string += '\nArguments are passed as follow: simgrid.py --argument-name value'
    return string

def parse_args(scenario):
    import sys
    if len(sys.argv) == 1:
        #we have only one argument, return (we use default values)
        return
    
    args = sys.argv[1:]
    if len(args) == 1 and 'help' in args[0]:
        print usage()
        sys.exit()
    
    task_classes = []
    index = 0
    while index < len(args):
        if args[index] == '--grid-size':
            value = int(args[index+1])
            if value <= 0:
                raise Exception, '--grid-size must be positive'
            scenario.grid_description = [(value,1)]
            index += 2
        
        elif args[index] == '--scheduling-algorithm':
            scenario.schedule_algorithm = algorithms_map[args[index+1]]
            index += 2
            
        elif args[index] == '--scheduler-qs':
            value = int(args[index+1])
            if value <= 0:
                raise Exception, '--scheduler-qs must be positive'
            scenario.scheduler_queue_size = value
            index += 2
            
        elif args[index] == '--machine-qs':
            value = int(args[index+1])
            if value <= 0:
                raise Exception, '--machine-qs must be positive'
            scenario.machine_queue_size = value
            index += 2
        
        elif args[index] == '--scheduler-ht':
            value = float(args[index+1])
            if value <= 0:
                raise Exception, '--scheduler-ht must be positive'
            scenario.scheduler_hold_time = value
            index += 2

        elif args[index] == '--task-class-seed':
            value = int(args[index+1])
            if value <= 0:
                raise Exception, '--task-class-seed must be positive'
            scenario.task_class_selector_seed = value
            index += 2
            
        elif args[index] == '--task-duration-seed':
            value = int(args[index+1])
            if value <= 0:
                raise Exception, '--task-duration-seed must be positive'
            scenario.task_duration_seed = value
            index += 2
            
        elif args[index] == '--task-arrival-seed':
            value = int(args[index+1])
            if value <= 0:
                raise Exception, '--task-arrival-seed must be positive'
            scenario.task_arrival_seed = value
            index += 2
            
        elif args[index] == '--task-arrival-mean':
            value = float(args[index+1])
            if value <= 0:
                raise Exception, '--task-arrival-mean must be positive'
            scenario.task_arrival_mean = value
            index += 2
            
        elif args[index] == '--task-class':
            name = args[index+1]
            prob = float(args[index+2])
            mintime = int(args[index+3])
            maxtime = int(args[index+4])
            task_classes.append((name, prob, mintime, maxtime))
            index+=5

        else:
            raise Exception, 'Unknown option:' + str(args[index])
        
        if len(task_classes) > 0:
            scenario.task_distribution = task_classes

def makeGraphs(scenario):
    plt=SimPlot()
#    plt.plotHistogram(scenario.monitors['N'].histogram(low=0.0, high=100, nbins=30), xlab="tempo", ylab="tasks no sistema")
#
#    plt.root.title("Tasks arrived and tasks finished")
#    lineA=plt.makeLine(scenario.monitors['A'],color='blue')
#    lineC=plt.makeLine(scenario.monitors['C'],color='red')
    #lineN=plt.makeLine(scenario.monitors['N'],color='green')
#    obj=plt.makeGraphObjects([lineA, lineC])
#    frame=Frame(plt.root)
#    graph=plt.makeGraphBase(frame,1024,768,title="Chegadas[azul], Saidas[vermelho], Fregueses no sistema[verde]")
#    graph.pack()
#    graph.draw(obj)
#    frame.pack()
    
    plt.root.title("Tasks in the system")
    lineN=plt.makeLine(scenario.machineList[0].monitors['N'],color='red')
    obj=plt.makeGraphObjects([lineN])
    frame=Frame(plt.root)
    graph=plt.makeGraphBase(frame,1024,768,title="Tasks in the system")
    graph.pack()
    graph.draw(obj)
    frame.pack()
    
    #graph.postscr() 

    plt.mainloop()
    
def operationalValidation(scenario, verbose):
    T=now()
    global total_leaving_tasks
    
    if verbose:
        print "***Operational Validation***"
        for mac in scenario.machineList:
            print '***', mac.name, '***'
            print 'current tasks =', len(mac.waitQ) + len(mac.activeQ)
            print 'Ai =', mac.A
            print 'Bi =', mac.monitors['B'].total()
            print 'Ci =', mac.C
            print 'Ui =', mac.monitors['B'].total()/float(T)
            print 'Xi =', mac.C/float(T)
            print 'Si =', mac.monitors['B'].total()/mac.C
            print 'lambda-i =', mac.A/float(T)
            print 'Vi =', mac.C/scenario.total_leaving_tasks
            print 'N =', mac.monitors['N'].timeAverage()
            print 'T =', mac.monitors['T'].mean()
        
        print '***System as a whole***'
        print 't =', T
        print 'Ao = ', scenario.scheduler.A
        print 'lambda = ', float(scenario.scheduler.A)/T
        print 'Co = ', scenario.total_leaving_tasks
        print 'Xo = ', float(scenario.total_leaving_tasks)/T
        print 'N =', scenario.monitors['N'].timeAverage()
        print 'T =', scenario.monitors['T'].mean()
    
    #TODO calcular o num. tasks no sistema

def run(scenario, verbose=True):
    scenario.init_objects()
    initialize()
    
    taskGenerator = TaskGenerator(scenario)
    activate(taskGenerator, taskGenerator.run(scenario.sim_time))
       
    simulate(until=scenario.sim_time)
    
    if verbose:
        print "Total tasks:", scenario.total_arriving_tasks
        print "task arrival seed: ", scenario.task_arrival_seed
        print "task class seed: ", scenario.task_class_selector_seed
        print "task duration seed: ", scenario.task_duration_seed
    
    #makeGraphs(scenario)
    operationalValidation(scenario, verbose)
    return scenario, now()

def main():
    scenario = CloudSimScenario()
    parse_args(scenario)
    return run(scenario)
    
if __name__ == '__main__':
    main()

