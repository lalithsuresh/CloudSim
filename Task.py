from SimPy.Simulation import *

def change_total_tasks(scenario, value):
    scenario.total_tasks += value
    scenario.monitors['N'].observe(scenario.total_tasks)

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
