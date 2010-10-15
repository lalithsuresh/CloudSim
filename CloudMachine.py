from SimPy.Simulation import *
from AbstractResource import *
from GlobalVars import *

class CloudMachine(Process):
    def __init__(self):
        Process.__init__(self, name="M")
        self.jobs = []
        self.started = 0
        self.index = -1
        self.availMem = NODE_MEM

    def addJob (self, job):
        self.jobs.append (job)
        if (len(self.jobs) == 1):
            reactivate (self)

    def shutdown(self):
        self.started = 0

    def start(self):
        self.started = 1 

        # Holds for startup time
        yield hold,self, NODE_STARTUP
       
        while(self.started):
            if(len(self.jobs) == 0):
               yield passivate, self

            # Selects next job in the list 
            self.index = (self.index+1)%len(self.jobs) 
            currentJob = self.jobs[self.index]

            # Executes selected job
            # instructions for a quantum
            yield hold,self,QUANTUM

            executed=QUANTUM*NODE_SPEED
            if(currentJob.req_mem > self.availMem):
                # Swapping out previous job
                executed -= NODE_SWAP_COST*(NODE_MEM - self.availMem)
                # Swapping in new job
                executed -= NODE_SWAP_COST*(currentJob.req_mem)
                # Updating available memory
                self.availMem = NODE_MEM - currentJob.req_mem
            else:
                # Uses available memory
                self.availMem -= currentJob.req_mem

            if(executed > 0):
                currentJob.size -= executed
            
            if(currentJob.size <= 0):
                self.finishJob(currentJob, self.index)


    def finishJob(self, job, index):
        self.queue.remove(index)
        job.finished = 1
        self.availMem -= job.req_mem
        yield hold, self, NODE_SCH_NOTIFICATION
        # Notify scheduler
