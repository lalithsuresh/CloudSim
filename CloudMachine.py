from SimPy.Simulation import *
from AbstractResource import *
from time import *

class CloudMachine(Process):
    def __init__(self, mId, scenario, start=0):
        Process.__init__(self, name="M-"+str(mId))
        self.id = mId
        self.jobs = {}
        self.started = start
        self.startTime = 0
        self.memory = {}
        self.availMem = scenario.wn_mem
        self.info = scenario
        self.debug = True

    def addJob (self, job):
        job.worker = self
        self.jobs[job.jobId] = job
        if (len(self.jobs) == 1):
            reactivate (self)

    def log(self, msg):
        if(self.debug):
          print self.name + ">> " + msg

    def start(self):
        if(self.started == 0):
          self.startTime = now()
          self.log("Starting machine.")
          self.started = 1
          # Holds for startup time
          yield hold,self,self.info.wn_startup
       
        index = -1

        while(self.started):
            if(len(self.jobs) == 0):
               yield passivate, self

            self.log("Jobs count: " + str(len(self.jobs)))

            # Selects next job in the list (round robin) 
            index = (index+1)%len(self.jobs) 
            currentJob = self.jobs.items()[index][1]

            self.log("Selected job: " + str(currentJob.jobId))

            swapInsts = 0

            if(not self.memory.has_key(currentJob.jobId)):
                swapInsts = self.doSwapping(currentJob)

            neededInstr = currentJob.size + swapInsts
            neededTime =  float(neededInstr)/self.info.wn_speed
 
            quantum = min(self.info.wn_quantum,neededTime)

            # Executes selected job
            # instructions for a quantum
            yield hold,self,quantum

            self.log("Quantum: " + str(quantum) + "s")

            executed = (int)(quantum*self.info.wn_speed) - swapInsts

            if(executed > 0):
                currentJob.size -= executed

            self.log("Job " + str(currentJob.jobId) + " size is: " + str(currentJob.size) + "insts.")

            if(currentJob.size <= 0):
                self.finishJob(currentJob)


    def finishJob(self, job):
        self.log("Finishing job " + str(job.jobId))
        del self.jobs[job.jobId]
        job.finished = 1
        self.swapOut(job.jobId)
        # Notify scheduler

    
    def swapOut(self, jobId):
        mem = self.memory.pop(jobId)

        self.availMem += mem
        self.log("Swapped out " + str(mem) + "MB from job " + str(jobId) + ".")
        self.log("Available memory: " + str(self.availMem))
        
        return mem

    def swapIn(self, jobId, jobMem):
        self.memory[jobId] = jobMem
        self.availMem -= jobMem
        self.log("Swapped in " + str(jobMem) + "MB for job " + str(jobId) + ".")
        self.log("Available memory: " + str(self.availMem))
        return jobMem

    def doSwapping(self, job):
        swapOut = 0
        swapIn = 0
        
        # Not enough space in memory - SWAP OUT
        if(self.availMem < job.req_mem):
            neededMem = job.req_mem - self.availMem
            # Swaps out jobs until it swapped out
            # the needed memory
            for memJob in self.memory.items():
              swapOut += self.swapOut(memJob[0])
              if(swapOut >= neededMem):
                break

        # Memory has enough space - SWAP IN
        swapIn = self.swapIn(job.jobId,job.req_mem)

        return max(1,int(self.info.wn_swap*max(swapOut,swapIn)))
