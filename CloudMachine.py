from SimPy.Simulation import *
from AbstractResource import *
from time import *
import math

class CloudMachine(Process):
    def __init__(self, mId, scenario, shutdownTime, started=False):
        Process.__init__(self, name="M-"+str(mId))
        self.id = mId
        self.jobs = []
        self.started = started
        self.startTime = 0
        self.stopTime = 0
        self.shutdownTime = shutdownTime
        self.memory = {}
        self.availMem = scenario.wn_mem
        self.scenario = scenario
        self.wasted = 0
        self.debug = False

    def getPossibleJobs(self, avgJobTime):
        leftTime = self.shutdownTime - int(now())
        return int(round(leftTime/avgJobTime))

    def getShutdownTime(self):
        return self.shutdownTime

    def getNumJobs(self):
        return len(self.jobs)

    def addJob (self, job):
        job.workerId = self.id
        self.jobs.append(job)
        if (len(self.jobs) == 1):
            reactivate (self)

    def log(self, msg):
        if(self.debug):
          print self.name + ">> " + msg

    def getExecutionTime(self):
        if(self.stopTime == 0):
            return now() - self.startTime
        else:
            return self.stopTime - self.startTime

    def getPaidTime(self):
        cpuTime = int(self.getExecutionTime())
        return cpuTime + 3600 - (cpuTime%3600)

    def getExecutionCost(self):
        paidTimeInHours = self.getPaidTime()/3600
        return paidTimeInHours * self.scenario.wn_cost

    def getWastedSwapAndStartup(self):
        return self.wasted;

    def getWastedPartialHours(self):
        return 3600 - (self.getExecutionTime()%3600)

    def getWastedTime(self):
        return self.getWastedSwapAndStartup() + self.getWastedPartialHours()

    def start(self):
        if(not self.started):
          self.log("Starting machine.")
          self.startTime = now()
          self.wasted += self.scenario.wn_startup
          self.started = True
          # Holds for startup time
          yield hold,self,self.scenario.wn_startup
       
        index = -1

        while(self.started):

            self.log("Jobs count: " + str(len(self.jobs)))

            if(len(self.jobs) > 0):

                # Selects next job in the list (round robin) 
                index = (index+1)%len(self.jobs) 
                currentJob = self.jobs[index]

                self.log("Selected job: " + str(currentJob.jobId))

                swapInsts = 0

                if(not self.memory.has_key(currentJob.jobId)):
                    swapInsts = self.doSwapping(currentJob)

                neededInstr = currentJob.size + swapInsts
                neededTime =  float(neededInstr)/self.scenario.wn_speed
 
                quantum = min(self.scenario.wn_quantum,neededTime)

                # Executes selected job
                # instructions for a quantum
                yield hold,self,quantum

                self.log("Quantum: " + str(quantum) + "s")

                executed = (int)(quantum*self.scenario.wn_speed) - swapInsts

                if(executed > 0):
                    currentJob.size -= executed

                self.log("Job " + str(currentJob.jobId) + " size is: " + str(currentJob.size) + "insts.")

                if(currentJob.size <= 0):
                    self.finishJob(currentJob)
            else:
                self.wasted += self.scenario.wn_quantum
                yield hold,self,self.scenario.wn_quantum

        print "[%.2f] Machine %d left loop" % (now(), self.id)

    def stop(self):
        self.started = False
        self.stopTime = now()
        for job in self.jobs:
            job.workerId = None
        return self.jobs

    def isFinished(self):
        return not self.started

    def finishJob(self, job):
        self.log("Finishing job " + str(job.jobId))
        self.jobs.remove(job)
        job.finished = True
        self.swapOut(job.jobId)
        self.scenario.scheduler.jobFinished(job, self.id)
    
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

        swapInsts = max(1,int(self.scenario.wn_swap*max(swapOut,swapIn)))

        self.wasted += swapInsts/self.scenario.wn_speed

        return swapInsts
