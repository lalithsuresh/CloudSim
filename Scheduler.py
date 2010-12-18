from SimPy.Simulation import *
from AbstractResource import *
from CloudMachine import *
import math

class Scheduler(Process):
    def __init__(self, scenario, name="Scheduler"):
        Process.__init__ (self, name=name)
        self.scenario = scenario
        
        self.nextHour = 3600
  
        self.nextDestroy = 1000000000

        self.started = False

        self.completedJobs = 0;

        self.genId = 0
        self.activeMachines = {}
        self.destroyedMachines = []

        self.finJobsPerTask = {}
        self.numJobsPerTask = {}
        self.jobsPerMachine = {}

        # [0] startTime
        # [1] remainingJobs
        self.taskInfos = {}
        self.taskJobs = {}

        self.maxRT = 0
        self.avgRT = [0, 0]
        self.minRT = 100000000
        self.maxRtPerWorker = {}
        self.avgRtPerWorker = {}
        self.minRtPerWorker = {}

        temp = scenario.addMonitor ("activeJobsMon")
        scenario.addMonitorPlot ("Running Jobs", temp)

        scenario.addMonitor("jobRT")
        scenario.addMonitor("taskRT")

        temp = scenario.addMonitor ("activeNodesMon")
        scenario.addMonitorPlot ("Running nodes", temp)

        temp = scenario.addMonitor("jobRTAvgMon")
        scenario.addMonitorPlot ("Average job response time", temp)

        temp = scenario.addMonitor("taskRTAvgMon")
        scenario.addMonitorPlot ("Average task response time", temp)

        temp = scenario.addMonitor("executionCostMon")
        scenario.addMonitorPlot ("Cost of execution", temp)

        self.taskMeanTimes = {} #TaskId > meanCompletionTime

        self.jobTimes = {} #JobId > jobCompletionTime

#        self.taskIncomingRate = {} #TaskId >

        self.nextPoll = 0

        self.lastAvgJobRT = 0
        self.lastAvgTaskRT = 0

        self.rescheduled = False
#self.hasFinished = False

        self.hasFinished = {}

    def addJob(self, job):
        job.startTime = now()
 
#        try:
#          incomingRate = self.taskIncomingRate[job.taskId]
#          if(incomingRate[1] == now()):
#            incomingRate[0] += 1
#            self.taskIncomingRate[job.taskId] = incomingRate
#        except:
#          self.taskIncomingRate[job.taskId] = [1, now()]
      
        self.jobTimes[(job.taskId, job.jobId)] = -now()

        jobList = self.taskJobs.get(job.taskId)
        if(jobList == None):
            jobList = []
            taskInfo = [job.startTime, job.numjobs]
            self.taskInfos[job.taskId] = taskInfo
        
        jobList.append(job)
        self.taskJobs[job.taskId] = jobList

    def jobFinished(self, job, machineId):
        self.hasFinished[job.taskId] = True

#self.log("Job " + str(job.jobId) + " from task " + str(job.taskId) + " finished on machine " + str(machineId))

        finishTime = now()
    
        self.completedJobs += 1

        self.jobTimes[(job.taskId, job.jobId)] += finishTime

        jobTime = self.jobTimes[(job.taskId, job.jobId)]
        try:
          previousMean = self.taskMeanTimes[job.taskId]
          self.taskMeanTimes[job.taskId] = \
                  [(previousMean[0]*previousMean[1] + jobTime)/(previousMean[1]+1), previousMean[1]+1]
#print "Task mean of task %d was updated to %f" %(job.taskId, self.taskMeanTimes[job.taskId][0])

        except:
          self.taskMeanTimes[job.taskId] = [jobTime, 1]

        # Remove job from job list
        self.taskJobs[job.taskId].remove(job)
        jobsInMachine = self.jobsPerMachine.get(machineId)
        jobsInMachine.remove(job)
        
        # Calculate job service time
        jobRT = finishTime - job.startTime
        self.scenario.monitors ["jobRT"].observe (jobRT)

        #Update average, max and minimum service time
        self.avgRT = [(self.avgRT[0]*self.avgRT[1]+jobRT)/(self.avgRT[1]+1), self.avgRT[1]+1]
        self.maxRT = max([jobRT,self.maxRT])
        self.minRT = min([jobRT,self.minRT])
        workerRT = self.avgRtPerWorker[machineId]
        self.avgRtPerWorker[machineId] = [(workerRT[0]*workerRT[1]+jobRT)/(workerRT[1]+1), workerRT[1]+1]
        self.maxRtPerWorker[machineId] = max([self.maxRtPerWorker[machineId], jobRT])
        self.minRtPerWorker[machineId] = min([self.minRtPerWorker[machineId], jobRT])

#print "avg RT: %s, max RT: %s, min RT: %s, worker %s min RT: %s, max RT %s, avg RT: %s)" % (self.avgRT, self.maxRT, self.minRT, machineId, self.minRtPerWorker[machineId], self.maxRtPerWorker[machineId], self.avgRtPerWorker[machineId]) 

        # Complete job in task info and check if
        # task is completed
        taskInfo = self.taskInfos[job.taskId]
        taskInfo[1] -= 1

        self.numJobsPerTask[job.taskId] -= 1
        self.finJobsPerTask[job.taskId] += 1

        # No jobs remaining - Task finished!
        if(taskInfo[1] == 0):
            print "Task %d is finished" %(job.taskId)
            # Calculate task response time
            taskRT = finishTime - taskInfo[0] #initialTime
            self.scenario.monitors ["taskRT"].observe (taskRT)
        
        allFinished = True
        for info in self.taskInfos.values():
            if(info[1] > 0):
                allFinished = False
                break

        if(allFinished and self.scenario.remainingTasks == 0):
            stopSimulation()
    def run(self):

        # Create initial machines
        for m_id in range(self.scenario.initial_machines):
           self.createMachine(True, shutdown=self.scenario.sim_time)

        self.started = True

        while (self.started):

            if(int(now()) >= self.nextDestroy):
                nextNextDestroy = 10000000000
                for machine in list(self.activeMachines.values()):
                    thisDestroy = machine.getShutdownTime()
                    if(int(now()) >= thisDestroy):
                        self.destroyMachine(machine)
                    elif(thisDestroy < nextNextDestroy):
                        nextNextDestroy = thisDestroy
                self.nextDestroy = nextNextDestroy

            allocations = self.scenario.schedule_algorithm (self.activeMachines.values(), self.taskJobs, self)

            self.allocate(allocations)

            if(int(now()) == self.nextPoll):
                self.scenario.monitors ["activeJobsMon"].observe (sum(map(lambda x: len(x), self.taskJobs.values())))
                self.scenario.monitors ["activeNodesMon"].observe (len(self.activeMachines))

                self.scenario.monitors ["jobRTAvgMon"].observe (self.getAvgJobRT())
                self.scenario.monitors ["taskRTAvgMon"].observe (self.getAvgTaskRT())

                currentCost = 0

                allMachines = self.activeMachines.values()+self.destroyedMachines
                for machine in allMachines:
                    currentCost += machine.getExecutionCost()
                self.scenario.monitors ["executionCostMon"].observe (int(currentCost))

                self.nextPoll += self.scenario.pollingInterval

            yield hold, self, self.scenario.sch_interval

    def allocate(self, allocations):
        # iterate over allocations generated by scheduling algorithm
        for machine_job in allocations:
            machine = machine_job[0]
            job = machine_job[1]

            if(machine == None or job == None):
                # Invalid allocation
                self.log("Invalid allocation")
                continue

            # start job on machine
#self.log("Adding job " + str(job.jobId) + " from task " + str(job.taskId) + " on machine " + str(machine.id))
            machine.addJob(job)
 
            jobsInMachine = self.jobsPerMachine[machine.id]
            jobsInMachine.append(job)

            if(self.numJobsPerTask.get(job.taskId) == None):
                self.numJobsPerTask[job.taskId] = 0
                self.finJobsPerTask[job.taskId] = 0
                self.hasFinished[job.taskId] = False
                
            self.numJobsPerTask[job.taskId] += 1

    def log(self, msg):
        print "[%d] %s" % (now(), msg)

    def getAvgJobRT(self):

        windowStart = max(now()-self.scenario.averageWindow, 0)

        jobRTs = self.scenario.monitors["jobRT"]
        avgJobRT = 0
        count = 0
        for jobRT in jobRTs:
            if(jobRT[0] >= windowStart):
                avgJobRT += jobRT[1]
                count += 1

        if(count > 0):
            avgJobRT = avgJobRT / count
        else:
            avgJobRT = self.lastAvgJobRT

        self.lastAvgJobRT = avgJobRT
      
        return avgJobRT

    def getAvgTaskRT(self):

        windowStart = max(now()-self.scenario.averageWindow, 0)

        taskRTs = self.scenario.monitors["taskRT"]
        avgTaskRT = 0
        count = 0
        for taskRT in taskRTs:
            if(taskRT[0] >= windowStart):
                avgTaskRT += taskRT[1]
                coTaskRT= 1

        if(count > 0):
            avgTaskRT = avgTaskRT / count
        else:
            avgTaskRT = self.lastAvgTaskRT
      
        self.lastAvgTaskRT = avgTaskRT

        return avgTaskRT

    def guessEstimatedTime(self, taskId):
        remainingJobs = self.taskInfos[taskId][1]

        remainingJobs -= len(self.taskJobs[taskId])

        return remainingJobs    

 
    def destroyMachine(self, machine):
        print "[%d] Destroying machine %d" % (now(), self.genId)
        machine.stop()
        #cancel(machine)
        del self.maxRtPerWorker[machine.id]
        del self.avgRtPerWorker[machine.id]
        del self.minRtPerWorker[machine.id]
        del self.jobsPerMachine[machine.id]
        del self.activeMachines[machine.id]
        self.destroyedMachines.append(machine)

    def createMachine(self, started=False, shutdown=3550):
        shutdown += int(now())

        print "[%d] Creating machine %d. Shutdown time: %d" % (now(), self.genId, shutdown)
        
        if(shutdown < self.nextDestroy):
            self.nextDestroy = shutdown

        machine = CloudMachine(self.genId, self.scenario, shutdown, started)
        self.jobsPerMachine[machine.id] = []
        self.activeMachines[machine.id] = machine
        self.maxRtPerWorker[machine.id] = 0
        self.avgRtPerWorker[machine.id] = [0, 0]
        self.minRtPerWorker[machine.id] = 10000000
        self.genId += 1
        activate(machine, machine.start())
        return machine


    def stop(self):
        self.started = False

