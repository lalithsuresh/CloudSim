from SimPy.Simulation import *
from CloudSim import *

def random_schedule(workerList, tasks, scheduler):

    orphanJobs = get_non_allocated_jobs(tasks)
  
    allocations = []

    for job in orphanJobs:
      allocations.append([workerList[random.randint(0,len(workerList)-1)], job])

    return allocations 

def round_robin(workerList, tasks, scheduler):
    global currentMachine

    orphanJobs = get_non_allocated_jobs(tasks)
    allocations = []

    for job in orphanJobs:
      machine=workerList[currentMachine%len(workerList)]
      currentMachine=(currentMachine+1)%len(workerList)   
      allocations.append([machine, job])

    return allocations 

idsSubmitted = []

def longest_processing_time_first (workerList, tasks, scheduler):
    sums = []

    orphanjobs = get_non_allocated_jobs (tasks)

    if (orphanjobs == []):
        return []

    for job in orphanjobs:
        try:
            scheduler.taskMeanTimes [job.taskId][0]
        except:
            if (job.taskId not in idsSubmitted):
              allocations = []
              allocations.append ([workerList[0], job])
              idsSubmitted.append (job.taskId)

              for t in tasks:
                if (job in tasks[t]):
                  return round_robin (workerList, tasks, scheduler)
            else:
              continue

    for job in orphanjobs:
        try:
            scheduler.taskMeanTimes [job.taskId][0]
        except:
            return []

    orphanjobs.sort (key= lambda x : scheduler.taskMeanTimes [x.taskId][0])
    for machine in workerList:
         jobs = scheduler.jobsPerMachine.get (machine.id)

         S = 0
         if (jobs != None):
             for job in jobs:
                S += scheduler.taskMeanTimes [job.taskId][0]
         sums.append ([S, machine])

    sums.sort ()
    allocations = []

    for each in orphanjobs:
         sums[0][0] += scheduler.taskMeanTimes[each.taskId][0]
         allocations.append ([sums[0][1], each])
         sums.sort ()

    return allocations

def get_non_allocated_jobs(tasks):
    
    result = []

    for taskJobs in tasks.values():
      for job in taskJobs:
        if(not job.finished and job.workerId == None):
          result.append(job)

    return result
