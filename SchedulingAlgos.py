from SimPy.Simulation import *
from CloudSim import *

def random_schedule(workerList, tasks):

    orfanJobs = get_non_allocated_jobs(tasks)
  
    allocations = []

    for job in orfanJobs:
      allocations.append([workerList[random.randint(0,len(workerList)-1)], job])

    return allocations 

def round_robin(workerList, tasks):
    global currentMachine

    orfanJobs = get_non_allocated_jobs(tasks)
  
    allocations = []

    for job in orfanJobs:
      machine=workerList[currentMachine%len(workerList)]
      currentMachine=(currentMachine+1)%len(workerList)   
      allocations.append([machine, job])

    return allocations 

def get_non_allocated_jobs(tasks):
    
    result = []

    for taskJobs in tasks.values():
      for job in taskJobs:
        if(not job.finished and job.workerId == None):
          result.append(job)

    return result
