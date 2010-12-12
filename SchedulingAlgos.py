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
                  temp = {t : [job]}
                  return round_robin (workerList, temp, scheduler)
            else:
              continue

    for job in orphanjobs:
        try:
            scheduler.taskMeanTimes [job.taskId][0]
        except:
            return []

    orphanjobs.sort (key= lambda x : scheduler.taskMeanTimes [x.taskId][0])
    #orphanjobs = orphanjobs [::-1]

    for machine in workerList:
         jobs = scheduler.jobsPerMachine.get (machine.id)

         S = 0
         if (jobs != None):
             for job in jobs:
                S += scheduler.taskMeanTimes [job.taskId][0]
         sums.append ([S, machine])

    flag = 0
    for each in sums:
        if (each[0] < 1000):
          flag = 1

    sums.sort ()
    allocations = []

    count = 0
    for each in orphanjobs:
         sums[0][0] += scheduler.taskMeanTimes[each.taskId][0]
         count += 1
         if (flag == 0 and count > len(orphanjobs)/2):
            sums[0][1] = None
         allocations.append ([sums[0][1], each])
         sums.sort ()

    return allocations


initialJobs = []
initialWorkers = []

def ESBOT(workerList, tasks, scheduler):

    # Populate initial workers list
    # These workers may not be used
    # for ordinary job executions, since
    # they're used to estimate the mean
    # job time for each task

    iJobs = initialJobs[:]

    for job in iJobs:
        if(job.finished):
            initialJobs.remove(job)
            initialWorkers.remove(job.workerId)
        elif (job.workerId != None and job.workerId not in initialWorkers):
             initialWorkers.append(job.workerId)

    allocations = []
    activeWorkers = len(workerList)

    # start one machine for a job of each
    # not yet allocated task
    nonAllocated = get_non_allocated_tasks(tasks)
    if(nonAllocated >= 0):
        freeMachines = get_free_machines(workerList, scheduler)
        for job in nonAllocated:
            machine = freeMachines.pop()
            if(machine):
                print "Adding job %d from unallocated task %d to idle machine %d" \
                      %(job.jobId, job.taskId, machine.id)
                initialWorkers.append(machine)
            else:
                print "Starting new machine for job %d from unallocated task %d." \
                      %(job.jobId, task.taskId)
                initialWorkers.append(machine)
  
            initialJobs.append(job)
            allocations.append((machine, job))
   
    return allocations
    
def get_free_machines(workerList, sch):
    result = []
    for worker in workerList:
        jobs = sch.jobsPerMachine.get(worker.id)
        if(not jobs):
            result.append(worker)

    return result

def get_non_allocated_tasks(tasks):
    
    result = []

    allocatedTasks = []

    for taskJobs in tasks.values():
      for job in taskJobs:
          if(job.finished or job.workerId != None):
              allocatedTasks.append(job.taskId)
              break;

    for task in tasks:
        if(task not in allocatedTasks):
            result.append(tasks[task][0])
          
    return result

def get_non_allocated_jobs(tasks):
    
    result = []

    for taskJobs in tasks.values():
      for job in taskJobs:
        if(not job.finished and job.workerId == None):
          result.append(job)

    return result
