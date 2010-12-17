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
done = 0
ESList = []

def longest_processing_time_first (workerList, tasks, scheduler):
    sums = []

    orphanjobs = get_non_allocated_jobs (tasks)

    if (orphanjobs == []):
        return []

    for job in orphanjobs:
        try:
            scheduler.taskMeanTimes [job.taskId][0]
        except:
            if(len(workerList) < len(tasks)):
              for i in range(len(tasks)-len(workerList)):
                  scheduler.createMachine()

            if (job.taskId not in idsSubmitted):
              allocations = []
              allocations.append ([workerList[0], job])
              idsSubmitted.append (job.taskId)

              for t in tasks:
                if (job in tasks[t]):
                  templist = tasks[t][0:]
                  templist.remove (job)
                  temp = {t : [job] + templist[:len(templist)/10]}
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

    ES = 0

    for each in idsSubmitted:
        ES += scheduler.guessEstimatedTime(each) * scheduler.taskMeanTimes[each][0]

#print ES%3600
    ESList.append (ES)
    flag = 0
    for each in sums:
        if (each[0] < max(ESList)/10):
          flag = 1

    sums.sort ()
    allocations = []

    #if (flag == 0):
    #  machine = scheduler.createMachine ()
    
    if (ES > 1000 and (ES % 3600 <= 200)):
        scheduler.createMachine ()

    count = 0
    for each in orphanjobs:
         sums[0][0] += scheduler.taskMeanTimes[each.taskId][0]
         count += 1
         if (flag == 0):
            sums[0][1] = None
         allocations.append ([sums[0][1], each])
         sums.sort ()

    return allocations

initialJobs = []

def ESBOT(workerList, tasks, scheduler):

    allocations = initESBOT(workerList, tasks, scheduler)

    initWorkers = get_initial_workers()

    necessaryComps = 0

    hostProcessingTime = 3600 # Use percentage of waste here


        
  
    return allocations

def initESBOT(workerList, tasks, scheduler):
    for job in list(initialJobs):
        if(job.finished):
            print "[%d] Initial job %d from task %d finished." %(now(), job.jobId, job.taskId)
            initialJobs.remove(job)

    allocations = []

    neverAllocated = get_never_allocated_tasks(tasks, scheduler)
    if(neverAllocated):
        freeMachines = get_free_machines(workerList, scheduler)
        for job in neverAllocated:
            machine = None
            try:
                machine = freeMachines.pop()
                print "[%d] Adding job %d from unallocated task %d to idle machine %d" \
                      %(now(), job.jobId, job.taskId, machine.id)
            except IndexError:
                machine = scheduler.createMachine()
                print "[%d] Starting new machine for job %d from unallocated task %d." \
                      %(now(), job.jobId, job.taskId)
 
            initialJobs.append(job)
            allocations.append((machine, job))

    return allocations

def get_initial_workers():
    return map( lambda x : x.workerId, initialJobs )

def get_free_machines(workerList, sch):
    result = []
    for worker in workerList:
        jobs = sch.jobsPerMachine.get(worker.id)
        if(not jobs):
            result.append(worker)

    return result

def get_never_allocated_tasks(tasks, sch):
    
    result = []

    allocatedTasks = []

    # Tasks that have mean times
    # were already allocated
    for task in sch.taskMeanTimes:
        allocatedTasks.append(task)

    # Tasks that have jobs running
    # were already allocated
    for taskJobs in tasks.values():
        for job in taskJobs:
            if(job.finished or job.workerId != None):
                allocatedTasks.append(job.taskId)
                break;

    if(len(allocatedTasks) < len(tasks)):
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
