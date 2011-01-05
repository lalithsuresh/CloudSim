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

def return_next (s, listofworkers):

    start = s
    while (start == listofworkers[0]):
      listofworkers = [listofworkers.pop()] + listofworkers

    return listofworkers

submittedList = []
lastMachineP = None
def irr(workerList, tasks, scheduler):
    global currentMachine
    global lastMachineP

    orphanJobs = get_non_allocated_jobs(tasks)
    allocations = []

    if (orphanJobs == 0):
        return []

    flag = 0
    weights = []
    i = 0
    for each in scheduler.avgRtPerWorker.values():
        weights.append (each[0] - scheduler.avgRT[0])
        if (each[0] < 100):
            flag = 1
        i += 1

    usingWeights = 0
    if (len(weights) == len(workerList)):
       usingWeights = 1
       minWeight = min (weights)
       if (minWeight < 0):
            minWeight = -minWeight
       if (minWeight == 0.0):
          minWeight = 1.0
       normalisedList =  map(lambda x : x + 2*minWeight, weights) # add 1 so that all weights are non-zero
       minWeight = min(normalisedList)
       normalisedList =  map (lambda x : x/minWeight, normalisedList)
       s = sum (normalisedList)
       normalisedList = map (lambda x : s/x, normalisedList)

    x = normalisedList[0]
    allSame = 1
    for each in normalisedList[1:]:
        if (x != each):
          allSame = 0
          break

    if (allSame == 1):
        normalisedList = map (lambda x : 1.0, normalisedList)

    newList = workerList [0:]

    if (usingWeights == 1):
        zz = []
        i = 0
        for each in newList:
            zz = [each] * int(normalisedList[i]) + zz
            i += 1
        newList = zz

    if (flag == 0):
        newList = newList + [scheduler.createMachine(), scheduler.createMachine()] * (int(sum(normalisedList)/2))

    last = None

    reqLen = 0

    for each in scheduler.avgRtPerWorker.values():
      if (each[0] > 0.0):
        reqLen += 1
        
    l = newList[0:]
    x = []
    last = l[0]
    
    while (l != []):
      if(last.id == l[-1].id):
          l = [l.pop()] + l
      else:
          last = l.pop()
          x.append (last)

      allSame = 1
      for each in l[1:]:
        if (each.id == l[0].id):
            allSame = 0

      if (allSame == 1):
          newList = newList + l
          break
    
    newList = x
    
    for job in orphanJobs:
      newList = [newList.pop()] + newList
      machine = newList[0]
      if (machine.id not in submittedList):
        allocations.append([machine, job])
        submittedList.append (machine.id)
      elif (reqLen == len(scheduler.avgRtPerWorker)):
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
              for i in range((len(tasks)-len(workerList))):
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

    ESList.append (ES)
#    flag = 0
#    for each in sums:
#        if (each[0] < max(ESList)/10):
#          flag = 1

    sums.sort ()
    allocations = []

    #if (flag == 0):
    #  machine = scheduler.createMachine ()
    
    #if (ES > 1000 and (ES % 3600 <= 200)):
        #machine = scheduler.createMachine ()

    neededWorkers = 0
    Nlist = []
    for N in range (100):
        #print ((ES)/(len(workerList) + N)) % 3600= 3600, scheduler.scenario.acceptableWaste/100 + scheduler.scenario.wn_startup
        if (((ES)/(len(workerList) + N)) % 3600 >= 3600/scheduler.scenario.acceptableWaste * 20 - scheduler.scenario.wn_startup):
            Nlist.append (N)

    newMachinesList = []

    if (Nlist != []):
        for x in range(max(Nlist)):
           newMachinesList.append (scheduler.createMachine())

    count = 0
    for each in orphanjobs:
         sums[0][0] += scheduler.taskMeanTimes[each.taskId][0]
         count += 1
 #        if (flag == 0):
            #print ES % (3600 - scheduler.scenario.wn_startup), 3600 * scheduler.scenario.acceptableWaste/100 
         if (ES > 1000 and newMachinesList != []):
              newMachinesList = [newMachinesList.pop()] + newMachinesList
              sums[0][1] = newMachinesList[0]
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
