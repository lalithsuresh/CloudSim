from SimPy.Simulation import *

#def weighted_random_schedule(machineList, scenario):
#    total = 0
#    for m in machineList:
#        total += m.factor
#    value = random.uniform(0, total)
#    total = 0
#    for m in machineList:
#        total += m.factor
#        if value < total:
#            return m
#    return machineList[len(machineList)-1]

def random_schedule(machineList, scenario):
    return machineList[random.randint(0,len(machineList)-1)]

def least_full(machineList, scenario):
    lower=-1
    lowerMachines=[]

    for machine in machineList:
        queue_size=machine.get_queue_size()
        if(queue_size < lower or lower == -1):
            lower=queue_size
            lowerMachine=[machine]
        elif (queue_size == lower):
            lowerMachine.append(machine)
    
    if(len(lowerMachine) > 0):
        return lowerMachine[random.randint(0, len(lowerMachine)-1)]
            
    return machineList[0]

def round_robin(machineList, scenario):
    global currentMachine
    machineToReturn=machineList[currentMachine]
    currentMachine=(currentMachine+1)%len(machineList)
    return machineToReturn

def fill_queue(machineList, scenario):
    global currentMachine
    if(not machineList[currentMachine].can_enqueue_an_element()):
        nextMachine=(currentMachine+1)%len(machineList)
        while nextMachine != currentMachine and not machineList[nextMachine].can_enqueue_an_element():
           nextMachine=(nextMachine+1)%len(machineList)
        currentMachine=nextMachine
    return machineList[currentMachine]
