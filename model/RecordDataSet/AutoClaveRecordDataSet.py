import abc
import json
from model.DataSet import DataSet

class SingleAutoClaveRecordEvent(DataSet):
    __prefix = ''
    __recordList = []
    __stateTime = []
    __startTime = 0
    __endTime = 0

    def __init__(self,prefix, claveId):
        self.__prefix = prefix
        self.__recordList = []
        self.__stateTime = []
        self.__claveId = claveId
        self.__startTime = 0
        self.__endTime = 0
    
    def getClaveId(self):
        return self.__claveId

    def getPrefix(self):
        return self.__prefix

    def setPrefix(self, prefix):
        self.__prefix = prefix

    def getType(self):
        return "SingleAutoClaveRecordEvent"
    
    def pushData(self, data):
        self.__recordList.append(data)

    def setStartTime(self, startTime):
        self.__startTime = startTime

    def getStartTime(self):
        return self.__startTime

    def setEndTime(self, endTime):
        self.__endTime = endTime

    def getEndTime(self):
        return self.__endTime

    def pushStateTime(self, stateTime):
        self.__stateTime.append(stateTime)

    def getSet(self, para):
        if para == 'json':
            if(len(self.__recordList)<=1):
                return 0
            else:
                pressure = []
                tempIn = []
                tempOut = []
                state = []
                for data in self.__recordList:
                    time = data.getTime()
  
                    tempIn.append({'t':time,'v':data.getInTemp()})
                    tempOut.append({'t':time,'v':data.getOutTemp()})
                    pressure.append({'t':time,'v':data.getInPress()})
                    state.append({'t':time,'v':data.getState()})

                recordDict = {'FuId':self.__claveId, 'startTime':self.__startTime, 'endTime':self.__endTime, 'stateTime':self.__stateTime, 'data':{'pressure':pressure, 'tempIn':tempIn, 'tempOut':tempOut, 'state':state}} 
                recordJson = json.dumps(recordDict)
                return recordJson
        else:
            return self.__recordList

class SingleAutoClaveRecordDataSet(DataSet):


    def __init__(self, claveId):
        self.eventList = []
        self.claveId = claveId


    def getType(self):
        return "SingleAutoClaveRecordDataSet"


    def pushData(self,data):
        if(isinstance(data,str)):
            event = SingleAutoClaveRecordEvent(data, self.claveId)
            self.eventList.append(event)

        else:
            self.eventList.append(data)


    def getSet(self):
        return self.eventList


#蒸压釜数据记录
class AutoClaveRecordDataSet(DataSet):

    def __init__(self, claveNum, nowTime):
        self.subRecordSetList = []
        self.nowTime = nowTime
        self.claveNum = claveNum
        for claveId in range (1 , self.claveNum+1):
            subRecordSet = SingleAutoClaveRecordDataSet(claveId)
            self.subRecordSetList.append(subRecordSet)

    def getRecordDate(self):
        return self.recordDate

    def getType(self):
        return "AutoClaveRecordDataSet"

    def getNowTime(self):
        return self.nowTime

    def getClaveNum(self):
        return self.claveNum

    def newEvent(self, prefix, claveId):
        return SingleAutoClaveRecordEvent(prefix, claveId)


    def pushData(self,claveId,data):

        self.subRecordSetList[claveId-1].pushData(data)

    def getSet(self,claveId):
        return self.subRecordSetList[claveId-1]

