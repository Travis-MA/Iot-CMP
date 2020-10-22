import abc
import json
import numpy as np


class AutoClaveRealtime:

    def __init__(self, id, dev_id, data_set):
        self.__clave_id = id
        self.__device_id = dev_id
        self.__data_set = data_set
        pass

    def get_dev_id(self):
        return self.__device_id

    def get_clave_id(self):
        return self.__clave_id

    def get_type(self):
        return 'AutoClaveRealTime'

    def getSet(self, type):
        if type == 'json':
            recordListJson = []
            for data in self.__recordList:
                recDict = {'time': data.getTime(), 'inTemp': int(100 * data.getInTemp()),
                           'outTemp': int(100 * data.getOutTemp()), 'inPress': int(100 * data.getInPress()),
                           'state': data.getState()}
                recordListJson.append(recDict)
            obsRecDict = {'claveId': self.getClaveID(), 'records': recordListJson}
            return json.dumps(obsRecDict)

        elif type == 'numpy':
            return self.__toNumPy(self.__recordList, [1 / 6, 1 / 6, 1 / 6, 1 / 6, 1 / 6, 1 / 6])

        elif type == 'list:':
            return self.__recordList

    def getLastTime(self):
        if len(self.__recordList) > 0:
            return self.__recordList[-1].getTime()
        else:
            return 0

    def __toNumPy(self, recordList, window):
        # 得到实时数据
        timeList = []
        inTempList = []
        outTempList = []
        inPressList = []
        stateList = []

        for autoClaveData in recordList:
            time = int(autoClaveData.getTime())

            inTemp = autoClaveData.getInTemp()
            outTemp = autoClaveData.getOutTemp()
            inPress = autoClaveData.getInPress()
            state = autoClaveData.getState()

            timeList.append(time)
            inTempList.append(inTemp)
            outTempList.append(outTemp)
            inPressList.append(inPress)
            stateList.append(state)

            # print("Id:"+str(claveId)+" T:"+str(time)+" iT:"+str(inTemp)+" oT:"+str(outTemp)+" iP:"+str(inPress)+" S:"+str(state))
        try:
            dataSet = np.array([timeList, inTempList, outTempList, inPressList, stateList])
            start = int(len(window) / 2)
            conv1 = np.convolve(dataSet[1, :], window)
            conv2 = np.convolve(dataSet[2, :], window)
            conv3 = np.convolve(dataSet[3, :], window)
            dataSet[1, :] = conv1[start:start + dataSet.shape[1]]
            dataSet[2, :] = conv2[start:start + dataSet.shape[1]]
            dataSet[3, :] = conv3[start:start + dataSet.shape[1]]
            return dataSet
        except:
            return [0]


# 蒸压釜实时数据
class AutoClaveRealTimeDataSet(DataSet):
    __AutoClaveDataSetList = []
    __AutoClaveNum = 0

    def __init__(self, claveNum):
        self.__AutoClaveNum = claveNum
        self.__AutoClaveDataSetList = []
        for claveId in range(1, self.__AutoClaveNum + 1):
            self.__AutoClaveDataSetList.append(SingleAutoClaveRealtimeDataSet(claveId, dev_prefix + str(claveId)))

    def getType(self):
        return 'AutoClaveRealTimeDataSet'

    def pushData(self, ID, val):
        if val.getType() == 'AutoClaveRealTimeData':
            self.__AutoClaveDataSetList[ID - 1].pushData(val)
        else:
            print('[AutoClaveRealTimeDataSet] Data Type Error')

    def getDevId(self, ID):
        return self.__AutoClaveDataSetList[ID - 1].getDevId()

    def getClaveNum(self):
        return self.__AutoClaveNum

    def getSet(self, ID):
        return self.__AutoClaveDataSetList[ID - 1]

    def getLastTime(self, ID):
        if len(self.__AutoClaveDataSetList[ID - 1].getSet()) > 0:
            return self.__AutoClaveDataSetList[ID - 1].getSet()[-1].getTimeStemp()
        else:
            return 0
