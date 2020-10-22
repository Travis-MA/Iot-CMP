#!/usr/bin/python
# -*- coding:utf-8 -*-

import os
import sys,functools,time
import json
import urllib3
import requests
import base64
import src.com.dis.utils.util as util
from src.com.dis.models import disrequest
from src.com.dis.models import disauth
# from src.com.dis.response import disresponse
from src.com.dis.response import disadminresponse
from src.com.dis.response import disrecordresponse
from src.com.dis.response import discheckpointresponse
from src.com.dis.models.disexception import DisException
from src.com.dis.models.base_model import IS_PYTHON2,IS_PYTHON35_UP,BASESTRING,IS_WINDOWS



DEFAULT_QUERY_RETRY_COUNT = 10
DEFAULT_QUERY_RETRY_INTERVAL = 0.2

RECORDS_RETRIES=20
EXCEPTION_RETRIES=10
stream_mes={}

if IS_PYTHON35_UP:
    requests.packages.urllib3.disable_warnings()
if IS_PYTHON2:
    urllib3.disable_warnings()



def log(message):
    import logging
    logger = logging.getLogger()
    logger.setLevel(logging.WARNING)
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger.warning(message)
    logger.removeHandler(ch)
    return logger

class disclient(object):
    """ Construct the disclient with endpoint, ak, sk, projectid.

    :type endpoint: string
    :param endpoint: dis service host name and port, for example, dis.cn-north-1.myhuaweicloud.com:20004

    :type ak: string
    :param ak: hws accesskey

    :type sk: string
    :param sk: hws secretkey
    
    :type region: string
    :param region: the service deploy region    
    
    :type projectid: string
    :param projectid: hws project id for the user  
    
    : the user can get the ak/sk/projectid  from the hws,the user can refer https://support.huaweicloud.com/usermanual-dis/dis_01_0043.html
    """

    DIS_SDK_VERSION = '1.0.0'
    USER_AGENT = 'dis-python-sdk-v-' + DIS_SDK_VERSION
    TIME_OUT = 60

    def __init__(self, endpoint, ak, sk, projectid, region,bodySerializeType=''):
        self.endpoint = endpoint
        if not endpoint.startswith("http") :
            self.endpoint = "https://" + endpoint
        self.host = endpoint.split("//")[-1]
        self.ak = ak
        self.sk = sk
        self.projectid = projectid
        self.region = region
        self.bodySerializeType = bodySerializeType
        self._timeout = self.TIME_OUT
        self._useragent = self.USER_AGENT
        self.result = []


    def updateAuthInfo(self, ak, sk, projectid, region):
        self.ak = ak
        self.sk = sk
        self.projectid = projectid
        self.region = region

    def setUserAgent(self, useragent):
        self._useragent=useragent


    def __assert_to_validate(self, param, msg):
        param = util.safe_encode(param)
        if param is None or util.toString(param).strip() == '' or param==b'':
            raise Exception(msg)


    def _generateRequest(self, method, uri, query={}, headers={}, body="", userak="", usersk="", userxSecrityToken=""):
        req = disrequest.disRequest(method=method, host=self.host, uri=uri, query=query, headers=headers, body=body)
        ak = self.ak
        sk = self.sk
        if userak is not "":
            ak = userak
        if usersk is not "":
            sk = usersk
        signer = disauth.Signer(ak, sk, self.region)
        signer.Sign(req)
        req.headers["user-agent"]=self._useragent
        req.headers['X-SDK-Version']="1.0.0/python"
        # req.headers["Content-type"]="application/json"
        req.headers["Content-Type"] = "application/json; charset=UTF-8"

        if userxSecrityToken is not "":
            req.headers["X-Security-Token"]=userxSecrityToken

        if (headers):
            headers.update(req.headers)
            req.headers = headers
        # print(req.__dict__)
        return req

    def _sendRequest(self, rawRequest):
        retryCount = 0
        url = self.endpoint + rawRequest.uri
        wait = 0.05
        while retryCount <= EXCEPTION_RETRIES:
            try:
                if retryCount != 0:
                    time.sleep(wait)
                    wait = wait * 2
                    rawRequest.headers.pop(disauth.HeaderXDate)
                    rawRequest.headers.pop(disauth.HeaderAuthorization)
                    disauth.Signer(self.ak, self.sk, self.region).Sign(rawRequest)
                r = requests.request(method=rawRequest.method, url = url, params=rawRequest.query, data=rawRequest.body,
                                     headers=rawRequest.headers, timeout = self.TIME_OUT,verify=False)
                if r.status_code >= 200 and r.status_code < 300:
                    if r.content is not b'':
                        if 'protobuf' in rawRequest.headers["Content-Type"]:
                            jsonResponse = r.content
                        else:
                            jsonResponse = r.json()
                        if jsonResponse:
                            return r.status_code,jsonResponse
                        else:
                            return r.status_code,{}
                    else:
                        return r.status_code, {}
                else:
                    errMsg = ''
                    errNo = ''
                    if r._content is not b'':
                        errNo = str(r.status_code)
                        errMsg = str(r._content)
                        #if r.status_code >= 500:
                        #    try:
                        #        jsonResponse = r.json()
                        #        errMsg = jsonResponse["message"]
                        #        errNo  = jsonResponse["errorCode"]
                        #    except:pass

                    raise DisException("GetResponseErr", "the response is err", r.status_code, errNo, errMsg)
            except Exception as ex:
                if retryCount < EXCEPTION_RETRIES and \
                        (type(ex) == DisException and ex.respStatus >= 500
                         or "connect timeout" in str(ex)
                         or ("read timeout" in str(ex) and rawRequest.method == "GET")):
                    log("Find Retriable Exception [" + str(ex) + "], url [" + rawRequest.method + " " + rawRequest.uri + "], currRetryCount is " + str(retryCount))
                    retryCount = retryCount + 1
                else:
                    if type(ex) == DisException:
                        raise ex
                    else:
                        raise DisException('_sendRequest', str(ex))



    def createStream(self, streamName, partitionCount, streamType="COMMON", ak="", sk="", xSecrityToken="",
                     jsonBody=''):

        '''
        create a dis stream . the stream captures and transport data records.

        you should specify the stream name ,the partition count and the streamtype

        the stream name is the id for the stream.it  Cannot repeat for a account of hws.

        the partition is the capacity for the stream. there are two types partition:COMMON and ADANCE

        the COMMON partition capacity is 1MB input per second, 1000 records input per second, 2MB output per second
        the ADVANCE partition capacity is 5MB input per second, 2000 records input per second, 10MB output per second

        the user specify the count and the type of partitions
        '''

        self.__assert_to_validate(streamName, "the stream Name is null")
        jsonString = json.dumps(jsonBody)
        uri = "/v2/" + self.projectid + "/streams/"
        req = self._generateRequest("POST", uri, body=jsonString, headers={}, query={}, userak=ak, usersk=sk,
                                    userxSecrityToken=xSecrityToken)
        (statusCode, responseData) = self._sendRequest(req)
        return disadminresponse.disCreateStreamResponse(statusCode, responseData)

    def add_dump_task(self, streamName, partitionCount, streamType, ak="", sk="", xSecrityToken="",jsonBody=''):
        self.__assert_to_validate(streamName, "the stream Name is null")
        jsonString = json.dumps(jsonBody)
        uri = "/v2/" + self.projectid + "/stream/" + streamName + '/transfer-tasks/'
        req = self._generateRequest("POST", uri, body=jsonString, headers={}, query={}, userak=ak, usersk=sk,
                                    userxSecrityToken=xSecrityToken)
        (statusCode, responseData) = self._sendRequest(req)
        return disadminresponse.disCreateStreamResponse(statusCode, responseData)


    def deleteStream(self, streamName, ak="", sk="", xSecrityToken=""):
        '''
        delete a stream , all its partitions and records in the partitions.
        
        before deleting, you make sure all of the app operating the stream are closed 
        '''
        self.__assert_to_validate(streamName, "the stream Name is null")
        uri = "/v2/"+ self.projectid + "/streams/" + streamName
        req = self._generateRequest("DELETE", uri, headers={}, query={}, body="",userak=ak, usersk=sk, userxSecrityToken=xSecrityToken)

        (statusCode, responseData) = self._sendRequest(req)
        return disadminresponse.disDeleteStreamResponse(statusCode, responseData)


    def delete_dump_task(self, streamName,task_name='',ak="", sk="", xSecrityToken=""):

        self.__assert_to_validate(streamName, "the stream Name is null")
        self.__assert_to_validate(task_name, "the task_name is null")
        uri = "/v2/" + self.projectid + "/stream/" + streamName + "/transfer-tasks/" + task_name + '/'
        req = self._generateRequest("DELETE", uri, headers={}, query={}, body="", userak=ak, usersk=sk,
                                    userxSecrityToken=xSecrityToken)

        (statusCode, responseData) = self._sendRequest(req)
        return disadminresponse.disDeleteStreamResponse(statusCode, responseData)



    def listStream(self, startStreamName="", limit = 100, ak="", sk="", xSecrityToken=""):
        '''
        list all of the stream of the user.
        
        the MAX of limit is 100,if larger than 100, the sdk should raise exception
        '''

        param = {}
        if startStreamName is not '':
            self.__assert_to_validate(startStreamName, "the stream Name is null")
            param["start_stream_name"]=startStreamName

        if limit > 100:
            raise DisException("invalidparam", "the limit is to large")

        if limit != 0:
            param["limit"]=str(limit)

        uri = "/v2/"+ self.projectid + "/streams/"
        req = self._generateRequest("GET", uri, query=param, headers={}, body="", userak=ak, usersk=sk, userxSecrityToken=xSecrityToken)

        (statusCode, responseData) = self._sendRequest(req)
        return disadminresponse.disListStreamResponse(statusCode, responseData)


    def describeStream(self, streamName, startPartitionId = "", limitPartitions = 1000, ak="", sk="", xSecrityToken=""):
        self.__assert_to_validate(streamName, "the streamname is null")
        param = {}

        if startPartitionId is not '':
            param["start_partitionId"]=startPartitionId

        if limitPartitions > 10000:
            raise DisException("invalidparam", "the limit is to large")

        if limitPartitions != 0:
            param["limit_partitions"]=str(limitPartitions)


        uri = "/v2/"+ self.projectid + "/streams/" + streamName + "/"

        req = self._generateRequest("GET", uri, query=param, headers={}, body="",userak=ak, usersk=sk, userxSecrityToken=xSecrityToken)

        (statusCode, responseData) = self._sendRequest(req)
        return disadminresponse.disDescribeStreamresultResponse(statusCode, responseData)


    def list_dump_task(self, streamName, ak="", sk="", xSecrityToken=""):
        self.__assert_to_validate(streamName, "the streamname is null")
        uri = "/v2/" + self.projectid + "/stream/" + streamName + "/" + "transfer-tasks/"

        req = self._generateRequest("GET", uri, headers={}, body="", userak=ak, usersk=sk,
                                    userxSecrityToken=xSecrityToken)

        (statusCode, responseData) = self._sendRequest(req)
        return disadminresponse.dislistdumptaskResponse(statusCode, responseData)

    def describe_dump_task(self, streamName, task_name, ak="", sk="", xSecrityToken=""):

        self.__assert_to_validate(streamName, "the stream Name is null")
        self.__assert_to_validate(task_name, "the task_name is null")

        uri = "/v2/" + self.projectid + "/stream/" + streamName + "/" + "transfer-tasks/"

        req = self._generateRequest("GET", uri, headers={}, body="", userak=ak, usersk=sk,
                                    userxSecrityToken=xSecrityToken)

        (statusCode, responseData) = self._sendRequest(req)
        for i in responseData['details']:
            if task_name == i['task_name']:
                return disadminresponse.disdescribedumptaskResponse(statusCode, i)
            else:
                raise DisException('errormeaasge','%s not exists'%(i['task_name']))

    def sendRecords(self, streamName, records, ak="", sk="", xSecrityToken=""):
        '''
        send records to the specify stream.
        
        :type streamName string
        :param streamName the streamName ID which want to send data
        
        :type records list
        :param records the data will be send,every one record MUST include two field: data and partition key.
        
        the data field is the RAW data will be sending to DIS.
        the partition_key field is the partition value, identify which partition should save the data
        '''

        if self.bodySerializeType:
            from src.com.dis.models import send_records_pb2
            p = send_records_pb2.PutRecordsRequest()
            p.streamName = streamName
            for j in records:
                p1 = p.records.add()
                if j.get('partition_key')!=None:
                    p1.partitionKey = j.get('partition_key')
                if j.get('partition_id') != None:
                    p1.partitionId = j.get('partition_id')

                # if j.get('explicit_hash_key') != None:
                #     p1.explicitHashKey  = j.get('explicit_hash_key')

                if IS_PYTHON2:
                    p1.data = bytes(str(j.get('data')))
                else:
                    p1.data=bytes(str(j.get('data')),encoding='utf-8')
            records = p.SerializeToString()
            uri = "/v2/" + self.projectid + "/records/"
            req = self._generateRequest("POST", uri, body=records, headers={}, query={}, userak=ak, usersk=sk,
                                        userxSecrityToken=xSecrityToken)
            req.headers["Content-Type"] = "application/x-protobuf;charset=utf-8"
        else:
            def data_Base64(x):
                if sys.version_info.major == 2 or sys.version_info.major < 3:
                    return base64.b64encode(str(x.get('data')))
                else:
                    tempdata=base64.b64encode(bytes(str(x.get('data')),encoding='utf-8'))
                    return str(tempdata, 'utf-8')

            datas=list(map(data_Base64,records))

            partition_keys=list(map(lambda x:x.get('partition_key'),records))
            if partition_keys[0]!=None:
                a = list(zip(partition_keys, datas))
                records = [{'partition_key': i, 'data': j} for i,j in a]

            partition_ids = list(map(lambda x: x.get('partition_id'), records))
            if partition_ids[0]!=None:
                a = list(zip(partition_ids, datas))
                records = [{'partition_id': i, 'data': j} for i, j in a]

            # explicit_hash_keys = list(map(lambda x: x.get('explicit_hash_key'), records))
            # if explicit_hash_keys[0] != None:
            #     a = list(zip(explicit_hash_keys, datas))
            #     new_records = [{'explicit_hash_key': i, 'data': j} for i, j in a]

            jsonBody = {"stream_name": streamName, "records": records}
            jsonString = json.dumps(jsonBody)

            uri = "/v2/" + self.projectid + "/records/"
            req = self._generateRequest("POST", uri, body=jsonString, headers={}, query={}, userak=ak, usersk=sk,
                                        userxSecrityToken=xSecrityToken)

        (statusCode, responseData) = self._sendRequest(req)

        return disrecordresponse.disPutRecordsResponse(statusCode, responseData)

    def list_of_groups(self,init_list, childern_list_len):
        list_of_group = zip(*(iter(init_list),) * childern_list_len)
        end_list = [list(i) for i in list_of_group]
        count = len(init_list) % childern_list_len
        end_list.append(init_list[-count:]) if count != 0 else end_list
        return end_list

    def Refine_data(self,streamname,records):
        totalPutRecordsResultEntryList={}
        totalPutRecordsResultEntryList['failed_record_count'] = 0
        totalPutRecordsResultEntryList['records']=[]
        rangeRecords = records
        putRecordsResultEntryList = None
        retryIndex = None
        retryPutRecordsRequest = rangeRecords
        retryCount = -1
        currentFailed = 0
        wait = 0.05
        while (retryIndex is None or len(retryIndex) > 0) and retryCount < RECORDS_RETRIES:
            if retryCount != -1:
                time.sleep(wait)
                wait = wait * 2
            r = self.sendRecords(streamname, retryPutRecordsRequest)
            currentFailed = r.failedRecordCount

            # print("%s: send %s records,failed %s records,retryCount %s"%(streamname,len(retryPutRecordsRequest),currentFailed,retryCount + 1))

            if putRecordsResultEntryList is None and currentFailed == 0 or RECORDS_RETRIES == 0:
                retryIndex = [-1 for temp in range(currentFailed)]
                putRecordsResultEntryList = r.recordResult
                break

            if putRecordsResultEntryList is None:
                putRecordsResultEntryList = [None for temp in range(len(rangeRecords))]

            retryIndexTemp = []

            if currentFailed > 0:
                retryPutRecordsRequest = []

            for j in range(0, len(r.recordResult)):
                if retryIndex:
                    originalIndex = retryIndex[j]
                else:
                    originalIndex = j
                putRecordsResultEntry = r.recordResult[j]

                error_code=putRecordsResultEntry.get('error_code')
                if error_code and (error_code == 'DIS.4303' or error_code == 'DIS.5250'):
                    retryIndexTemp.append(originalIndex)
                    retryPutRecordsRequest.append(rangeRecords[originalIndex])

                putRecordsResultEntryList[originalIndex] = putRecordsResultEntry

            if len(retryIndexTemp) > 0:
                retryIndex = retryIndexTemp
            else:
                retryIndex = []

            retryCount += 1

        totalPutRecordsResultEntryList["failed_record_count"] += len(retryIndex)
        totalPutRecordsResultEntryList["records"].extend(putRecordsResultEntryList)

        Faile_count = int(totalPutRecordsResultEntryList.get('failed_record_count'))
        # print('{}:send {} records,failed {} records'.format(streamname,len(records),Faile_count))
        return totalPutRecordsResultEntryList


    def putRecords(self,streamname,records=''):
        self.__assert_to_validate(streamname, "the streamname is null")
        if not stream_mes.get(streamname):
            try:
                r = self.describeStream(streamname)
                if r.statusCode == 200:
                    stream_type = r.streamType
                    partitions = len([i for i in r.partitions if i.get('status') == 'ACTIVE'])
                    stream_mes[streamname] = {"stream_type": stream_type, "partitions": partitions}
            except Exception as ex:
                print(str(ex))

        partitioncount = stream_mes.get(streamname).get('partitions')
        if stream_mes.get(streamname).get("stream_type") == 'COMMON':
            end_list = self.list_of_groups(records, partitioncount*1000)
        else:
            end_list = self.list_of_groups(records, partitioncount*2000)

        totalPutRecordsResultEntryList = {}
        totalPutRecordsResultEntryList['failed_record_count'] = 0
        totalPutRecordsResultEntryList['records'] = []
        limitBytes = 4 * 1024 * 1024
        new_records = []
        for i in range(0, len(end_list)):
            while end_list[i]:
                b = []
                curBytes = 0
                for k in end_list[i]:
                    b.append(k)
                    itemLen = len(str(k))
                    curBytes += itemLen
                    if curBytes <= limitBytes :
                        continue
                    else:
                        if len(b) > 1:
                            b.pop()
                            curBytes -= itemLen
                        break
                new_records.append(b)
                end_list[i] = end_list[i][len(b):]
        for j in range(0, len(new_records)):
            rangeRecords = new_records[j]
            r = self.Refine_data(streamname, rangeRecords)
            totalPutRecordsResultEntryList['failed_record_count'] += r['failed_record_count']
            totalPutRecordsResultEntryList['records'].extend(r['records'])
        return disrecordresponse.disPutRecordsResponse(200, totalPutRecordsResultEntryList)



    def getCursor(self, streamName, partitionId, cursorType, startSeq, ak="", sk="", xSecrityToken=""):
        '''
        the cursor is the pointer to get the data in partition.
        
        :type streamName string
        :param streamName the streamName ID which want to send data    
        
        :type partitionId string
        :param partitionId the partition ID which want to get data, you can get all of the partition info from describeStream interface              
        
        :type cursorType string
        :param cursorType. there are four type for the cursor
            :AT_SEQUENCE_NUMBER  The consumer application starts reading from the position denoted by a specific sequence number. This is the default Cursor Type.
            :AFTER_SEQUENCE_NUMBER The consumer application starts reading right after the position denoted by a specific sequence number.
            :TRIM_HORIZON  The consumer application starts reading at the last untrimmed record in the partition in the system, which is the oldest data record in the partition.
            :LATEST  Start reading just after the most recent record in the partition, so that you always read the most recent data in the partition
        
        
        :type startSeq tring
        :param startSeq 
           Sequence number of the data record in the partition from which to start reading.
           Value range: 0 to 9223372036854775807
           Each data record has a sequence number that is unique within its partition. The sequence number is assigned by DIS when a data producer calls PutRecords to add data to a DIS stream.
           Sequence numbers for the same partition key generally increase over time; the longer the time period between write requests (PutRecords requests), the larger the sequence numbers become.
        '''
        param = {}

        self.__assert_to_validate(streamName, "the streamname is null")
        
        param["stream-name"] = streamName
        param["partition-id"] = partitionId

        if(cursorType != "AT_SEQUENCE_NUMBER" and cursorType != "AFTER_SEQUENCE_NUMBER" and cursorType != "TRIM_HORIZON" and cursorType != "LATEST"):
            raise DisException("Invalid param", "the cursor type is invalid")
        param["cursor-type"] = cursorType

        if (cursorType == "AT_SEQUENCE_NUMBER" or cursorType == "AFTER_SEQUENCE_NUMBER"):
            param["starting-sequence-number"] = startSeq

        uri = "/v2/"+ self.projectid + "/cursors/"

        req = self._generateRequest("GET", uri, query=param, headers={}, body="",userak=ak, usersk=sk, userxSecrityToken=xSecrityToken)

        (statusCode, responseData) = self._sendRequest(req)
        return disrecordresponse.disGetCursorResponse(statusCode, responseData)

    def getRecords(self, partitioncursor, limit = 1000, ak="", sk="", xSecrityToken=""):
        '''
        :type partitioncursor string
        :param partitioncursor: you can get the cursor from getCursor interface
            Cursor, which specifies the position in the partition from which to start reading data records sequentially.
            Value: 1 to 512 characters
            
        :type limit int
        :param limit :The maximum number of records to return.
            Value range: 1 to 10000
            Default value: 1000
        '''

        if self.bodySerializeType:
            from src.com.dis.models import send_records_pb2
            p = send_records_pb2.GetRecordsRequest()
            p.shardIterator = partitioncursor
            p.limit = 100
            params = p.SerializeToString()
            uri = "/v2/" + self.projectid + "/records/"
            req = self._generateRequest("GET", uri, query=params, headers={}, body="", userak=ak, usersk=sk,
                                        userxSecrityToken=xSecrityToken)
            if req.query:
                from src.com.dis.models import send_records_pb2
                target = send_records_pb2.GetRecordsRequest()
                target.ParseFromString(req.query)
                d = {}
                d['partition-cursor'] = target.shardIterator
                d['limit'] = target.limit
                req.query = d


        else:
            params = {"partition-cursor": partitioncursor}

            if limit is not 0:
                params["limit"] = str(limit)

            uri = "/v2/"+ self.projectid + "/records/"
            req = self._generateRequest("GET", uri, query=params, headers={}, body="",userak=ak, usersk=sk, userxSecrityToken=xSecrityToken)

        (statusCode, responseData) = self._sendRequest(req)
        return disrecordresponse.disGetRecordsResponse(statusCode, responseData)


    def createApp(self, appName, ak="", sk="", xSecrityToken=""):


        self.__assert_to_validate(appName, "the appname is null")

        jsonBody = {"app_name": appName}
        jsonString = json.dumps(jsonBody)

        uri = "/v2/"+ self.projectid + "/apps/"

        req = self._generateRequest("POST", uri, headers={}, query={}, body=jsonString, userak=ak, usersk=sk, userxSecrityToken=xSecrityToken)

        (statusCode, responseData) = self._sendRequest(req)
        return discheckpointresponse.disCreateAppResponse(statusCode, responseData)

    def describeApp(self, appName, ak="", sk="", xSecrityToken=""):


        mark = "[`~!@#$%^&*()_\\-+=|{}':;',\\[\\].<>/?~！@#￥%……&*（）——+|{}【】‘；;:/?.,：”“’。，、？ ]"
        for i in mark:
            if appName.startswith(i) or appName.endswith(i):
                print("--the appname is Invalid--")
                sys.exit()


        self.__assert_to_validate(appName, "the appname is null")

        uri = "/v2/" + self.projectid + "/apps/"+appName
        req = self._generateRequest("GET", uri, headers={}, query={}, userak=ak, usersk=sk,
                                    userxSecrityToken=xSecrityToken)
        (statusCode, responseData) = self._sendRequest(req)
        return discheckpointresponse.disdescribeAppResponse(statusCode, responseData)


    def Applist(self, appName, ak="", sk="", xSecrityToken="",limit=100):

        param = {}

        uri = "/v2/" + self.projectid + "/apps"
        req = self._generateRequest("GET", uri, headers={}, query={'limit':'100'}, userak=ak, usersk=sk,
                                    userxSecrityToken=xSecrityToken)
        (statusCode, responseData) = self._sendRequest(req)
        if responseData.get('apps'):
            total_number = len(responseData.get('apps'))
        else:
            total_number = ''

        if limit > 100:
            raise DisException("invalidparam", "the limit is to large")

        if limit != 0:
            param["limit"] = str(limit)

        if appName is not '':
            self.__assert_to_validate(appName, "the appname is null")
            param["start_app_name"] = appName

        uri = "/v2/" + self.projectid + "/apps"

        req = self._generateRequest("GET", uri, headers={}, query=param, userak=ak, usersk=sk,
                                    userxSecrityToken=xSecrityToken)

        (statusCode, responseData) = self._sendRequest(req)
        return discheckpointresponse.disApplistResponse(statusCode, responseData,total_number)



    def deleteApp(self, appName, ak="", sk="", xSecrityToken=""):


        self.__assert_to_validate(appName, "the appname is null")

      
        uri = "/v2/"+ self.projectid + "/apps/" + appName + "/"

        req = self._generateRequest("DELETE", uri, headers={}, query={}, body="",userak=ak, usersk=sk, userxSecrityToken=xSecrityToken)

        (statusCode, responseData) = self._sendRequest(req)
        return discheckpointresponse.disDeleteAppResponse(statusCode, responseData)

    def commitCheckpoint(self, streamName, appName, partitionId, seqNumber, metaData = "", checkpointType = "LAST_READ", ak="", sk="", xSecrityToken=""):

        self.__assert_to_validate(streamName, "the streamname is null")
      
        uri = "/v2/"+ self.projectid + "/checkpoints/"

        jsonData = {"stream_name": streamName,
                    "app_name": appName,
                    "partition_id": partitionId,
                    "sequence_number": seqNumber,
                    "metadata": metaData,
                    "checkpoint_type":checkpointType}

        jsonStrig = json.dumps(jsonData)

        req = self._generateRequest("POST", uri, headers={}, query={}, body=jsonStrig, userak=ak, usersk=sk, userxSecrityToken=xSecrityToken)

        (statusCode, responseData) = self._sendRequest(req)
        return discheckpointresponse.disCommitCheckpointResponse(statusCode, responseData)


    def getCheckpoint(self, streamName, appName, partitionId,  checkpointType = "LAST_READ", ak="", sk="", xSecrityToken=""):

        self.__assert_to_validate(streamName, "the streamname is null")
      
        uri = "/v2/"+ self.projectid + "/checkpoints/"

        param = {"stream_name": streamName,
                 "app_name": appName,
                 "partition_id": partitionId,
                 "checkpoint_type":checkpointType}


        req = self._generateRequest("GET", uri, query=param, headers={}, body="",userak=ak, usersk=sk, userxSecrityToken=xSecrityToken)

        (statusCode, responseData) = self._sendRequest(req)
        return discheckpointresponse.disGetCheckpointResponse(statusCode, responseData)


    def changepartitionCount(self, stream_name_test, target_partition_count, ak="", sk="",xSecrityToken="",mes=''):

        self.__assert_to_validate(stream_name_test, "the streamname is null")

        uri = "/v2/" + self.projectid + "/streams/"+stream_name_test
        jsonBody = {
            "stream_name": stream_name_test,
            "target_partition_count": target_partition_count
        }
        jsonString = json.dumps(jsonBody)

        req = self._generateRequest("PUT", uri,headers={}, body=jsonString, userak=ak, usersk=sk,
                                    userxSecrityToken=xSecrityToken)

        (statusCode, responseData) = self._sendRequest(req)
        return discheckpointresponse.disGetCheckpointResponse(statusCode, responseData)