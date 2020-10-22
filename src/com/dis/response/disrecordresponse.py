#!/usr/bin/python
# -*- coding:utf-8 -*-

import base64,sys,json
from src.com.dis.response.disresponse import DisResponse


class disPutRecordsResponse(DisResponse):
    
    def __init__(self, statusCode, body):
        DisResponse.__init__(self, statusCode, body)
        if type(body) != dict:
            from src.com.dis.models import send_records_pb2
            target = send_records_pb2.PutRecordsResult()
            target.ParseFromString(body)
            records=[]
            self.body = {}
            records.extend([{'partition_id': target.records[i].shardId,
                             'sequence_number':target.records[i].sequenceNumber,
                             'error_message':target.records[i].errorMessage,
                             'error_code': target.records[i].errorCode} for i in
                            range(len(target.records))])

            for i in records:
                if 'DIS' not in i['error_code']:
                    del i['error_code']
                    del i['error_message']

            self.body['records'] = records
            self.body['failed_record_count']=target.failedRecordCount
        else:
            self.body = body

        self.failedRecordCount = self.body["failed_record_count"]
        self.recordResult = self.body["records"]


    def _printResponse(self):
        print ("PutRecordsResponse")
        print ("failed_record_count: %d" %(int(self.failedRecordCount)))
        print ("recordResult %s: " %(self.recordResult))


    def getSendFailuerRecord(self, originRecords):
        failRecord = []

        if self.failedRecordCount == 0:
            return failRecord

        for i in range(len(self.recordResult)):
            if "sequence_number" in self.recordResult[i].keys():
                pass

            else :
                failRecord.append(originRecords[i])

        return failRecord

    def getSendRecordResult(self,originRecords):
        r=ListObj(originRecords)
        return r


class ListObj(object):
    def __init__(self, a):
        self.count = -1
        self.a = a

    def __iter__(self):
        return self

    def next(self):
        self.count += 1
        if self.count >= len(self.a):
            raise StopIteration()
        return self

    def __next__(self):
        self.count += 1
        if self.count >= len(self.a):
            raise StopIteration()
        return self

    def setValue(self, name, value):
        setattr(self, name, value)

    def __getattr__(self, key):
        return self.a[self.count].get(key,'')



class disGetCursorResponse(DisResponse):
    
    def __init__(self, statusCode, body):
        DisResponse.__init__(self, statusCode, body)
        self.cursor = body["partition_cursor"]
        
    def _printResponse(self):
        print ("GetCursorResponse")
        print ("cursor: %s" %(self.cursor))
        
        
class disGetRecordsResponse(DisResponse):
    
    def __init__(self, statusCode, body):
        DisResponse.__init__(self, statusCode, body)
        if type(body)==dict:
            if sys.version_info.major == 2 or sys.version_info.major < 3:
                self.body={}
                self.body['next_partition_cursor']=body["next_partition_cursor"]
                for i in range(len(body["records"])):
                    tempData = body["records"][i]["data"].encode('utf-8')
                    body["records"][i]["data"] = base64.b64decode(tempData)
                self.body['records'] = [{'data': i.get('data'), 'sequence_number': i.get('sequence_number')} for i in body["records"]]

            else:
                for i in range(len(body["records"])):
                    tempData = body["records"][i]["data"].encode('utf-8')
                    try:
                        body["records"][i]["data"] = base64.b64decode(tempData).decode('utf-8').replace('\ufeff','')
                    except:pass
                self.body['records'] = [{'data': i.get('data'), 'sequence_number': i.get('sequence_number')} for i in
                                        body["records"]]

        else:
            from src.com.dis.models import send_records_pb2
            target=send_records_pb2.GetRecordsResult()
            target.ParseFromString(body)
            self.body={}
            self.body['next_partition_cursor']=target.nextShardIterator
            import re
            p1 = re.compile(r'data:.*\n')
            p = re.compile(r'sequenceNumber:.*\n')
            datas = []
            for i in p1.findall(str(target.records)):
                datas.append((i.replace('\\','').replace('"','').strip()))
            sequenceNumber = []
            for i in p.findall(str(target.records)):
                sequenceNumber.append((i.replace('\\', '').replace('"', '').strip()))
            ddd=list(zip(datas,sequenceNumber))
            records = [{'sequence_number': i[1].split('sequenceNumber:')[-1].strip(), 'data': i[0].split('data:',1)[-1].strip()} for i in ddd]
            self.body['records']=records


        self.nextPartitionCursor = self.body["next_partition_cursor"]
        self.recordResult = self.body["records"]
        self.recordCount = len(self.body["records"])

    def _printResponse(self):
        print ("GetRecordsResponse")
        print ("next_partition_cursor: %s" %(self.nextPartitionCursor))
        print ("recordCount %d" %(self.recordCount))
        print ("recordResult %s: " %(self.recordResult))
        
    def getRecordResult(self,originRecords):
        r=ListObj(originRecords)
        return r

    def getRecordResult(self, originRecords):
        r = ListObj(originRecords)
        return r
