from src.com.dis.client import disclient
from configparser import ConfigParser

confPath = 'conf.ini'
partition_id = 'shardId-0000000000'
startSeq = 0
streamName = 'dis-YDY1'


def get_cursor_test(cli):
    try:
        r = cli.getCursor(streamName=streamName, partitionId=partition_id, cursorType='TRIM_HORIZON', startSeq=startSeq)
        return r.cursor
    except Exception as ex:
        print('DISDataMan __getCursor_test ' + str(ex))


def new_object():
    conf = ConfigParser()
    conf.read(confPath)
    # Use configuration file
    try:
        project_id = conf.get('DISconfig', 'projectid')
        ak = conf.get('DISconfig', 'ak')
        sk = conf.get('DISconfig', 'sk')
        region = conf.get('DISconfig', 'region')
        endpoint = conf.get('DISconfig', 'endpoint')

        try:
            dis = disclient.disclient(endpoint=endpoint, ak=ak, sk=sk, projectid=project_id, region=region)
            return dis

        except Exception as ex:
            print('[ZYFDataFromDIS] (new_object) dislink' + str(ex))

    except Exception as ex:
        print('[ZYFDataFromDIS] (new_object) conf load ' + str(ex))
    pass


def get_records():
    cli = new_object()
    cursor = get_cursor_test(cli)
    records = []
    try:
        while cursor:
            r = cli.getRecords(partitioncursor=cursor)
            cursor = r.nextPartitionCursor
            if not r.recordResult:
                break

            records.extend(r.body["records"])

        return records
    except Exception as ex:
        print('[ZYFDataFromDIS] (get_records_test)' + str(ex))

