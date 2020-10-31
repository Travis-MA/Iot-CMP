
# !/usr/bin/python
# -*- coding:utf-8 -*-
import DataFromDIS
from RealTimeDataPool import RealTimeDataPool
from AutoClaveBreakStorage import AutoClaveBreakStorage
from AutoClaveDataAnalysis import AutoClaveDataAnalysis
import time
import json
import schedule
from obs import ObsClient
from configparser import ConfigParser

para = 1
period = 5
conf_path = 'conf.ini'


def new_obs_client():
    conf = ConfigParser()
    conf.read(conf_path)
    # Use configuration file
    try:
        ak = conf.get('OBSconfig', 'ak')
        sk = conf.get('OBSconfig', 'sk')
        server = conf.get('OBSconfig', 'server')
        obs_client = ObsClient(access_key_id=ak, secret_access_key=sk, server=server)
        return obs_client

    except Exception as ex:
        print('New OBS client ' + str(ex))
    pass


def job():
    modbus_record = DataFromDIS.get_records()
    obs_client = new_obs_client()
    real_time_data_pool = RealTimeDataPool(obs_client)

    # 蒸压釜过程
    np_data_list = real_time_data_pool.auto_clave_process(modbus_record)
    break_storage = AutoClaveBreakStorage(obs_client, np_data_list)
    resp, now = break_storage.break_storage_process()
    analysis_obj = AutoClaveDataAnalysis(obs_client, now)
    resp = analysis_obj.data_analysis_process()

if para == 1:
    job()
elif para == 2:
    print('nojob')
    obs_client = new_obs_client()
    bucket_name = 'obs-ydy1'
    prefix = 'Service/ZyRecord/2020-10-17/4XFIN1602902895Y1602943935'
    resp = obs_client.getObject(bucket_name, prefix, downloadPath=None)
    print(resp)
    records = ''
    if resp.status < 300:
        response = resp.body.response
        chunk_size = 65536
        if response is not None:
            while True:
                chunk = response.read(chunk_size)
                if not chunk:
                    break

                records = records+bytes.decode(chunk)

            response.close()
    print(json.loads(records))
else:
    schedule.every(period).minutes.do(job)
    while True:
        schedule.run_pending()
        time.sleep(1)
