
# !/usr/bin/python
# -*- coding:utf-8 -*-
import DataFromDIS
from RealTimeDataPool import RealTimeDataPool
from AutoClaveBreakStorage import AutoClaveBreakStorage
import time
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
    resp = break_storage.break_storage_process()


if para == 1:
    job()
else:
    schedule.every(period).minutes.do(job)
    while True:
        schedule.run_pending()
        time.sleep(1)
