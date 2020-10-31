import json
from datetime import timedelta
from datetime import datetime
from analysis import AutoClaveStateDetect

clave_num = 7
hour_offset = 7
confPath = 'conf.ini'
bucket_name = 'obs-ydy1'
folder_path = 'Service/ZyRecord/'
tresh = 0.01
padding = 120


class AutoClaveDataAnalysis:
    def __init__(self, obs_client, now):
        self.obs_client = obs_client
        self.now = now
        pass

    def __list_object(self, prefix):
        # 调用listObjects接口列举指定桶内的所有对象
        resp = self.obs_client.listObjects(bucket_name, prefix=prefix)
        if resp.status < 300:
            index = 0
            # 遍历输出所有对象信息
            obj_list = []
            for content in resp.body.contents:
                obj_list.append(content)
                index += 1
            return obj_list
        else:
            # 输出错误码
            print('OBS listobj:errorCode:', resp.errorCode)
            # 输出错误信息
            print('OBS listobj:errorMessage:', resp.errorMessage)

    def __refresh_analsis(self, clave_id):
        offset_time = self.now - timedelta(hours=hour_offset)  # 减去7个小时的时间（今天上午七点前是昨天）
        today_initial = datetime(year=offset_time.year, month=offset_time.month, day=offset_time.day, hour=hour_offset,
                                 minute=0,
                                 second=0)
        today_folder_path = folder_path + offset_time.date().isoformat() + '/'
        today_record_list = self.__list_object(today_folder_path)

        if len(today_record_list) > 0:  # 有今天的
            time_ing_max = int(today_initial.timestamp())
            prefix = 'none'
            for content in today_record_list:
                X_index = content.key.find("X")
                #找出最后一个ING
                if content.key[X_index - 1:X_index] == str(clave_id):
                    if content.key[X_index + 1:X_index + 4] == 'ING':
                        time_ing = int(content.Key[X_index + 4:X_index + 14])
                        if time_ing > time_ing_max:
                            time_ing_max = time_ing
                            prefix = content.key


            if len(prefix) > 10:
                resp1 = self.obs_client.getObject(bucket_name, prefix, downloadPath=None)
                resp1_ok = 0
                records = ''
                if resp1.status < 300:
                    response = resp1.body.response
                    chunk_size = 65536
                    if response is not None:
                        while True:
                            chunk = response.read(chunk_size)
                            if not chunk:
                                break
                            records = records + bytes.decode(chunk)
                            resp1_ok = 1
                        response.close()
                else:
                    print("[AutoClaveDataAnalysis] getObject error resp1")
                load_dict1 = json.loads(records)

                prefix_analysis = today_folder_path + str(clave_id) + 'R' + str(time_ing_max)
                resp2 = self.obs_client.getObject(bucket_name, prefix_analysis, downloadPath=None)
                resp2_ok = 0
                records_analysis = ''
                if resp2.status < 300:
                    response = resp2.body.response
                    chunk_size = 65536
                    if response is not None:
                        while True:
                            chunk = response.read(chunk_size)
                            if not chunk:
                                break
                            records_analysis = records_analysis + bytes.decode(chunk)
                            resp2_ok = 1
                        response.close()
                else:
                    print("[AutoClaveDataAnalysis] getObject error resp2")

                #print('records_analysis:   '+records_analysis)
                # load_dict2 = json.loads(records_analysis)
                load_dict2 = {}
                with open("doc/rec_templete.json", 'rb') as load_f:
                    load_dict2 = json.load(load_f)

                if resp1_ok and resp2_ok:
                    load_dict3 = AutoClaveStateDetect.state_detect(load_dict1, load_dict2)
                    resp = self.obs_client.deleteObject(bucket_name, prefix_analysis)
                    if resp.status < 300:
                        resp = self.obs_client.putContent(bucket_name, prefix_analysis, str(json.dumps(load_dict3, ensure_ascii=False)))
                        if resp.status > 300:
                            print('[AutoClaveDataAnalysis] OBS putContent:errorCode:', resp.errorCode)
                            print('[AutoClaveDataAnalysis] OBS putContent:errorMessage:', resp.errorMessage)
                    else:
                        print('[AutoClaveDataAnalysis] OBS deleteObject:errorCode:', resp.errorCode)
                        print('[AutoClaveDataAnalysis] OBS deleteObject:errorMessage:', resp.errorMessage)
                else:
                    print('[AutoClaveDataAnalysis] resp1_ok:'+str(resp1_ok)+',or resp2_ok:' +str(resp2_ok)+ ' is false, empty record')
            else:
                print("[AutoClaveDataAnalysis], no today ing event, ClaveId="+str(clave_id))
        # print('hasToday')
        else:
            print('[AutoClaveDataAnalysis],  No Today Record')
            return 0


    def data_analysis_process(self):
        for clave_id in range(1, clave_num + 1):
            resp = self.__refresh_analsis(clave_id)