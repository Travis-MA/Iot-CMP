import json
from datetime import timedelta
from datetime import datetime

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

    def __get_dataset(self, clave_id):
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

                # 保证每条釜都有
                if content.key[X_index - 1:X_index] == str(clave_id):
                    if content.key[X_index + 1:X_index + 4] == 'ING':
                        time_ing = int(content.Key[X_index + 4:X_index + 14])
                        if time_ing > time_ing_max:
                            time_ing_max = time_ing
                            prefix = content.key

                if len(prefix) > 10:
                    resp = self.obs_client.getObject(bucket_name, prefix, downloadPath=None)
                else:
                    print("AutoClaveDataAnalysis, no today ing event")
            # print('hasToday')
        else:

            return 0
