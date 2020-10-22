from datetime import timedelta
from datetime import datetime



clave_num = 7
hour_offset = 7
now = datetime.today()
confPath = 'conf.ini'
bucket_name = 'obs-ydy1'
folder_path = 'Service/ZyRecord/'

class AutoClaveBreakStorage:
    def __init__(self, obs_client, np_data_list):
        self.obs_client = obs_client
        self.np_data_list = np_data_list
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

    # 查找是否有今天的文件夹，如果没有就新建，并且去昨天的找，如果昨天也没有，那么就从7点算起建立新事件
    def __folder_init(self):

        offset_time = now - timedelta(hours=hour_offset)  # 减去7个小时的时间（今天上午七点前是昨天）
        today_initial = datetime(year=offset_time.year, month=offset_time.month, day=offset_time.day, hour=hour_offset,
                                 minute=0,
                                 second=0)
        today_folder_path = folder_path + offset_time.date().isoformat() + '/'
        today_record_list = self.__list_object(today_folder_path)

        latest_event_list = []
        if len(today_record_list) > 0:  # 有今天的

            for clave_id in range(1, clave_num + 1):
                clave_check = 0
                time_ing_max = int(today_initial.timestamp())
                time_fin_max = int(today_initial.timestamp())
                for content in today_record_list:
                    X_index = content.key.find("X")

                    # 保证每条釜都有
                    if content.key[X_index - 1:X_index] == str(clave_id):
                        if content.key[X_index + 1:X_index + 4] == 'ING':
                            clave_check = 1
                            time_ing = int(content.Key[X_index + 4:X_index + 14])
                            if time_ing > time_ing_max:
                                time_ing_max = time_ing

                        elif content.key[X_index + 1:X_index + 4] == 'FIN':
                            Y_index = content.key.find("Y")
                            time_fin = int(content.Key[Y_index + 1:Y_index + 11])
                            if time_fin > time_fin_max:
                                time_fin_max = time_fin

                if clave_check == 0:  # 这个釜目前没有ING，新建一个ING，开始时间是最晚的FIN结束时间
                    latest_event_list.append(time_fin_max)
                    self.__new_record(clave_id, time_fin_max, offset_time)
                else:  # 这个釜有ING，开始时间是ING开始时间
                    latest_event_list.append(time_ing_max)
            # print('hasToday')
        else:  # 没有今天的
            # print('noToday')

            offset_time_yestd = offset_time - timedelta(days=1)
            yestd_folder_path = folder_path + offset_time_yestd.date().isoformat() + '/'
            yestd_record_list = self.__list_object(yestd_folder_path)
            if len(yestd_record_list) > 0:  # 有昨天的
                for clave_id in range(1, clave_num + 1):
                    flag = 0
                    time_ing_max = today_initial.timestamp() - 24 * 3600 + 1
                    max_prefix = ''
                    for content in yestd_record_list:
                        X_index = content.key.find("X")
                        if content.key[X_index - 1:X_index] == str(clave_id):
                            if content.key[X_index + 1:X_index + 4] == 'ING':
                                time_ing = int(content.key[X_index + 4:X_index + 14])
                                if time_ing > time_ing_max:
                                    time_ing_max = time_ing
                                    max_prefix = content.key
                                    flag = 1

                    if flag == 0:
                        self.__new_record(clave_id, today_initial.timestamp(), offset_time)
                        latest_event_list.append(int(today_initial.timestamp()))
                    elif flag == 1:
                        X_index = max_prefix.find("X")
                        new_today_key = today_folder_path + max_prefix[X_index - 1:]
                        resp = self.obs_client.copyObject(bucket_name, max_prefix, bucket_name, new_today_key)
                        if resp.status >= 300:
                            print('OBS copy obj:errorCode:', resp.errorCode)
                            print('OBS copy obj:errorMessage:', resp.errorMessage)

                        latest_event_list.append(time_ing_max)

            else:  # 没有昨天的
                for clave_id in range(1, clave_num + 1):
                    self.__new_record(clave_id, today_initial.timestamp(), offset_time)
                    latest_event_list.append(int(today_initial.timestamp()))

        return latest_event_list

    def __new_record(self, clave_id, today_initial, offset_time):
        today_folder_path = folder_path + offset_time.date().isoformat() + '/'
        event_prefix = today_folder_path + str(clave_id) + 'XING' + str(int(today_initial)) + 'Y'
        resp = self.obs_client.putContent(bucket_name, event_prefix, str(0))  # 新建今日文件夹

        if resp.status >= 300:
            print('OBS putContent:errorCode:', resp.errorCode)
            print('OBS putContent:errorMessage:', resp.errorMessage)

    def break_storage_process(self):
        latest_event_list = self.__folder_init()
