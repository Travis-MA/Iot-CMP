import json
from configparser import ConfigParser
from datetime import datetime
import numpy as np

confPath = 'conf.ini'
bucket_name = 'obs-ydy1'


def auto_clave_data_convert(data_json, obs_client):
    conf = ConfigParser()
    conf.read(confPath)
    clave_num = 7
    np_array = []

    for clave_id in range(1, clave_num + 1):

        dev_id = conf.get('AutoClave' + str(clave_id), 'devId')

        in_temp_channel = conf.get('AutoClave' + str(clave_id), 'inTempChannel')
        in_temp_slope = conf.get('AutoClave' + str(clave_id), 'inTempSlope')
        in_temp_shift = conf.get('AutoClave' + str(clave_id), 'inTempShift')

        out_temp_channel = conf.get('AutoClave' + str(clave_id), 'outTempChannel')
        out_temp_slope = conf.get('AutoClave' + str(clave_id), 'outTempSlope')
        out_temp_shift = conf.get('AutoClave' + str(clave_id), 'outTempShift')

        in_press_channel = conf.get('AutoClave' + str(clave_id), 'inPressChannel')
        in_press_slope = conf.get('AutoClave' + str(clave_id), 'inPressSlope')
        in_press_shift = conf.get('AutoClave' + str(clave_id), 'inPressShift')

        state_channel = conf.get('AutoClave' + str(clave_id), 'stateChannel')

        time_list = []
        in_temp_list = []
        out_temp_list = []
        in_press_list = []
        state_list = []

        record_dict = []

        for devRec in data_json:
            data = json.loads(devRec['data'])
            if data['device_id'] == dev_id:
                services = data['services'][0]
                properties = services['properties']
                try:

                    time = datetime.strptime(services['event_time'], '%Y%m%dT%H%M%SZ').timestamp() + 3600 * 8
                    in_temp = float(properties[in_temp_channel]) * float(in_temp_slope) + float(in_temp_shift)
                    out_temp = float(properties[out_temp_channel]) * float(out_temp_slope) + float(out_temp_shift)
                    in_press = float(properties[in_press_channel]) * float(in_press_slope) + float(in_press_shift)
                    state = properties[state_channel]

                    time_list.append(time)
                    in_temp_list.append(in_temp)
                    out_temp_list.append(out_temp)
                    in_press_list.append(in_press)
                    state_list.append(state)

                    rec_dict = {'time': time, 'inTemp': in_temp,
                                'outTemp': out_temp, 'inPress': in_press,
                                'state': state}

                    record_dict.append(rec_dict)

                except:
                    pass

        data_set_np = np.array([time_list, in_temp_list, out_temp_list, in_press_list, state_list])
        np_array.append(data_set_np)

        obs_rec_dict = json.dumps({'claveId': clave_id, 'records': record_dict})
        obs_rec_prefix = 'Service/ZyRealTime/clave' + str(clave_id)
        resp = obs_client.deleteObject(bucket_name, obs_rec_prefix)
        if resp.status >= 300:
            print('OBS DELETE obj: common msg:status:', resp.status, 'prefix ', obs_rec_prefix, ',errorCode:',
                  resp.errorCode, ',errorMessage:', resp.errorMessage)

        resp = obs_client.putContent(bucket_name, obs_rec_prefix, str(obs_rec_dict))
        if resp.status >= 300:
            print('OBS PUT content: common msg:status:', resp.status, 'prefix ', obs_rec_prefix, ',errorCode:',
                  resp.errorCode, ',errorMessage:', resp.errorMessage)

    return np_array


class RealTimeDataPool:

    def __init__(self, obs_client):
        self.obs_client = obs_client
        pass

    def auto_clave_process(self, data_json):
        return auto_clave_data_convert(data_json, self.obs_client)
