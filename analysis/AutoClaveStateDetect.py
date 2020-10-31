# !/usr/bin/python
# -*- coding:utf-8 -*-
import json


def state_detect(load_dict, load_dict2):
    print(load_dict)
    data = load_dict['data']
    state = data['state']

    load_dict2["devInput"] = "正常"
    load_dict2["sensorInput"] = "正常"

    index_cnt = 0
    state_cnt = -1
    state_name_list = ["开门", "关门", "预养进气开", "邻釜导进开", "邻釜导进停", "隔釜导进开", "升压开", "隔釜导进停", "隔釜导出开", "预养出气开", "邻釜导出开", "排空开"]
    pre_state = {
        "state_id" : -1,
        "endInPress" : data['pressure'][0]["v"],
        "endInTemp" : data['tempIn'][0]["v"],
        "endOutTemp" : data['tempOut'][0]["v"]
    }
    for rec in state:
        state_id = int(rec['v'] / 4095 * 12)
        refresh_pre = 0
        if (pre_state['state_id'] != state_id):
            refresh_pre = 1
            pre_state['state_id'] = state_id

            new_rec_dict = {
                "name": state_name_list[state_id],
                "startIndex": index_cnt,
                "endIndex": 0,
                "startTime": rec["t"],
                "endTime": 0,
                "deltaInPress": 0,
                "deltaInTemp": 0,
                "deltaOutTemp": 0
            }
            load_dict2["stateInfo"]["state"].append(new_rec_dict)
            load_dict2["stateInfo"]["state"][state_cnt]["endIndex"] = index_cnt
            load_dict2["stateInfo"]["state"][state_cnt]["endTime"] = rec["t"]

        if state_cnt == 11:
            load_dict2["stateInfo"]["state"][state_cnt]["endIndex"] = index_cnt
            load_dict2["stateInfo"]["state"][state_cnt]["endTime"] = rec["t"]
        #print("state_cnt:" + str(state_cnt) + "  now Pressure:" + str(round(data['pressure'][index_cnt]["v"], 3)) + " pre Pressure:" + str(round(pre_state['endInPress'], 3)))


        load_dict2["stateInfo"]["state"][state_cnt]["deltaInPress"] = round(data['pressure'][index_cnt]["v"]-pre_state['endInPress'],3)
        load_dict2["stateInfo"]["state"][state_cnt]["deltaInTemp"] = round(data['tempIn'][index_cnt]["v"] - pre_state['endInTemp'],3)
        load_dict2["stateInfo"]["state"][state_cnt]["deltaOutTemp"] = round(data['tempOut'][index_cnt]["v"] - pre_state['endOutTemp'],3)


        if refresh_pre == 1:
            pre_state['endInPress'] = data['pressure'][index_cnt]["v"]
            pre_state['endInTemp'] = data['tempIn'][index_cnt]["v"]
            pre_state['endOutTemp'] = data['tempOut'][index_cnt]["v"]
            state_cnt = state_cnt + 1


        index_cnt = index_cnt + 1
        if (index_cnt > len(state)):
            break

    return load_dict2
