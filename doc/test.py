import json

load_dict = {}
with open("4XFIN1602902895Y1602943935.json",'r') as load_f:
    load_dict = json.load(load_f)

print(load_dict)
data = load_dict['data']
state = data['state']
"""
t_min = state[0]['t']
t_max = 0

for rec in state:
    t = rec['t']
    t_max = t

len = len(state)
add = int(4095/12)
t_add = int((t_max-t_min)/13)

for rec in state:
    t = rec['t']
    a = int((t-t_min)/t_add)
    if a > 12:
        a = 12
    state_val = a*add
    rec['v'] = state_val

print(state)
with open("4XFIN1602902895Y1602943935.json","w") as f:
    json.dump(load_dict,f)
    print("加载入文件完成...")
"""
load_dict2 = {}
with open("4R1602902895.json",'r') as load_f:
    load_dict2 = json.load(load_f)

load_dict2["startTime"] = 1602902895
load_dict2["endTime"] = 1602943935
load_dict2["sampNum"] = len(state)
pre_state = -1
info = load_dict2["stateInfo"]
cnt = 0
for rec in state:
    state_id = int(rec['v']/4090*12)
    if(pre_state != state_id):
        pre_state = state_id
        t = rec["t"]
        sinf = info["state"+str(state_id)]
        sinf["startTime"] = t
        sinf["startIndex"] = cnt
        if state_id > 0:
            sinf = info["state" + str(state_id-1)]
            sinf["endTime"] = t
            sinf["endIndex"] = cnt-1
        print(t)
    cnt = cnt + 1
    if(cnt > len(state)):
        break
with open("4R1602902895.json","w") as f:
    json.dump(load_dict2,f)
    print("加载入文件完成...")
