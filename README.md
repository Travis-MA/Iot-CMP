# Iot-CMP
小微工业企业云端智能解决方案-联接管理平台CMP

![avatar](doc/CMP.png)

V1.0 支持Modbus协议设备接入 2020-10-23
V1.1 蒸压釜设备应用开发完成 2020-10-31

运行说明：
入口：server.py

服务器运行：
1, nohup : nohup python3 server.py &

2, crontab: crontab -e */9 * * * * /home/Iot-CMP/autoRestart.sh

