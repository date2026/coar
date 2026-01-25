import os
import csv
import sys
import time
import subprocess
import signal
from redis import Redis
import json
import psutil
DEV="ens33"
TOTAL_BD = 1000
MIN_REPAIR_BD = 0.3*TOTAL_BD
T = 5
output_file = "[your path]/build/bd-output.csv"
IBM1 = "java"
IBM2 = "ECAgent"
LASTLINEID = -1
alpha = 1




# read bandwidth from file, parse, update upload and download bandwidth
def RefreshBandWidth():
    with open("[your path]/conf/1.json") as f:
        conf = json.load(f)
    coor_ip = conf["coor_ip"]
    local_ip = conf["local_ip"]
    
    coor_connect = Redis(host=coor_ip, port=6379, db=0)

    upload_speeds = []
    download_speeds =  []

    for i in range(T):
        io_start = psutil.net_io_counters(pernic=True).get(DEV)
        assert io_start
        bytes_sent_start = io_start.bytes_sent
        bytes_recv_start = io_start.bytes_recv
        time.sleep(1)
        io_end = psutil.net_io_counters(pernic=True).get(DEV)
        assert io_end
        bytes_sent_end = io_end.bytes_sent
        bytes_recv_end = io_end.bytes_recv
        upload_speed = (float)(bytes_sent_end - bytes_sent_start) / 1.0 / 1024.0 / 1024.0
        download_speed = (float)(bytes_recv_end - bytes_recv_start) / 1.0 / 1024.0 / 1024.0
        upload_speeds.append(upload_speed)
        download_speeds.append(download_speed)
    assert len(upload_speeds) == len(download_speeds) and len(upload_speeds) == T
    
    fore_ground_recv = 0.0
    fore_ground_send = 0.0
    for i in range(len(upload_speeds)):
        fore_ground_recv += download_speeds[i]
        fore_ground_send += upload_speeds[i]
    fore_ground_recv /= float(T) 
    fore_ground_send /= float(T)
    print("ave_recv = " '%.3f' % fore_ground_recv)
    print("ave_send = " '%.3f' % fore_ground_send)
    repair_bd_recv = int(max(MIN_REPAIR_BD, alpha * TOTAL_BD - fore_ground_recv))
    repair_bd_send = int(max(MIN_REPAIR_BD, alpha * TOTAL_BD - fore_ground_send))
    coor_connect.set('recovery_up_' + local_ip, '%d'%repair_bd_send)
    coor_connect.set('recovery_dw_' + local_ip, '%d'%repair_bd_recv)
    print(f"repair_bd_recv: {repair_bd_recv} \n repair_bd_send: {repair_bd_send}")


         
