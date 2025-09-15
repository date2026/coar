import os
import csv
import sys
import time
import subprocess
import signal
from redis import Redis
import json
import psutil
import threading
DEV="eth0"
TOTAL_BD = 1875
MIN_REPAIR_BD = 0.3*TOTAL_BD
T = 1
alpha = 1

# read bandwidth from file, parse, update upload and download bandwidth
def RefreshBandWidth():
    with open("/root/coar/conf/1.json") as f:
        conf = json.load(f)
    local_ip = conf["local_ip"]
    
    redis_connect = Redis(host=local_ip, port=6379, db=0)
    while True:
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
        redis_connect.set('recovery_up_' + local_ip, '%d'%repair_bd_send)
        redis_connect.set('recovery_dw_' + local_ip, '%d'%repair_bd_recv)
        print(f"repair_bd_recv: {repair_bd_recv} \n repair_bd_send: {repair_bd_send}")



def RefreshCPU():
    with open("/root/coar/conf/1.json") as f:
        conf = json.load(f)
    local_ip = conf["local_ip"]
    
    redis_connect = Redis(host=local_ip, port=6379, db=0)

    while True:
        cpu_usages = []

        # for i in range(T):
        #     cpu_usage = psutil.cpu_percent(interval=1)
        #     cpu_usages.append(cpu_usage)
        
        # cpu_usage = 0
        # for i in range(len(cpu_usages)):
        #     cpu_usage += cpu_usages[i]
        # cpu_usage /= int(T)
        cpu_usage = psutil.cpu_percent(interval=0.5)
        
        redis_connect.set('cpu_' + local_ip, '%d'%cpu_usage)
        print(f"cpu_usage: {cpu_usage}")
        
def RefreshLoadAverage():
    with open("/root/coar/conf/1.json") as f:
        conf = json.load(f)
    local_ip = conf["local_ip"]
    
    redis_connect = Redis(host=local_ip, port=6379, db=0)

    while True:
        load_avg = os.getloadavg()
        load_avg = load_avg[0] * 100
        redis_connect.set('load_avg_' + local_ip, '%d'%load_avg)
        time.sleep(0.2)


if __name__ == "__main__":
    print("start")
    refresh_bandwidth_thd = threading.Thread(target=RefreshBandWidth)
    refresh_cpu_thd = threading.Thread(target = RefreshCPU)
    refresh_load_avg_thd = threading.Thread(target = RefreshLoadAverage)
    refresh_bandwidth_thd.start()
    refresh_cpu_thd.start()
    refresh_load_avg_thd.start()
    refresh_bandwidth_thd.join()
    refresh_cpu_thd.join()
    refresh_load_avg_thd.join()