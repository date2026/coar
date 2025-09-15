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
output_file = "/root/coar/build/bd-output.csv"
IBM1 = "java"
IBM2 = "ECAgent"
LASTLINEID = -1
alpha = 1


# get bandwidth from redis
# def GetBandWidth(recv, send, T, coor_connect, local_ip):
#     fore_ground_recv = 0
#     fore_ground_send = 0
#     assert len(recv) == len(send)
#     if len(recv) != T:
#         print("recv and send length not enough")
#         return
#     for i in range(len(recv)):
#         fore_ground_recv += float(recv[i])
#         fore_ground_send += float(send[i])
#         print('%.3f' % recv[i], '%.3f' % send[i])

#     fore_ground_recv /= float(T) * 128.0 # KByte/s --> Mbit/s
#     fore_ground_send /= float(T) * 128.0
#     print("ave_recv = " '%.3f' % fore_ground_recv)
#     print("ave_send = " '%.3f' % fore_ground_send)
#     repair_bd_recv = int(max(MIN_REPAIR_BD, alpha * TOTAL_BD - fore_ground_recv))
#     repair_bd_send = int(max(MIN_REPAIR_BD, alpha * TOTAL_BD - fore_ground_send))
#     coor_connect.set('recovery_up_' + local_ip, '%d'%repair_bd_send)
#     coor_connect.set('recovery_dw_' + local_ip, '%d'%repair_bd_recv)



# # read bandwidth from file, parse, update upload and download bandwidth
# def RefreshBandWidth(file, T, coor_connect, local_ip):
#     global LASTLINEID
#     recv = []
#     send = []
#     data = []

#     with open(output_file) as f:
#         reader = csv.reader(f)
#         all_rows = list(reader)
#         all_len = len(all_rows)
#         id = 0
#         print(f"read from file line: {LASTLINEID+1} to {all_len}")
#         for i in range(LASTLINEID+1, all_len):
#             if all_rows[i] == []:
#                 continue
#             if str(all_rows[i]) == "['Refreshing:']": # get a time step
#                 print(f"time step: {id}")
#                 id += 1
#                 if id > T:
#                     LASTLINEID = i-1
#                     break
#                 recv.append(0)
#                 send.append(0)
#                 continue
#             if id > 0 and (str(all_rows[i]).find(IBM1) != -1): # get a java output
#                 s = str(all_rows[i]).split("\\t")[1]
#                 r = str(all_rows[i]).split("\\t")[2][:-2]
#                 recv[id-1] += float(r)
#                 send[id-1] += float(s)
#                 data.append(all_rows[i])
#                 print(f'get a java output, recv: {r}, send: {s}')

            
#             if id > 0 and (str(all_rows[i]).find(IBM2) != -1): # get a ECAgent output
#                 s = str(all_rows[i]).split("\\t")[1]
#                 r = str(all_rows[i]).split("\\t")[2][:-2]
#                 recv[id-1] += float(r)
#                 send[id-1] += float(s)
#                 data.append(all_rows[i])
#                 print(f'get a ECAgent output, recv: {r}, send: {s}')
    
#     GetBandWidth(recv, send, T, coor_connect, local_ip)


# start a subprocess to get bandwidth
# update subprocess bandwidth
# def UpdateBandWidth():
#     # get conf
#     with open("/home/openec/coar/conf/1.json") as f:
#         conf = json.load(f)
#     coor_ip = conf["coor_ip"]
#     local_ip = conf["local_ip"]
    
#     # init available bandwidth for recovery
#     coor_connect = Redis(host=coor_ip, port=6379, db=0)
#     coor_connect.set('recovery_up_' + local_ip, '0')
#     coor_connect.set('recovery_dw_' + local_ip, '0')

#     cmd = "sudo nethogs " + DEV + " -t > " + output_file
#     # subprocess.Popen(['/bin/bash', '-c', cmd])
#     print(f"start {cmd}")
#     proc = subprocess.Popen(
#         ['/bin/bash', '-c', cmd],
#         preexec_fn=os.setsid  
#     )

#     try:
#         time.sleep(1)
#         while True:
#             time.sleep(int(T))
#             RefreshBandWidth(output_file, int(T), coor_connect, local_ip)
#     except:
#         print("error")
#         os.killpg(os.getpgid(proc.pid), signal.SIGTERM)



# read bandwidth from file, parse, update upload and download bandwidth
def RefreshBandWidth():
    with open("/root/coar/conf/1.json") as f:
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


         
