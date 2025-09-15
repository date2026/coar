from redis import Redis
import json
import sys 
import logging
import threading
import time
from util import gf_cost_recorder
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(filename)s:%(lineno)d] %(levelname)s  - %(message)s'
)

# collect node stats for generate ecdag 
def CollectStats(cpu_flag, mem_flag, disk_flag, net_flag, gf_bandwidths_flag = False, w = 8, recorders = None):
    with open("/root/coar/conf/1.json") as f:
        conf = json.load(f)
    local_ip = conf["local_ip"]
    agent_ips = conf["agent_ips"]
    agent_num = conf["agent_num"]
    ret = {"cpu": [], "upload_bandwidth": [], "download_bandwidth": [], "mem": [], "disk": [], "load_avg": []}
    for i in range(agent_num):
        agent_ip = agent_ips[i]
        agent_connect = Redis(host=agent_ip, port=6379, db=0)
        if cpu_flag:
            cpu_usage = agent_connect.get('cpu_' + agent_ip)
            ret["cpu"].append(int(cpu_usage))
            load_avg = agent_connect.get('load_avg_' + agent_ip)
            ret["load_avg"].append(int(load_avg) / 100.0)
        if net_flag:
            upload_bandwidth = agent_connect.get('recovery_up_' + agent_ip)
            download_bandwidth = agent_connect.get('recovery_dw_' + agent_ip)
            ret["upload_bandwidth"].append(float(upload_bandwidth))
            ret["download_bandwidth"].append(float(download_bandwidth))
    
    # get gf bandwidth of all worker nodes
    if gf_bandwidths_flag:
        ret["gf_bandwidth"] = recorders.GetGFBandwidths(ret["cpu"], w)


    return ret

# collect node stats for generate ecdag 
def CollectJobs():
    with open("/root/coar/conf/1.json") as f:
        conf = json.load(f)
    agent_ips = conf["agent_ips"]
    coor_connect = Redis(host="localhost", port=6379, db=0)
    download_tasks = []
    upload_tasks = []
    for i in range(len(agent_ips)):
        agent_ip = agent_ips[i]        
        job_cnt = coor_connect.get('download_tasks_' + agent_ip)
        if job_cnt is None:
            download_tasks.append(0)
        else:
            download_tasks.append(int(job_cnt))
        job_cnt = coor_connect.get('upload_tasks_' + agent_ip)
        if job_cnt is None:
            upload_tasks.append(0)
        else:
            upload_tasks.append(int(job_cnt))
    return download_tasks, upload_tasks

def UpdateTasks(download_tasks, upload_tasks):
    with open("/root/coar/conf/1.json") as f:
        conf = json.load(f)
    agent_ips = conf["agent_ips"]
    coor_connect = Redis(host="localhost", port=6379, db=0)
    for i in range(len(agent_ips)):
        agent_ip = agent_ips[i]        
        coor_connect.set('download_tasks_' + agent_ip, download_tasks[i])
        coor_connect.set('upload_tasks_' + agent_ip, upload_tasks[i])

def ReStoreTasks(download_selector, upload_selector):
    print(f"download_selector: {download_selector}")
    print(f"upload_selector: {upload_selector}")
    with open("/root/coar/conf/1.json") as f:
        conf = json.load(f)
    agent_ips = conf["agent_ips"]
    coor_connect = Redis(host="localhost", port=6379, db=0)
    for i in range(len(agent_ips)):
        node_id = i + 1
        agent_ip = agent_ips[i]
        val = download_selector.get(node_id, 0)
        if val != 0:
            coor_connect.decrby('download_tasks_' + agent_ip, val)
        val = upload_selector.get(node_id, 0)
        if val != 0:
            coor_connect.decrby('upload_tasks_' + agent_ip, val)
