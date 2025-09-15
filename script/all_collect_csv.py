import pandas as pd
from pathlib import Path
import paramiko
from io import StringIO
import csv
import matplotlib.pyplot as plt

remote_dir = "/root/coar/script/sysstat"  
local_dir = "/root/coar/script/sysstat/aggregated_data" 
pic_dir = "/root/coar/script/sysstat/pic"  
ssh_username = "root"  
plt.rcParams['font.family'] = ['Times New Roman', 'serif']  

step_num = 160
Path(local_dir).mkdir(parents=True, exist_ok=True)
Path(pic_dir).mkdir(parents=True, exist_ok=True)


def fetch_remote_files():
    for i in range(1, 10):
        node = f"node0{i}"
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(node, username=ssh_username)
            
            sftp = ssh.open_sftp()
            
            for file_type in ["cpu", "mem", "disk", "net"]:
                remote_path = f"{remote_dir}/*-{file_type}.csv"
                
                # 执行远程查找命令
                stdin, stdout, stderr = ssh.exec_command(f"ls {remote_path}")
                file_list = stdout.read().decode().split()
                
                # 传输文件
                for remote_file in file_list:
                    local_filename = f"{node}_{Path(remote_file).name}"
                    local_path = Path(local_dir) / local_filename
                    sftp.get(remote_file, str(local_path))
            
            sftp.close()
            ssh.close()
            print(f"[Success] Retrieved files from {node}")
            
        except Exception as e:
            print(f"[Error] Failed to connect to {node}: {str(e)}")

def aggregate_data():
    node_stats = []
    # for each node
    for i in range(1, 10):
        node_name = "node0" + str(i)
        node_stat = {
            "cpu": [],
            "mem": [],
            "disk_read": [],
            "disk_write": [],
            "net_receive": [],
            "net_send": []
        }
        # for each time step
        for j in range(1, step_num + 1):
            cpu_csv_file = f"{local_dir}/{node_name}_{j}-cpu.csv"
            mem_csv_file = f"{local_dir}/{node_name}_{j}-mem.csv"
            disk_csv_file = f"{local_dir}/{node_name}_{j}-disk.csv"
            net_csv_file = f"{local_dir}/{node_name}_{j}-net.csv"

            # cpu
            with open(cpu_csv_file, 'r') as f:
                reader = csv.reader(f, delimiter=';')
                headers = next(reader) 
                user_index = headers.index('%user')
                user_values = []
                for row in reader:
                    value = float(row[user_index])
                    user_values.append(value)
            average = sum(user_values) / len(user_values)
            node_stat["cpu"].append(average)

            # mem
            with open(mem_csv_file, 'r') as f:
                reader = csv.reader(f, delimiter=';')
                headers = next(reader) 
                user_index = headers.index('%memused')
                user_values = []
                for row in reader:
                    value = float(row[user_index])
                    user_values.append(value)
            average = sum(user_values) / len(user_values)
            node_stat["mem"].append(average)


            # disk
            with open(disk_csv_file, 'r') as f:
                reader = csv.reader(f, delimiter=';')
                headers = next(reader) 
                user_index = headers.index('rkB/s')
                user_values = []

                for row in reader:
                    value = float(row[user_index])
                    user_values.append(value)
            average = sum(user_values)
            node_stat["disk_read"].append(average)

            with open(disk_csv_file, 'r') as f:
                reader = csv.reader(f, delimiter=';')
                headers = next(reader) 
                user_index = headers.index('wkB/s')
                user_values = []

                for row in reader:
                    value = float(row[user_index])
                    user_values.append(value)
            average = sum(user_values)
            node_stat["disk_write"].append(average)


            # net
            with open(net_csv_file, 'r') as f:
                reader = csv.reader(f, delimiter=';')
                headers = next(reader) 
                user_index = headers.index('rxkB/s')
                user_values = []
                for row in reader:
                    value = float(row[user_index])
                    user_values.append(value)
            average = sum(user_values) / len(user_values)
            node_stat["net_receive"].append(average)
            
            with open(net_csv_file, 'r') as f:
                reader = csv.reader(f, delimiter=';')
                headers = next(reader) 
                user_index = headers.index('txkB/s')
                user_values = []
                for row in reader:
                    value = float(row[user_index])
                    user_values.append(value)
            average = sum(user_values) / len(user_values)
            node_stat["net_send"].append(average)
        
        
        
        node_stats.append(node_stat)
    
    return node_stats

if __name__ == "__main__":
    fetch_remote_files()
    
    results = aggregate_data()
    
    x = list(range(1, step_num + 1))
    plt.figure(figsize=(30, 12))
    plt.plot(x, results[0]["cpu"], marker = 'o', label = 'node01')
    plt.plot(x, results[1]["cpu"], marker = 'o', label = 'node02')
    plt.plot(x, results[2]["cpu"], marker = 'o', label = 'node03')
    plt.plot(x, results[3]["cpu"], marker = 'o', label = 'node04')
    plt.plot(x, results[4]["cpu"], marker = 'o', label = 'node05')
    plt.plot(x, results[5]["cpu"], marker = 'o', label = 'node06')
    plt.plot(x, results[6]["cpu"], marker = 'o', label = 'node07')
    plt.plot(x, results[7]["cpu"], marker = 'o', label = 'node08')
    plt.plot(x, results[8]["cpu"], marker = 'o', label = 'node09')
    plt.xlabel('time step', fontsize=12)
    plt.ylabel('cpu util(%)', fontsize=12)
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.savefig(f"{pic_dir}/cpu.svg")
    plt.close()


    x = list(range(1, step_num + 1))
    plt.figure(figsize=(30, 17))
    subset_node_ids = [4, 5, 8]
    for node_id in subset_node_ids:
        plt.plot(x, results[node_id - 1]["cpu"], marker = 'o', label = f'node0{node_id}', markersize=8)
    plt.xlabel('time step', fontsize=60)
    plt.ylabel('cpu util(%)', fontsize=60)
    plt.xticks(fontsize=50)
    plt.yticks(fontsize=50)
    plt.legend(fontsize = 35)
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.savefig(f"{pic_dir}/cpu3_6_9.pdf")
    plt.close()

    x = list(range(1, step_num + 1))
    plt.figure(figsize=(30, 12))
    for node_id in subset_node_ids:
        plt.plot(x, results[node_id - 1]["mem"], marker = 'o', label = f'node0{node_id}')
    plt.xlabel('time step', fontsize=12)
    plt.ylabel('mem usage', fontsize=12)
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.savefig(f"{pic_dir}/mem3_6_9.svg")
    plt.close()

    
    x = list(range(1, step_num + 1))
    plt.figure(figsize=(30, 12))
    for node_id in subset_node_ids:
        plt.plot(x, results[node_id - 1]["net_receive"], marker = 'o', label = f'node0{node_id}')
    plt.xlabel('time step', fontsize=12)
    plt.ylabel('net receive(KB/s)', fontsize=12)
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.savefig(f"{pic_dir}/net_receive3_6_9.svg")
    plt.close()

    x = list(range(1, step_num + 1))
    plt.figure(figsize=(30, 12))
    for node_id in subset_node_ids:
        plt.plot(x, results[node_id - 1]["net_send"], marker = 'o', label = f'node0{node_id}')
    plt.xlabel('time step', fontsize=12)
    plt.ylabel('net send(KB/s)', fontsize=12)
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.savefig(f"{pic_dir}/net_send3_6_9.svg")
    plt.close()


    x = list(range(1, step_num + 1))
    plt.figure(figsize=(30, 12))
    for node_id in subset_node_ids:
        plt.plot(x, results[node_id - 1]["disk_read"], marker = 'o', label = f'node0{node_id}')
    plt.xlabel('time step', fontsize=12)
    plt.ylabel('disk read(KB/s)', fontsize=12)
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.savefig(f"{pic_dir}/disk_read3_6_9.svg")
    plt.close()



    x = list(range(1, step_num + 1))
    plt.figure(figsize=(30, 12))
    for node_id in subset_node_ids:
        plt.plot(x, results[node_id - 1]["disk_write"], marker = 'o', label = f'node0{node_id}')
    plt.xlabel('time step', fontsize=12)
    plt.ylabel('disk write(KB/s)', fontsize=12)
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.savefig(f"{pic_dir}/disk_write3_6_9.svg")
    plt.close()


    for i in range (1, 10):
        node_name = f"node0{i}"
        x = list(range(1, step_num + 1))
        plt.figure(figsize=(30, 12))
        plt.plot(x, results[i - 1]["cpu"], marker = 'o', label = 'cpu')
        plt.ylim(0, 100)
        plt.xlabel('time step', fontsize=12)
        plt.ylabel('cpu util(%)', fontsize=12)
        plt.title('cpu util over time', fontsize=14)
        plt.legend()
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.savefig(f"{pic_dir}/{node_name}_cpu.svg")
        plt.close()


    for i in range (1, 10):
        node_name = f"node0{i}"
        x = list(range(1, step_num + 1))
        plt.figure(figsize=(30, 12))
        plt.plot(x, results[i - 1]["mem"], marker = 'o', label = 'mem')
        plt.ylim(0, 100)
        plt.xlabel('time step', fontsize=12)
        plt.ylabel('mem util(%)', fontsize=12)
        plt.title('mem util over time', fontsize=14)
        plt.legend()
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.savefig(f"{pic_dir}/{node_name}_mem.svg")
        plt.close()

    for i in range (1, 10):
        node_name = f"node0{i}"
        x = list(range(1, step_num + 1))
        plt.figure(figsize=(30, 12))
        plt.plot(x, results[i - 1]["disk_read"], marker = 'o', markersize = 5, label = 'disk read')
        plt.plot(x, results[i - 1]["disk_write"], marker = 'x', markersize = 5, label = 'disk write')

        plt.xlabel('time step', fontsize=12)
        plt.ylabel('disk overhead(KB/s)', fontsize=12)
        plt.title('disk overhead over time', fontsize=14)
        plt.legend()
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.savefig(f"{pic_dir}/{node_name}_disk.svg")
        plt.close()


    for i in range (1, 10):
        node_name = f"node0{i}"
        x = list(range(1, step_num + 1))
        plt.figure(figsize=(30, 12))
        plt.plot(x, results[i - 1]["net_receive"], marker = 'o', markersize = 5, label = 'receive')
        plt.plot(x, results[i - 1]["net_send"], marker = 'x', markersize = 5, label = 'send')

        plt.xlabel('time step', fontsize=12)
        plt.ylabel('net overhead(KB/s)', fontsize=12)
        plt.title('net overhead over time', fontsize=14)
        plt.legend()
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.savefig(f"{pic_dir}/{node_name}_net.svg")
        plt.close()
