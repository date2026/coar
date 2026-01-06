import pandas as pd
from pathlib import Path
import paramiko
from io import StringIO
import csv
import matplotlib.pyplot as plt

remote_dir = "/root/lmq_openec/script/sysstat_pagerank_executor_8_core_4_memory_1_iter_300"  
local_dir = "/root/lmq_openec/script/sysstat_pagerank_executor_8_core_4_memory_1_iter_300/aggregated_data" 
pic_dir = "/root/lmq_openec/script/sysstat_pagerank_executor_8_core_4_memory_1_iter_300/pic"  
ssh_username = "root"  
plt.rcParams['font.family'] = ['Times New Roman', 'serif']  

step_num = 200
Path(local_dir).mkdir(parents=True, exist_ok=True)
Path(pic_dir).mkdir(parents=True, exist_ok=True)

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
    
    results = aggregate_data()
    
    node_index = 5
    node_data = results[node_index]
    
    # 创建 DataFrame
    df = pd.DataFrame({
        'cpu': node_data['cpu'],
        'net_receive': node_data['net_receive'],
        'net_send': node_data['net_send']
    })
    
    # 保存到 CSV 文件
    output_file = f"{local_dir}/cpu_net.csv"
    df.to_csv(output_file, index=False)
    print(f"已保存数据到: {output_file}")
    