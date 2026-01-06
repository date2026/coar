import pandas as pd
from pathlib import Path
import paramiko
from io import StringIO
import csv
import matplotlib.pyplot as plt

remote_dir = "/root/lmq_openec/script/"  # 远程节点内存带宽文件存放目录
local_dir = "/root/lmq_openec/script/sysstat/aggregated_bandwidth_data"  # 本地带宽文件存放目录
pic_dir = "/root/lmq_openec/script/sysstat/bandwidth_pic"  # 图片输出目录
ssh_username = "root"  
bandwidth_filename = "linux_memory_bandwidth_mbw_stats.csv"  
total_nodes = 31  
step_num = 256

# 绘图字体配置（保持与原脚本一致）
plt.rcParams['font.family'] = ['Times New Roman', 'serif']
plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['ps.fonttype'] = 42

# 创建目录（若不存在）
Path(local_dir).mkdir(parents=True, exist_ok=True)
Path(pic_dir).mkdir(parents=True, exist_ok=True)


def get_node_name(node_index):
    """
    生成节点名称（node01-node09, node10-node32）
    :param node_index: 节点序号（1-32）
    :return: 标准化节点名
    """
    if node_index < 10:
        return f"node0{node_index}"
    else:
        return f"node{node_index}"


def fetch_remote_bandwidth_files():
    """
    从所有远程节点拉取内存带宽统计文件
    """
    for node_idx in range(1, total_nodes + 1):
        node_name = get_node_name(node_idx)
        try:
            # 建立SSH连接
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(node_name, username=ssh_username)
            
            # 打开SFTP会话
            sftp = ssh.open_sftp()
            
            # 远程带宽文件路径
            remote_bandwidth_path = f"{remote_dir}/{bandwidth_filename}"
            
            # 本地带宽文件路径（添加节点前缀区分）
            local_bandwidth_filename = f"{node_name}_{bandwidth_filename}"
            local_bandwidth_path = Path(local_dir) / local_bandwidth_filename
            
            # 拉取文件
            sftp.get(remote_bandwidth_path, str(local_bandwidth_path))
            
            # 关闭连接
            sftp.close()
            ssh.close()
            print(f"[Success] Retrieved bandwidth file from {node_name}, saved to {local_bandwidth_path}")
            
        except Exception as e:
            print(f"[Error] Failed to connect to {node_name} or fetch bandwidth file: {str(e)}")


def aggregate_bandwidth_data():
    """
    聚合所有节点的内存带宽数据，核心指标为avg_bandwidth_mb_s（MB/s），可选指标为avg_bandwidth_gb_s（GB/s）
    :return: 节点内存带宽统计列表
    """
    node_bandwidth_stats = []
    
    for node_idx in range(1, total_nodes + 1):
        node_name = get_node_name(node_idx)
        node_bandwidth_stat = {
            "avg_bandwidth_mb_s": [],  # 核心指标：平均内存带宽（MB/s）
            "avg_bandwidth_gb_s": []   # 可选指标：平均内存带宽（GB/s，更直观）
        }
        
        # 读取当前节点的带宽CSV文件
        local_bandwidth_filename = f"{node_name}_{bandwidth_filename}"
        local_bandwidth_path = Path(local_dir) / local_bandwidth_filename
        
        try:
            with open(local_bandwidth_path, 'r', encoding="utf-8") as f:
                reader = csv.DictReader(f)
                # 按统计次数提取数据（对应step_num=100次）
                for row_idx, row in enumerate(reader):
                    if row_idx >= step_num:
                        break
                    # 提取核心指标：平均带宽（MB/s）
                    avg_bw_mb = float(row["avg_bandwidth_mb_s"])
                    node_bandwidth_stat["avg_bandwidth_mb_s"].append(avg_bw_mb)
                    
                    # 提取可选指标：平均带宽（GB/s）
                    avg_bw_gb = float(row["avg_bandwidth_gb_s"])
                    node_bandwidth_stat["avg_bandwidth_gb_s"].append(avg_bw_gb)
            
            print(f"[Success] Aggregated bandwidth data for {node_name}")
        
        except Exception as e:
            print(f"[Error] Failed to aggregate bandwidth data for {node_name}: {str(e)}")
            # 若读取失败，填充空值保证数据结构一致
            node_bandwidth_stat["avg_bandwidth_mb_s"] = [0.0] * step_num
            node_bandwidth_stat["avg_bandwidth_gb_s"] = [0.0] * step_num
        
        node_bandwidth_stats.append(node_bandwidth_stat)
    
    return node_bandwidth_stats


# plot all nodes memory bandwidth usages
if __name__ == "__main__":
    print("===== Start fetching remote bandwidth files =====")
    fetch_remote_bandwidth_files()
    
    print("\n===== Start aggregating bandwidth data =====")
    bandwidth_results = aggregate_bandwidth_data()
    
    x = list(range(1, step_num + 1))
    subset_node_ids = [1, 5, 8, 10, 15, 20, 25, 31]
    
    print("\n===== Start drawing all nodes bandwidth chart (MB/s) =====")
    plt.figure(figsize=(50, 20)) 
    for node_idx in range(total_nodes):
        node_name = get_node_name(node_idx + 1)
        plt.plot(x, bandwidth_results[node_idx]["avg_bandwidth_mb_s"], 
                 marker='o', markersize=4, label=node_name, alpha=0.7)
    
    plt.xlabel('time step', fontsize=16)
    plt.ylabel('Average Memory Bandwidth (MB/s)', fontsize=16)
    plt.title('Average Memory Bandwidth (MB/s) of All Nodes Over Time', fontsize=20)
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=10)
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()  # 自动调整布局
    plt.savefig(f"{pic_dir}/all_nodes_bandwidth_mb_s.svg")
    plt.close()
    print(f"[Success] Saved all nodes bandwidth chart (MB/s) to {pic_dir}/all_nodes_bandwidth_mb_s.svg")
    
    # 4. 绘制指定子集节点的核心带宽指标折线图（PDF高清格式，仿照原脚本样式）
    print("\n===== Start drawing subset nodes bandwidth PDF chart (MB/s) =====")
    fig, ax = plt.subplots(figsize=(33, 22))
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    for node_id in subset_node_ids:
        node_idx = node_id - 1
        node_name = get_node_name(node_id)
        plt.plot(x, bandwidth_results[node_idx]["avg_bandwidth_mb_s"], 
                 marker='o', markersize=8, label=node_name, linewidth=2)
    
    plt.xlabel('time step', fontsize=80)
    plt.ylabel('Average Memory Bandwidth (MB/s)', fontsize=80)
    plt.xticks(fontsize=80)
    plt.yticks(fontsize=80)
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.05), ncol=4, fontsize=70, frameon=False,
              columnspacing=0.2, labelspacing=0.1, handlelength=1.0, handletextpad=0.2)
    plt.tight_layout()
    plt.savefig(f"{pic_dir}/subset_nodes_bandwidth_mb_s.pdf")
    plt.close()
    print(f"[Success] Saved subset nodes bandwidth PDF chart (MB/s) to {pic_dir}/subset_nodes_bandwidth_mb_s.pdf")
    
    # 5. 绘制指定子集节点的核心带宽指标折线图（SVG格式）
    print("\n===== Start drawing subset nodes bandwidth SVG chart (MB/s) =====")
    plt.figure(figsize=(30, 12))
    for node_id in subset_node_ids:
        node_idx = node_id - 1
        node_name = get_node_name(node_id)
        plt.plot(x, bandwidth_results[node_idx]["avg_bandwidth_mb_s"], 
                 marker='o', label=node_name)
    
    plt.xlabel('time step', fontsize=12)
    plt.ylabel('Average Memory Bandwidth (MB/s)', fontsize=12)
    plt.title('Average Memory Bandwidth (MB/s) of Subset Nodes Over Time', fontsize=14)
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.savefig(f"{pic_dir}/subset_nodes_bandwidth_mb_s.svg")
    plt.close()
    print(f"[Success] Saved subset nodes bandwidth SVG chart (MB/s) to {pic_dir}/subset_nodes_bandwidth_mb_s.svg")
    
    # 6. 绘制每个节点单独的核心带宽指标折线图（SVG格式，MB/s）
    print("\n===== Start drawing single node bandwidth charts (MB/s) =====")
    for node_idx in range(total_nodes):
        node_id = node_idx + 1
        node_name = get_node_name(node_id)
        
        plt.figure(figsize=(30, 12))
        # 绘制核心指标：平均带宽（MB/s）
        plt.plot(x, bandwidth_results[node_idx]["avg_bandwidth_mb_s"], 
                 marker='o', color='#2E86AB', label='Average Bandwidth (MB/s)')
        
        plt.xlabel('time step', fontsize=12)
        plt.ylabel('Average Memory Bandwidth (MB/s)', fontsize=12)
        plt.title(f'Average Memory Bandwidth (MB/s) of {node_name} Over Time', fontsize=14)
        plt.legend()
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.savefig(f"{pic_dir}/{node_name}_bandwidth_mb_s.svg")
        plt.close()
        
        if node_idx % 10 == 0:
            print(f"[Progress] Finished {node_idx+1}/{total_nodes} single node bandwidth charts (MB/s)")
    
    # 7. 绘制每个节点单独的可选带宽指标折线图（SVG格式，GB/s，更直观）
    print("\n===== Start drawing single node bandwidth charts (GB/s, optional) =====")
    for node_idx in range(total_nodes):
        node_id = node_idx + 1
        node_name = get_node_name(node_id)
        
        plt.figure(figsize=(30, 12))
        # 绘制可选指标：平均带宽（GB/s）
        plt.plot(x, bandwidth_results[node_idx]["avg_bandwidth_gb_s"], 
                 marker='x', color='#A23B72', label='Average Bandwidth (GB/s)')
        
        plt.xlabel('time step', fontsize=12)
        plt.ylabel('Average Memory Bandwidth (GB/s)', fontsize=12)
        plt.title(f'Average Memory Bandwidth (GB/s) of {node_name} Over Time', fontsize=14)
        plt.legend()
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.savefig(f"{pic_dir}/{node_name}_bandwidth_gb_s.svg")
        plt.close()
        
        if node_idx % 10 == 0:
            print(f"[Progress] Finished {node_idx+1}/{total_nodes} single node bandwidth charts (GB/s)")
    
    # 8. 绘制所有节点的可选带宽指标（avg_bandwidth_gb_s）折线图（SVG格式，可选）
    print("\n===== Start drawing all nodes bandwidth chart (GB/s, optional) =====")
    plt.figure(figsize=(50, 20))
    for node_idx in range(total_nodes):
        node_name = get_node_name(node_idx + 1)
        plt.plot(x, bandwidth_results[node_idx]["avg_bandwidth_gb_s"], 
                 marker='x', markersize=4, label=node_name, alpha=0.7)
    
    plt.xlabel('time step', fontsize=16)
    plt.ylabel('Average Memory Bandwidth (GB/s)', fontsize=16)
    plt.title('Average Memory Bandwidth (GB/s) of All Nodes Over Time', fontsize=20)
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=10)
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()
    plt.savefig(f"{pic_dir}/all_nodes_bandwidth_gb_s.svg")
    plt.close()
    print(f"[Success] Saved all nodes bandwidth chart (GB/s) to {pic_dir}/all_nodes_bandwidth_gb_s.svg")
    
    print("\n===== All memory bandwidth chart drawing tasks completed =====")
    print(f"All charts are saved to: {pic_dir}")