import os
import glob
import pandas as pd
import numpy as np

# ================= 配置区域 =================
DATA_DIR = "/root/lmq_openec/script/profile_data"
LIMIT_STEPS = 70

# 标准列名
EXPECTED_COLS = [
    "Timestamp", "EC_Time(s)", "EC_Data(MB)", 
    "Compute_Speed(MB/s)", "Avail_Download(Mbps)", "Avail_Upload(Mbps)"
]

def load_data(data_dir):
    all_files = glob.glob(os.path.join(data_dir, "*.csv"))
    if not all_files:
        print(f"[Error] No CSV files found in {data_dir}")
        return []

    all_files.sort()
    
    node_data = []
    for filename in all_files:
        try:
            df = pd.read_csv(filename, skipinitialspace=True)
            df.columns = [c.strip() for c in df.columns]

            if "Timestamp" not in df.columns:
                first_col_name = str(df.columns[0])
                if "202" in first_col_name or ":" in first_col_name:
                    df = pd.read_csv(filename, names=EXPECTED_COLS, header=None, skipinitialspace=True)
                else:
                    continue

            node_name = os.path.basename(filename).replace("node_", "").replace(".csv", "")
            node_data.append((node_name, df))
        except Exception as e:
            print(f"[Error] Failed to process {filename}: {e}")
    
    return node_data

def find_high_compute_low_download(node_data):
    if not node_data:
        print("No data loaded.")
        return

    nodes = [n for n, _ in node_data]
    n_nodes = len(nodes)
    
    # 1. 构建矩阵
    mat_compute = np.zeros((LIMIT_STEPS, n_nodes))
    mat_down = np.zeros((LIMIT_STEPS, n_nodes))

    for i, (name, df) in enumerate(node_data):
        count = min(len(df), LIMIT_STEPS)
        mat_compute[:count, i] = df['Compute_Speed(MB/s)'].values[:count]
        mat_down[:count, i]    = df['Avail_Download(Mbps)'].values[:count]

    # 2. 寻找最佳时间步
    best_t = -1
    max_total_score = -float('inf')
    best_scores = []

    for t in range(30, LIMIT_STEPS):
        c_row = mat_compute[t]
        d_row = mat_down[t]
        
        # 归一化到 0~1 (Min-Max Scaling)
        # 加上 1e-6 防止分母为 0
        c_min, c_max = np.min(c_row), np.max(c_row)
        d_min, d_max = np.min(d_row), np.max(d_row)
        
        c_norm = (c_row - c_min) / (c_max - c_min + 1e-6)
        d_norm = (d_row - d_min) / (d_max - d_min + 1e-6)
        
        # === 核心逻辑 ===
        # 我们寻找 Compute 高 (接近1)，Download 低 (接近0) 的节点
        # Score = Norm_Compute - Norm_Download
        # Score 越大，这种特征越明显
        diffs = c_norm - d_norm
        
        # 计算该时刻的总分：只关注 Score > 0 的节点 (符合我们特征的节点)
        current_total_score = np.sum(diffs[diffs > 0])
        
        if current_total_score > max_total_score:
            max_total_score = current_total_score
            best_t = t
            best_scores = diffs

    # 3. 打印结果
    print("\n" + "="*80)
    print(f" ANALYSIS: Nodes with High Compute & Low Download")
    print("="*80)
    print(f"Target TimeStep (Index): {best_t}")
    print(f"Total Mismatch Score:    {max_total_score:.4f}")
    print("-" * 80)
    print(f"{'Node':<10} | {'Compute (MB/s)':<18} | {'Down (Mbps)':<15} | {'Mismatch Score'}")
    print("-" * 80)

    # 按分数从高到低排序 (分数越高，越符合“计算强、网络弱”)
    indices = np.argsort(best_scores)[::-1]
    
    for i in indices:
        node_name = nodes[i]
        c_val = mat_compute[best_t, i]
        d_val = mat_down[best_t, i]
        score = best_scores[i]
        
        # 标记出符合特征的节点 (分数 > 0.2 算显著)
        marker = "(*)" if score > 0.2 else "   "
        print(f"{marker} {node_name:<6} | {c_val:<18.2f} | {d_val:<15.2f} | {score:+.4f}")
    
    print("-" * 80)
    print("(*) Mismatch Score = Normalized_Compute - Normalized_Download")
    print("    Score 接近 +1.0 表示该节点是全场计算最强且下载最弱。")





# find time step, whose repair computation throughput and network bandwidth differ most
if __name__ == "__main__":
    data = load_data(DATA_DIR)
    find_high_compute_low_download(data)