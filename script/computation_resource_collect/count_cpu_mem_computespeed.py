import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os

# --- 1. 配置路径 ---
METRICS_FILE = "/root/lmq_openec/script/system_metrics.csv"
RESOURCE_FILE = "/root/lmq_openec/script/resource_usage.csv"
OUTPUT_IMAGE = "cpu_mem_computespeed.pdf"

# --- 2. 手动定义列名 (对应生成脚本的顺序) ---
# 必须严格对应生成脚本中的写入顺序
COLS_METRICS = [
    "Timestamp", 
    "EC_Time(s)", 
    "EC_Data(MB)", 
    "Compute_Speed(MB/s)", 
    "Avail_Download(Mbps)", 
    "Avail_Upload(Mbps)"
]

COLS_RESOURCES = [
    "Timestamp", 
    "CPU_Usage(%)", 
    "Mem_Usage(%)"
]

def plot_data():
    print("正在读取数据 (无表头模式)...")
    
    if not os.path.exists(METRICS_FILE) or not os.path.exists(RESOURCE_FILE):
        print("错误：找不到 CSV 文件。")
        return

    # --- 3. 读取数据 ---
    # header=None: 告诉 pandas 第一行不是表头
    # names=...: 手动给每一列起名字
    try:
        df_metrics = pd.read_csv(METRICS_FILE, header=None, names=COLS_METRICS)
        df_res = pd.read_csv(RESOURCE_FILE, header=None, names=COLS_RESOURCES)
    except Exception as e:
        print(f"读取文件失败: {e}")
        return

    # --- 4. 数据清洗 ---
    # 转换时间格式
    try:
        df_metrics['Timestamp'] = pd.to_datetime(df_metrics['Timestamp'])
        df_res['Timestamp'] = pd.to_datetime(df_res['Timestamp'])
    except Exception as e:
        print(f"时间转换失败: {e}")
        print("请检查 CSV 第一列是否真的是时间格式 (例如 2023-10-01 12:00:00)")
        return

    # 合并数据
    df = pd.merge(df_metrics, df_res, on='Timestamp', how='inner')
    df = df.sort_values('Timestamp')

    if df.empty:
        print("错误：合并后数据为空，请检查两个文件的时间戳是否对应。")
        return

    # --- 5. 绘图 ---
    fig, ax1 = plt.subplots(figsize=(12, 6))
    plt.title('Performance Analysis: Resource Usage vs Compute Speed', fontsize=14, pad=15)
    ax1.grid(True, linestyle='--', alpha=0.5)

    # === 左轴：资源 (CPU/Mem) ===
    color_cpu = '#d62728'  # 红
    color_mem = '#1f77b4'  # 蓝

    l1 = ax1.plot(df['Timestamp'], df['CPU_Usage(%)'], color=color_cpu, label='CPU Usage', linewidth=1.5)
    l2 = ax1.plot(df['Timestamp'], df['Mem_Usage(%)'], color=color_mem, label='Mem Usage', linewidth=1.5)

    ax1.set_xlabel('Time')
    ax1.set_ylabel('Resource Usage (%)', color='black', fontsize=12)
    ax1.set_ylim(0, 105)

    # === 右轴：速度 ===
    ax2 = ax1.twinx()
    color_speed = '#2ca02c'  # 绿
    
    l3 = ax2.plot(df['Timestamp'], df['Compute_Speed(MB/s)'], 
                  color=color_speed, label='Compute Speed', 
                  linewidth=2, linestyle='--', marker='^', markersize=6)

    ax2.set_ylabel('Compute Speed (Mbps)', color=color_speed, fontsize=12)
    ax2.tick_params(axis='y', labelcolor=color_speed)

    # === 格式化 ===
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    fig.autofmt_xdate(rotation=45)

    # 合并图例
    lines = l1 + l2 + l3
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc='upper left', frameon=True, shadow=True)

    plt.tight_layout()
    plt.savefig(OUTPUT_IMAGE, dpi=300)
    print(f"成功！图片已保存为: {OUTPUT_IMAGE}")

if __name__ == "__main__":
    plot_data()