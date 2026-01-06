import os
import time
import subprocess
import csv
import re
import random
from datetime import datetime

EC_TEST_CMD = ["/root/lmq_openec/build/ECTest"] 

NIC_NAME = "eth0" 
MAX_BANDWIDTH_MBPS = 15000  

CSV_FILE = "/root/lmq_openec/script/system_metrics.csv"
RESOURCE_CSV_FILE = "/root/lmq_openec/script/resource_usage.csv"

SAMPLE_INTERVAL = 5


class ResourceMonitor:
    """
    监控 CPU 和 内存使用率
    """
    def __init__(self):
        self.last_cpu_total = 0
        self.last_cpu_idle = 0
        # 初始化读取一次，作为计算差值的基准
        self._read_cpu_stats()

    def _read_cpu_stats(self):
        """读取 /proc/stat 获取 CPU 时间片"""
        try:
            with open('/proc/stat', 'r') as f:
                line = f.readline()  # 第一行通常是 'cpu  ...'
                parts = line.split()
                # /proc/stat 格式: cpu  user nice system idle iowait irq softirq ...
                # 我们只需要数值部分
                times = [float(x) for x in parts[1:]]
                
                idle = times[3]  # 第4列是 idle
                total = sum(times) # 所有列之和是 total
                
                return total, idle
        except Exception as e:
            print(f"[Error] Reading /proc/stat failed: {e}")
            return 0, 0

    def get_resource_usage(self):
        """
        返回: (cpu_usage_percent, mem_usage_percent)
        """
        # 1. 计算 CPU 使用率
        curr_total, curr_idle = self._read_cpu_stats()
        
        diff_total = curr_total - self.last_cpu_total
        diff_idle = curr_idle - self.last_cpu_idle
        
        cpu_usage = 0.0
        if diff_total > 0:
            # 使用率 = 1 - (空闲时间增量 / 总时间增量)
            cpu_usage = (1.0 - (diff_idle / diff_total)) * 100.0

        # 更新状态
        self.last_cpu_total = curr_total
        self.last_cpu_idle = curr_idle

        # 2. 获取 内存 使用率
        mem_total = 0
        mem_avail = 0
        try:
            with open('/proc/meminfo', 'r') as f:
                for line in f:
                    if 'MemTotal:' in line:
                        mem_total = int(line.split()[1]) # KB
                    elif 'MemAvailable:' in line:
                        mem_avail = int(line.split()[1]) # KB
                    # 如果读到了两个值就可以退出了
                    if mem_total > 0 and mem_avail > 0:
                        break
        except Exception:
            pass
            
        mem_usage = 0.0
        if mem_total > 0:
            mem_usage = ((mem_total - mem_avail) / mem_total) * 100.0

        return cpu_usage, mem_usage

class NetworkMonitor:
    def __init__(self, interface, capacity_mbps):
        self.interface = interface
        # 将 Mbps 转换为 Bytes/s 用于计算: (Mbps * 1,000,000) / 8
        self.capacity_bytes = (capacity_mbps * 1_000_000) / 8
        self.last_rx = 0
        self.last_tx = 0
        self.last_time = 0
        self._init_counters()

    def _read_proc_net_dev(self):
        try:
            with open('/proc/net/dev', 'r') as f:
                lines = f.readlines()
            for line in lines:
                if self.interface in line:
                    data = line.split(':')[1].split()
                    # data[0]: RX bytes (下载), data[8]: TX bytes (上传)
                    return int(data[0]), int(data[8])
        except Exception as e:
            print(f"[Error] Reading /proc/net/dev failed: {e}")
            return 0, 0
        return 0, 0

    def _init_counters(self):
        self.last_rx, self.last_tx = self._read_proc_net_dev()
        self.last_time = time.time()

    def get_available_bandwidth(self):
        current_rx, current_tx = self._read_proc_net_dev()
        current_time = time.time()
        
        time_delta = current_time - self.last_time
        if time_delta <= 0:
            return 0.0, 0.0

        # 计算这一瞬间的占用带宽 (Bytes/s)
        used_rx_bps = (current_rx - self.last_rx) / time_delta
        used_tx_bps = (current_tx - self.last_tx) / time_delta

        # 更新状态
        self.last_rx = current_rx
        self.last_tx = current_tx
        self.last_time = current_time

        # 计算剩余可用带宽 (Capacity - Used)
        # 确保不出现负数
        avail_rx_bytes = max(0, self.capacity_bytes - used_rx_bps)
        avail_tx_bytes = max(0, self.capacity_bytes - used_tx_bps)

        # 转换回 Mbps
        avail_rx_mbps = (avail_rx_bytes * 8) / 1_000_000
        avail_tx_mbps = (avail_tx_bytes * 8) / 1_000_000

        return avail_rx_mbps, avail_tx_mbps

def run_ec_test_real():
    try:
        # 执行命令，捕获标准输出
        result = subprocess.run(
            EC_TEST_CMD, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            universal_newlines=True,
            check=True
        )
        output = result.stdout.strip()
        
        # 假设 ECTest 输出格式为: "Time: 0.25 s, Data: 64 MB"
        # 使用正则表达式提取数值
        # 你需要根据实际 ECTest 的输出修改这里的正则
        time_match = re.search(r"Time:\s*([\d\.]+)", output)
        data_match = re.search(r"Data:\s*([\d\.]+)", output)
        
        if time_match and data_match:
            calc_time = float(time_match.group(1))
            data_size = float(data_match.group(1))
            return calc_time, data_size
        else:
            print(f"[Error] Failed to parse output: {output}")
            return None, None

    except FileNotFoundError:
        print("[Error] ECTest binary not found.")
        return None, None
    except Exception as e:
        print(f"[Error] execution failed: {e}")
        return None, None

def main():
    net_monitor = NetworkMonitor(NIC_NAME, MAX_BANDWIDTH_MBPS)
    res_monitor = ResourceMonitor()


    file_exists = os.path.isfile(CSV_FILE)
    with open(CSV_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                "Timestamp", 
                "EC_Time(s)", 
                "EC_Data(MB)", 
                "Compute_Speed(MB/s)", 
                "Avail_Download(Mbps)", 
                "Avail_Upload(Mbps)"
            ])


    file_exists = os.path.isfile(RESOURCE_CSV_FILE)
    with open(RESOURCE_CSV_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                "Timestamp", "CPU_Usage(%)", "Mem_Usage(%)"
            ])


    print(f"Starting monitor. Logging to {CSV_FILE}...")
    print("-" * 60)

    try:
        for i in range(100):
            cycle_start = time.time()
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            cpu_pct, mem_pct = res_monitor.get_resource_usage()            
            # 写入资源监控 CSV
            with open(RESOURCE_CSV_FILE, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    timestamp, 
                    f"{cpu_pct:.2f}", 
                    f"{mem_pct:.2f}"
                ])

            calc_time, data_size = run_ec_test_real()

            # 计算计算速度 (吞吐量)
            compute_speed = 0
            if calc_time and data_size and calc_time > 0:
                compute_speed = data_size / calc_time * 8
            else:
                calc_time, data_size = 0, 0

            # 2. 获取当前网络可用带宽
            # 注意：这里计算的是从上一次循环结束到现在的网络状态
            avail_rx, avail_tx = net_monitor.get_available_bandwidth()

            # 3. 数据写入 CSV
            log_row = [
                timestamp,
                f"{calc_time:.4f}",
                f"{data_size:.2f}",
                f"{compute_speed:.2f}",
                f"{avail_rx:.2f}",
                f"{avail_tx:.2f}"
            ]
            
            with open(CSV_FILE, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(log_row)

            # 屏幕打印
            print(f"[{timestamp}] EC Speed: {compute_speed:6.2f} Mbps | "
                  f"Net Avail: ↓{avail_rx:6.2f} Mbps, ↑{avail_tx:6.2f} Mbps")

            # 4. 等待下一个周期
            # 减去执行过程消耗的时间，保持周期稳定
            elapsed = time.time() - cycle_start
            sleep_time = max(0, SAMPLE_INTERVAL - elapsed)
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        print("\nMonitoring stopped.")

if __name__ == "__main__":
    main()