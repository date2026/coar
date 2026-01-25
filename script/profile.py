import os
import time
import subprocess
import csv
import re
import random
from datetime import datetime

EC_TEST_CMD = ["[your path]/build/ECTest"] 

NIC_NAME = "eth0" 
MAX_BANDWIDTH_MBPS = 15000  

CSV_FILE = "[your path]/script/system_metrics.csv"
RESOURCE_CSV_FILE = "[your path]/script/resource_usage.csv"

SAMPLE_INTERVAL = 5


class ResourceMonitor:

    def __init__(self):
        self.last_cpu_total = 0
        self.last_cpu_idle = 0
        self._read_cpu_stats()

    def _read_cpu_stats(self):
        try:
            with open('/proc/stat', 'r') as f:
                line = f.readline()  
                parts = line.split()
                times = [float(x) for x in parts[1:]]
                
                idle = times[3]  
                total = sum(times) 
                
                return total, idle
        except Exception as e:
            print(f"[Error] Reading /proc/stat failed: {e}")
            return 0, 0

    def get_resource_usage(self):
        curr_total, curr_idle = self._read_cpu_stats()
        
        diff_total = curr_total - self.last_cpu_total
        diff_idle = curr_idle - self.last_cpu_idle
        
        cpu_usage = 0.0
        if diff_total > 0:
            cpu_usage = (1.0 - (diff_idle / diff_total)) * 100.0

        self.last_cpu_total = curr_total
        self.last_cpu_idle = curr_idle

        mem_total = 0
        mem_avail = 0
        try:
            with open('/proc/meminfo', 'r') as f:
                for line in f:
                    if 'MemTotal:' in line:
                        mem_total = int(line.split()[1]) # KB
                    elif 'MemAvailable:' in line:
                        mem_avail = int(line.split()[1]) # KB
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

        used_rx_bps = (current_rx - self.last_rx) / time_delta
        used_tx_bps = (current_tx - self.last_tx) / time_delta

        self.last_rx = current_rx
        self.last_tx = current_tx
        self.last_time = current_time

        avail_rx_bytes = max(0, self.capacity_bytes - used_rx_bps)
        avail_tx_bytes = max(0, self.capacity_bytes - used_tx_bps)

        avail_rx_mbps = (avail_rx_bytes * 8) / 1_000_000
        avail_tx_mbps = (avail_tx_bytes * 8) / 1_000_000

        return avail_rx_mbps, avail_tx_mbps

def run_ec_test_real():
    try:
        result = subprocess.run(
            EC_TEST_CMD, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            universal_newlines=True,
            check=True
        )
        output = result.stdout.strip()
        
        
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
            with open(RESOURCE_CSV_FILE, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    timestamp, 
                    f"{cpu_pct:.2f}", 
                    f"{mem_pct:.2f}"
                ])

            calc_time, data_size = run_ec_test_real()

            compute_speed = 0
            if calc_time and data_size and calc_time > 0:
                compute_speed = data_size / calc_time * 8
            else:
                calc_time, data_size = 0, 0

            avail_rx, avail_tx = net_monitor.get_available_bandwidth()

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

            print(f"[{timestamp}] EC Speed: {compute_speed:6.2f} Mbps | "
                  f"Net Avail: ↓{avail_rx:6.2f} Mbps, ↑{avail_tx:6.2f} Mbps")

            elapsed = time.time() - cycle_start
            sleep_time = max(0, SAMPLE_INTERVAL - elapsed)
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        print("\nMonitoring stopped.")

if __name__ == "__main__":
    main()