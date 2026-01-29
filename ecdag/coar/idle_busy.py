import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from scipy.signal import medfilt
import os

class NodeStatePredictor:
    def __init__(self, sample_rate_hz=1/5.0):
        self.sample_rate_hz = sample_rate_hz
        self.threshold = 0.0
        self.estimated_period = None
        self.start_timestamp = None
        self.busy_phase_mid = 0.0 
        self.is_fitted = False

    def _find_period(self, signal):
        detrended = signal - np.mean(signal)
        yf = np.fft.fft(detrended)
        xf = np.fft.fftfreq(len(signal), 1 / self.sample_rate_hz)
        peak_idx = np.argmax(np.abs(yf[1:len(signal)//2])) + 1
        return 1 / xf[peak_idx] if xf[peak_idx] > 0 else 0

    def fit(self, cpu_series, timestamps):
        data = cpu_series.values.reshape(-1, 1)
        kmeans = KMeans(n_clusters=2, n_init=10, random_state=42).fit(data)
        centers = sorted(kmeans.cluster_centers_.flatten())
        self.threshold = np.mean(centers)
        
        self.estimated_period = self._find_period(cpu_series.values)
        
        t_seconds = pd.to_datetime(timestamps).view(np.int64) // 10**9
        self.start_timestamp = t_seconds[0]
        relative_times = t_seconds - self.start_timestamp
        
        phases = (relative_times % self.estimated_period) / self.estimated_period
        
        busy_phases = phases[cpu_series >= self.threshold]
        self.busy_phase_mid = np.median(busy_phases)
        
        self.is_fitted = True

    def predict_state(self, current_cpu, current_time, window_data=None, lookahead_seconds=10.0):
        if not self.is_fitted:
            raise Exception("Model not fitted yet")

        t_current = pd.to_datetime(current_time).timestamp()
        phase = ((t_current - self.start_timestamp) % self.estimated_period) / self.estimated_period
        
        t_future = t_current + lookahead_seconds
        future_phase = ((t_future - self.start_timestamp) % self.estimated_period) / self.estimated_period
        if window_data is not None and len(window_data) >= 3:
            smoothed_cpu = medfilt(window_data, kernel_size=3)[-1]
        else:
            smoothed_cpu = current_cpu
        
        def check_in_busy_range(p):
            dist = min(abs(p - self.busy_phase_mid), 1 - abs(p - self.busy_phase_mid))
            return dist < 0.25 
        is_currently_busy_phase = check_in_busy_range(phase)
        will_be_busy_phase = check_in_busy_range(future_phase)

        if is_currently_busy_phase or will_be_busy_phase:
            return 1 if smoothed_cpu > (self.threshold * 0.7) else 0
        else:
            return 1 if smoothed_cpu > (self.threshold * 1.3) else 0


def get_migration_straggler_decision(all_node_cpus, all_node_timestamps, src_candidates, predictor, stats, k):

    node_states = []
    source_node_idx = -1
    
    idle_node_indices = []
    
    node_num = len(all_node_cpus)
    dw_bw_list = stats.get("download_bandwidth", [])
    gf_bw_list = stats.get("gf_bandwidth", [])

    for i in range(node_num):
        cpu_series = np.array(all_node_cpus[i])
        time_series = np.array(all_node_timestamps[i])
        
        curr_cpu = cpu_series[-1]
        curr_time = time_series[-1]
        
        window = cpu_series[-3:] if len(cpu_series) >= 3 else cpu_series
        
        state = predictor.predict_state(curr_cpu, curr_time, window)
        node_states.append(state)
        
        if state == 1 and source_node_idx == -1 and i in src_candidates:
            source_node_idx = i
        
        elif state == 0:
            idle_node_indices.append(i)
    
    target_node_idx = -1
    max_score = -1.0
    
    for idx in idle_node_indices:
        if idx >= len(dw_bw_list) or idx >= len(gf_bw_list):
            continue
            
        dbw = dw_bw_list[idx]
        gbw = gf_bw_list[idx]
        
        if dbw < 0 or gbw < 0:
            continue
            
        current_score = min(dbw / (k - 1), gbw / k)
        
        if current_score > max_score or idx in src_candidates:
            max_score = current_score
            target_node_idx = idx

    return source_node_idx, target_node_idx, np.array(node_states)

if __name__ == "__main__":

    predictor = NodeStatePredictor(sample_rate_hz=1/5.0)
    train_df = pd.read_csv("../script/sysstat/resource_profile_data/sample.csv", 
                           header=None, names=['timestamp', 'cpu', 'mem_percent']).dropna()
    predictor.fit(train_df['cpu'], train_df['timestamp'])

    node_cpus = [[8.76, 22.84, 8.30], [95.99, 92.82, 95.65], [95.91, 58.98, 94.94], [8.30, 7.58, 8.28], [98.30, 95.58, 96.29], [7.70, 37.44, 8.80], [99.65, 92.60, 98.25], [15.03, 8.30, 7.58], [95.17, 95.33, 98.70], [8.35, 7.34, 8.32], [92.13, 96.55, 88.99], [7.57, 10.64, 7.37], [95.80, 95.94, 90.65], [8.78, 7.56, 8.29]]
    node_times = [ ["2026-01-28 10:11:00", "2026-01-28 10:11:05", "2026-01-28 10:11:10"] for _ in range(len(node_cpus)) ] 
    stats = {
        "download_bandwidth": [1342.5, 1288.4, 1415.2, 1306.8, 1462.1, 1295.7, 1384.3, -1, 1433.6, 1478.9, 1355.4, 1392.1, 1321.8, 1445.7],
        "gf_bandwidth": [1178.234, 1162.512, 1205.884, 1148.921, 1422.356, 1155.672, 1189.431, -1, 1212.556, 1431.125, 1174.889, 1195.342, 1168.455, 1418.667]
    }
    src_candidates = [0, 1, 3, 4]


    src, dst, states = get_migration_straggler_decision(node_cpus, node_times, src_candidates, predictor, stats, k=10)

    if src != -1 and dst != -1:
        print(f"src: {src}, dst: {dst}")