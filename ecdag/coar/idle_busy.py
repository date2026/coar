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
def get_migration_straggler_decision(all_node_cpus, all_node_timestamps, predictor):
    node_states = []
    source_node_idx = -1
    target_node_idx = -1
    
    node_num = len(all_node_cpus)

    for i in range(node_num):
        cpu_series = np.array(all_node_cpus[i])
        time_series = np.array(all_node_timestamps[i])
        
        curr_cpu = cpu_series[-1]
        curr_time = time_series[-1]
        
        window = cpu_series[-3:] if len(cpu_series) >= 3 else cpu_series
        
        state = predictor.predict_state(curr_cpu, curr_time, window)
        node_states.append(state)
        
        if state == 1 and source_node_idx == -1:
            source_node_idx = i
        elif state == 0 and target_node_idx == -1:
            target_node_idx = i
                

    return source_node_idx, target_node_idx, np.array(node_states)

if __name__ == "__main__":

    predictor = NodeStatePredictor(sample_rate_hz=1/5.0)
    train_df = pd.read_csv("../script/sysstat/resource_profile_data/sample.csv", 
                           header=None, names=['timestamp', 'cpu', 'mem_percent']).dropna()
    predictor.fit(train_df['cpu'], train_df['timestamp'])

    node_cpus = [[8.76, 22.84, 8.30], [95.99, 92.82, 95.65], [95.91, 58.98, 94.94], [8.30, 7.58, 8.28], [98.30, 95.58, 96.29], [7.70, 37.44, 8.80], [99.65, 92.60, 98.25], [15.03, 8.30, 7.58], [95.17, 95.33, 98.70], [8.35, 7.34, 8.32], [92.13, 96.55, 88.99], [7.57, 10.64, 7.37], [95.80, 95.94, 90.65], [8.78, 7.56, 8.29]]
    node_times = [ ["2026-01-28 10:11:00", "2026-01-28 10:11:05", "2026-01-28 10:11:10"] for _ in range(len(node_cpus)) ] 

    src, dst, states = get_migration_straggler_decision(node_cpus, node_times, predictor)

    if src != -1 and dst != -1:
        print(f"src: {src}, dst: {dst}")