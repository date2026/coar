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
        self.busy_phase_offset = 0.0 
        self.is_fitted = False

    def _find_period(self, signal):
        detrended = signal - np.mean(signal)
        yf = np.fft.fft(detrended)
        xf = np.fft.fftfreq(len(signal), 1 / self.sample_rate_hz)
        
        positive_freqs = xf[:len(signal)//2]
        magnitudes = np.abs(yf[:len(signal)//2])
        
        peak_idx = np.argmax(magnitudes[1:]) + 1
        peak_freq = positive_freqs[peak_idx]
        return 1 / peak_freq if peak_freq > 0 else 0

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

    def predict_state(self, current_cpu, current_time, window_data=None):
        if not self.is_fitted:
            raise Exception("Model not fitted yet")

        t_current = pd.to_datetime(current_time).timestamp()
        phase = ((t_current - self.start_timestamp) % self.estimated_period) / self.estimated_period
        
        if window_data is not None and len(window_data) >= 3:
            smoothed_cpu = medfilt(window_data, kernel_size=3)[-1]
        else:
            smoothed_cpu = current_cpu
        
        phase_dist = min(abs(phase - self.busy_phase_mid), 1 - abs(phase - self.busy_phase_mid))
        
        in_busy_phase = phase_dist < 0.25 
        
        if in_busy_phase:
            return 1 if smoothed_cpu > (self.threshold * 0.7) else 0
        else:
            return 1 if smoothed_cpu > (self.threshold * 1.3) else 0

if __name__ == "__main__":
    base_path = ".." 
    res_path = f'{base_path}/script/sysstat/resource_profile_data/sample.csv'
    thr_path = f'{base_path}/script/sysstat/throughput_profile_data/sample.csv'

    df1 = pd.read_csv(res_path, header=None, 
                        names=['timestamp', 'cpu', 'mem_percent'])
    df2 = pd.read_csv(thr_path, header=None, 
                        names=['timestamp', 'time', 'size', 'throughput', 'download', 'upload'])

    df1 = df1.dropna().reset_index(drop=True)

    predictor = NodeStatePredictor(sample_rate_hz=1/5.0)
    predictor.fit(df1['cpu'], df1['timestamp'])

    states = []
    cpu_values = df1['cpu'].values
    timestamps = df1['timestamp'].values
    
    for i in range(len(cpu_values)):
        window = cpu_values[max(0, i-2):i+1]
        state = predictor.predict_state(cpu_values[i], timestamps[i], window)
        states.append(state)

    df1['detected_state'] = states