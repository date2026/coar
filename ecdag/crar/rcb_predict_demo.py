import pandas as pd
import numpy as np
import xgboost as xgb
import matplotlib.pyplot as plt
from io import StringIO
from scipy.fft import fft, fftfreq

df1 = pd.read_csv('/root/lmq_openec/script/sysstat/resource_profile_data/node01.csv', header=None, names=['timestamp', 'cpu', 'mem_percent'])

df2 = pd.read_csv('/root/lmq_openec/script/sysstat/throughput_profile_data/node01.csv', header=None, names=['timestamp', 'time', 'size', 'throughput', 'download', 'upload'])



# 转换时间戳格式
df1['timestamp'] = pd.to_datetime(df1['timestamp'])
df2['timestamp'] = pd.to_datetime(df2['timestamp'])

# **合并数据**：基于时间戳合并 (Inner Join确保时间对齐)
df = pd.merge(df1, df2[['timestamp', 'throughput']], on='timestamp', how='inner')

print(f"数据加载完成，总行数: {len(df)}")

df['elapsed_seconds'] = (df['timestamp'] - df['timestamp'].iloc[0]).dt.total_seconds()


def find_period(signal, sample_rate_hz):
    # 去掉直流分量
    signal = signal - np.mean(signal)
    yf = fft(signal)
    xf = fftfreq(len(signal), 1 / sample_rate_hz)
    
    # 找到正频率部分最强的峰值
    positive_freqs = xf[:len(signal)//2]
    magnitudes = np.abs(yf[:len(signal)//2])
    
    # 忽略接近0的低频
    peak_idx = np.argmax(magnitudes[1:]) + 1
    peak_freq = positive_freqs[peak_idx]
    
    return 1 / peak_freq if peak_freq > 0 else 0

# 采样间隔是5秒，所以采样率是 1/5 Hz
estimated_period = find_period(df['cpu'].values, 1/5.0)
print(f"估算的负载周期: {estimated_period:.2f} 秒 (如果这个值不准，请手动在代码中修改)")

# *** 如果估算不准，请取消下面这行的注释并手动填入真实周期 ***
# estimated_period = 30.0  # 例如您的负载每30秒循环一次

# --- 构造相位特征 ---
# 相位 = (当前时间 % 周期) / 周期
df['phase_raw'] = df['elapsed_seconds'] % estimated_period

# Sin/Cos 编码 (处理周期的首尾衔接)
df['phase_sin'] = np.sin(2 * np.pi * df['phase_raw'] / estimated_period)
df['phase_cos'] = np.cos(2 * np.pi * df['phase_raw'] / estimated_period)

# --- 构造其他特征 ---
# 滞后特征：添加上一时刻的 CPU 和 吞吐量作为特征
df['cpu_lag1'] = df['cpu'].shift(1)
df['throughput_lag1'] = df['throughput'].shift(1)

# 去除因 shift 产生的第一行空值
df = df.dropna().reset_index(drop=True)

# ==========================================
# 3. 划分训练集与测试集 (Train/Test Split)
# ==========================================
train_size = 25
# 确保数据没有越界（因为drop了一行，可能变成99行）
cutoff = min(train_size, len(df) - (100 - train_size))
train_df = df.iloc[:cutoff]
test_df = df.iloc[cutoff:]

# 定义输入特征 (X) 和 目标变量 (y)
# feature_cols = ['cpu', 'mem_percent', 'phase_sin', 'phase_cos', 'cpu_lag1', 'throughput_lag1']
# feature_cols = ['cpu', 'mem_percent', 'cpu_lag1', 'throughput_lag1']

feature_cols = ['cpu', 'cpu_lag1', 'throughput_lag1']         # 265.06


target_col = 'throughput'

X_train = train_df[feature_cols]
y_train = train_df[target_col]
X_test = test_df[feature_cols]
y_test = test_df[target_col]

# ==========================================
# 4. 模型训练 (XGBoost)
# ==========================================
model = xgb.XGBRegressor(
    n_estimators=100,      # 树的数量
    max_depth=3,           # 树深设小一点，防止过拟合（因为只有80个样本）
    learning_rate=0.1,
    objective='reg:squarederror',
    random_state=42
)

model.fit(X_train, y_train)

# ==========================================
# 5. 预测与评估 (Prediction & Evaluation)
# ==========================================
# 注意：这里做的是 One-step ahead 预测
# 即预测第81个点时，我们使用了第80个点的真实lag数据。
# 预测第82个点时，使用了第81个点的真实lag数据。
y_pred = model.predict(X_test)

# 计算误差
mae = np.mean(np.abs(y_test - y_pred))
print(f"平均绝对误差 (MAE): {mae:.2f}")


plt.figure(figsize=(12, 6))

plt.plot(test_df['timestamp'], y_test, 'b-o', label='Actual Throughput', markersize=4)

plt.plot(test_df['timestamp'], y_pred, 'r--x', label='Predicted Throughput', markersize=6)

plt.ylabel('estimated repair computation throughput (MB/s)')
plt.xlabel('time step')
plt.xticks(rotation=45)
plt.legend()
plt.tight_layout()
plt.show()
plt.savefig("predict_result.pdf", dpi=300, bbox_inches='tight')