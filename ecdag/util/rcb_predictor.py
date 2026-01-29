import pandas as pd
import numpy as np
import xgboost as xgb
from scipy.fft import fft, fftfreq
import threading
from concurrent.futures import ThreadPoolExecutor 



class GBDTThroughputPredictor:
    def __init__(self, sample_rate_hz=1/5.0, n_estimators=100, max_depth=3):
        self.sample_rate_hz = sample_rate_hz
        self.model = xgb.XGBRegressor(
            n_estimators=n_estimators,
            max_depth=max_depth,
            learning_rate=0.1,
            objective='reg:squarederror',
            random_state=42
        )


        # self.feature_cols = ['cpu', 'cpu_lag1', 'throughput_lag1']
        self.feature_cols = ['cpu']
        self.target_col = 'throughput'
        self.is_fitted = False
        self.estimated_period = None
        self.first_timestamp = None

    def _find_period(self, signal):
        signal = signal - np.mean(signal)
        yf = fft(signal)
        xf = fftfreq(len(signal), 1 / self.sample_rate_hz)
        
        positive_freqs = xf[:len(signal)//2]
        magnitudes = np.abs(yf[:len(signal)//2])
        
        peak_idx = np.argmax(magnitudes[1:]) + 1
        peak_freq = positive_freqs[peak_idx]
        return 1 / peak_freq if peak_freq > 0 else 0

    def prepare_data(self, df_res, df_thr, manual_period=None):
        df_res.loc[:, 'timestamp'] = pd.to_datetime(df_res['timestamp'])
        df_thr.loc[:, 'timestamp'] = pd.to_datetime(df_thr['timestamp'])
        
        df = pd.merge(df_res, df_thr[['timestamp', 'throughput']], on='timestamp', how='inner')
        df = df.sort_values('timestamp').reset_index(drop=True)
        
        self.first_timestamp = df['timestamp'].iloc[0]
        
        if manual_period:
            self.estimated_period = manual_period
        else:
            self.estimated_period = self._find_period(df['cpu'].values)
        
        df['cpu_lag1'] = df['cpu'].shift(1)
        df['throughput_lag1'] = df['throughput'].shift(1)
        
        return df.dropna().reset_index(drop=True)

    def train(self, df_res, df_thr, train_size=25, manual_period=None):
        df = self.prepare_data(df_res, df_thr, manual_period)
        
        train_df = df.iloc[:train_size]
        X = train_df[self.feature_cols]
        y = train_df[self.target_col]
        
        self.model.fit(X, y)
        self.is_fitted = True


    def predict(self, current_cpu, last_cpu = None, last_throughput = None):
        """
        :param current_cpu: current cpu
        :param last_cpu: cpu_lag1
        :param last_throughput: throughput_lag1
        """
        if not self.is_fitted:
            raise Exception("not train")

        input_data = pd.DataFrame([{
            'cpu': current_cpu,
            'cpu_lag1': last_cpu,
            'throughput_lag1': last_throughput
        }])
        
        X = input_data[self.feature_cols]
        return self.model.predict(X)[0]


class GFCostRecorderArray:
    def __init__(self, node_num, cpu_threshold = 0.0):
        self.node_num = node_num
        self.gf_cost_recorders =  [GBDTThroughputPredictor() for _ in range(node_num)]
        self.train_multi_thread()


    def _train_single_node(self, node_idx):
        try:
            df1 = pd.read_csv(f'[your path]/script/sysstat/resource_profile_data/sample.csv', 
                      header=None, names=['timestamp', 'cpu', 'mem_percent'])
            df2 = pd.read_csv(f'[your path]/script/sysstat/throughput_profile_data/sample.csv', 
                      header=None, names=['timestamp', 'time', 'size', 'throughput', 'download', 'upload'])

            data = pd.concat([df1[['cpu']], df2[['throughput']]], axis=1)
            data['prev_cpu'] = data['cpu'].shift(1)
            data['prev_throughput'] = data['throughput'].shift(1)
            data = data.dropna().reset_index(drop=True)

            TEST_SIZE = 0
            split_index = len(data) - TEST_SIZE

            df1_train = df1.iloc[:split_index]
            df2_train = df2.iloc[:split_index]
            self.gf_cost_recorders[node_idx].train(df1_train, df2_train, train_size=split_index)
            
        except Exception as e:
            print(f"node{node_idx+1} train fail: {str(e)}")

    def train_multi_thread(self):
        with ThreadPoolExecutor(max_workers=self.node_num) as executor:
            futures = [executor.submit(self._train_single_node, i) for i in range(self.node_num)]
            
            for idx, future in enumerate(futures):
                future.result()  
                

    def _predict_single_node(self, node_id, cpu_usage):
        try:
            return self.gf_cost_recorders[node_id].predict(cpu_usage) / 1000
        except Exception as e:
            print(f"node{node_id+1} predict fail: {str(e)}")
            return 0.0


    def GetGFBandwidths(self, cpu_usages, w):

        assert(len(cpu_usages) == self.node_num)
        gf_bandwidths = []
        # for node_id in range(self.node_num):
        #     gf_bandwidths.append(self.gf_cost_recorders[node_id].predict(cpu_usages[node_id]) / 1000)
            # gf_bandwidths.append(self.gf_cost_recorders[node_id].predict(cpu_usages[node_id]) * 125) # Gbps-->MB/s

        node_num = self.node_num
        # node_num = 12
        with ThreadPoolExecutor(max_workers=node_num) as executor:
            futures = [
                executor.submit(self._predict_single_node, node_id, cpu_usages[node_id])
                for node_id in range(node_num)
            ]
            
            gf_bandwidths = [future.result() for future in futures]
        return gf_bandwidths                        # MB/s

    def RecordGFOverhead(self):
        return 



if __name__ == "__main__":
    df1 = pd.read_csv('./script/sysstat/resource_profile_data/node06.csv', 
                      header=None, names=['timestamp', 'cpu', 'mem_percent'])
    df2 = pd.read_csv('./script/sysstat/throughput_profile_data/node06.csv', 
                      header=None, names=['timestamp', 'time', 'size', 'throughput', 'download', 'upload'])


    data = pd.concat([df1[['cpu']], df2[['throughput']]], axis=1)
    
    data['prev_cpu'] = data['cpu'].shift(1)
    data['prev_throughput'] = data['throughput'].shift(1)
    
    data = data.dropna().reset_index(drop=True)
    

    TEST_SIZE = 70
    

    split_index = len(data) - TEST_SIZE
    

    df1_train = df1.iloc[:split_index]
    df2_train = df2.iloc[:split_index]
    
    test_data = data.iloc[split_index:]


    predictor = GBDTThroughputPredictor()
    predictor.train(df1_train, df2_train, train_size=split_index)


    
    actuals = []
    predictions = []
    
    for index, row in test_data.iterrows():
        curr_cpu = row['cpu']
        prev_cpu = row['prev_cpu']
        prev_throughput = row['prev_throughput']
        
        actual_throughput = row['throughput']
        
        # predict
        pred = predictor.predict(curr_cpu, None, None)
        
        actuals.append(actual_throughput)
        predictions.append(pred)

    # -------------------------------------------------------------------------
    # test
    # -------------------------------------------------------------------------
    actuals = np.array(actuals)
    predictions = np.array(predictions)
    
    epsilon = 1e-6 
    mae = np.mean(np.abs(actuals - predictions))
    mape = np.mean(np.abs((actuals - predictions) / (actuals + epsilon))) * 100
    print(f"MAPE, train node06[0:30], test node06[30:70]: {mape}")



    # test with proflie from node05
    df1 = pd.read_csv('./script/sysstat/resource_profile_data/node05.csv', 
                        header=None, names=['timestamp', 'cpu', 'mem_percent'])
    df2 = pd.read_csv('./script/sysstat/throughput_profile_data/node05.csv', 
                        header=None, names=['timestamp', 'time', 'size', 'throughput', 'download', 'upload'])

    data = pd.concat([df1[['cpu']], df2[['throughput']]], axis=1)
    data['prev_cpu'] = data['cpu'].shift(1)
    data['prev_throughput'] = data['throughput'].shift(1)
    data = data.dropna().reset_index(drop=True)


    actuals = []
    predictions = []
    
    for index, row in data.iterrows():
        curr_cpu = row['cpu']
        
        actual_throughput = row['throughput']
        

        pred = predictor.predict(curr_cpu, None, None)
        
        actuals.append(actual_throughput)
        predictions.append(pred)

    actuals = np.array(actuals)
    predictions = np.array(predictions)
    
    epsilon = 1e-6 
    mae = np.mean(np.abs(actuals - predictions))
    mape = np.mean(np.abs((actuals - predictions) / (actuals + epsilon))) * 100
    print(f"MAPE, train node06[0:30], test node05[0:100]: {mape}")
