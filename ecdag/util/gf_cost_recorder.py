import json
import time
from redis import Redis
import logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(filename)s:%(lineno)d] %(levelname)s  - %(message)s'
)


class GFCostRecorder:
    def __init__(self, cpu_similarity_threshold = 5.0):

        self.records = []                   # Storage for historical records
        self.cpu_threshold = cpu_similarity_threshold   # Threshold for CPU similarity
    
    def record(self, cpu_usage, w, blocks, block_size, encode_decode_time):
        """
        Record a new GF operation
        """

        record = {
            "cpu_usage": cpu_usage,
            "blocks": blocks,
            "block_size": block_size,
            "w": w,
            "time": encode_decode_time
        }
        self.records.append(record)

    def predict(self, cpu_usage, w):
        '''
        return gf bandwidth (MB/s)
        '''
        logging.info(f"predict, current cpu_usage: {cpu_usage}, current records: {self.records}")
        gf_bws = []
        for record in self.records:
            cpu_diff = abs(record["cpu_usage"] - cpu_usage)
            if cpu_diff <= self.cpu_threshold and record["w"] == w:
                gf_bws.append((record["blocks"] * record["block_size"]) / (record["time"] / 1000.0))
                logging.info(f"hit, current_cpu_usage: {cpu_usage}, w: {w}, history_cpu_usage: {record['cpu_usage']}, blocks: {record['blocks']}, block_size: {record['block_size']}, time: {record['time']}, gf_bw: {gf_bws[-1]}")
        
        if gf_bws:
            return sum(gf_bws) / len(gf_bws)
        
        return 10240.0

class GFCostRecorderArray:
    def __init__(self, node_num, cpu_threshold):
        self.node_num = node_num
        self.gf_cost_recorders = recorders = [GFCostRecorder(cpu_similarity_threshold=cpu_threshold) for _ in range(node_num)]
    
    def GetGFBandwidths(self, cpu_usages, w):

        assert(len(cpu_usages) == self.node_num)
        gf_bandwidths = []
        for node_id in range(self.node_num):
            gf_bandwidths.append(self.gf_cost_recorders[node_id].predict(cpu_usages[node_id], w))
            logging.info(f"predict gf bw for node: {node_id}: {gf_bandwidths[-1]} MB/s")
        return gf_bandwidths

    def RecordGFOverhead(self):

        with open("/root/coar/conf/1.json") as f:
            conf = json.load(f)
        local_ip = conf["local_ip"]
        
        redis_connect = Redis(host=local_ip, port=6379, db=0)
        
        queue_name = "gf_overhead_queue"
        
        while True:
            result = redis_connect.blpop(queue_name, timeout=0)
            assert(result)
            # node_id:cpu_usage:w:blocks:block_size:time
            data_str = result[1].decode('utf-8')
            parts = data_str.split(':')
            assert(len(parts) == 6)
            node_id = int(parts[0])
            cpu_usage = float(parts[1])
            w = int(parts[2])
            blocks = int(parts[3])
            block_size = int(parts[4])
            time = float(parts[5])
            
            assert(node_id >= 0 and node_id <= self.node_num - 1)
            self.gf_cost_recorders[node_id].record(cpu_usage, w, blocks, block_size, time)
            logging.info(f"node_id: {node_id}, cpu_usage: {cpu_usage}, w: {w}, blocks: {blocks}, block_size: {block_size}, time: {time}")

        return 