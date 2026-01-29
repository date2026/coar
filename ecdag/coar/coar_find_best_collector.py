import math
import numpy as np

class Node:
    def __init__(self, node_id, download_bw, upload_bw, gf_bw):
        self.id = node_id
        self.download_bw = download_bw  # BW_down
        self.upload_bw = upload_bw      # BW_up
        self.gf_bw = gf_bw              # R_co (Repair Computation Throughput)
        
        self.is_active = False         
        self.L = 0                      

    def __repr__(self):
        return f"Node(id={self.id}, L={self.L}, Active={self.is_active})"

def get_best_fresh_node(current_nodes, k, chunk_size_mb):
    fresh_nodes = [n for n in current_nodes if not n.is_active]
    
    if not fresh_nodes:
        return None, float('inf')

    best_c = max(fresh_nodes, key=lambda n: min(n.gf_bw / k, n.download_bw / (k - 1)))
    
    t_fresh = max(k / best_c.gf_bw, (k - 1) / best_c.download_bw) * chunk_size_mb
    
    return best_c, t_fresh


def schedule_coar_sr(src_node_ids, all_stats, k, chunk_size_mb, alpha, num_failed_chunks, slice_num, coar_node_ids):
    """
    Returns:
        repair_plan: [(chunk_index, node_id, assignment_type), ...]
    """
    
    nodes = []
    raw_download = [all_stats["download_bandwidth"][i - 1] for i in src_node_ids]
    raw_gf = [all_stats["gf_bandwidth"][i - 1] for i in src_node_ids]

    print(f"download bandwidths: {raw_download}")
    print(f"gf bandwidths: {raw_gf}")



    for idx, node_id in enumerate(src_node_ids):
        nodes.append(Node(
            node_id=node_id,
            download_bw=raw_download[idx],
            upload_bw = -1,
            gf_bw=raw_gf[idx]
        ))

    repair_plan = []



    # ---------------------------------------------------------
    # Step 1: Establish a repair plan for the first chunk (Baseline COAR)
    # ---------------------------------------------------------
    for coar_node_id in coar_node_ids:
        nodes[coar_node_id - 1].is_active = True
        nodes[coar_node_id - 1].L += 1

        repair_plan.append({
            'chunk_idx': 0, 
            'node_id': coar_node_id, 
            'type': 'COAR'
        })


    # ---------------------------------------------------------
    # Step 2 & 3: sequent failed chunks
    # ---------------------------------------------------------
    for chunk_i in range(1, num_failed_chunks + 1):
        # get fresh node c from W_bar
        
        fresh_candidates = []
        active_candidates = []

        for n in nodes:
            if not n.is_active:
                bw = n.download_bw if n.download_bw > 0.001 else 0.001
                time_fresh = max(chunk_size_mb * k / n.gf_bw, chunk_size_mb * (k - 1) / n.download_bw)
                fresh_candidates.append({'node': n, 'time': time_fresh})
            else:
                # Reuse time calculation: k * L / (R_co_i * alpha)
                time_reuse = chunk_size_mb * (k * n.L) / (n.gf_bw * alpha)
                active_candidates.append({'node': n, 'time': time_reuse})
        fresh_candidates.sort(key=lambda x: x['time'])        
        best_fresh_time = fresh_candidates[0]['time'] if fresh_candidates else float('inf')
        
        
        active_candidates.sort(key=lambda x: x['time'])        
        

        qualified_active_nodes = []
        if fresh_candidates:
            for item in active_candidates:
                if item['time'] < best_fresh_time:
                    qualified_active_nodes.append(item)
        else:
            qualified_active_nodes = active_candidates[:]

        print(f"qualified active node: {qualified_active_nodes}")

        if len(qualified_active_nodes) >= slice_num:
            for slice_i in range(slice_num):
                n = qualified_active_nodes[slice_i]['node']
                n.L += 1
                repair_plan.append({
                    'chunk_idx': chunk_i,
                    'node_id': n.id,
                    'type': 'Reuse'
                })
        else:
            for slice_i in range(slice_num):
                n = fresh_candidates[slice_i]['node']
                n.L += 1
                n.is_active = True
                repair_plan.append({
                    'chunk_idx': chunk_i,
                    'node_id': n.id,
                    'type': 'Fresh'
                })



    return repair_plan

def get_alpha():
    chunk_num = [2, 3, 4, 5]
    no_reuse = [801.131000, 1189.131000, 1569.483000, 1971.269000]  
    reuse = [500.406000, 613.838000, 675.354000, 783.333000]        
    
    k_no_reuse, b_no_reuse = np.polyfit(chunk_num, no_reuse, 1)
    k_reuse, b_reuse = np.polyfit(chunk_num, reuse, 1)
    return k_no_reuse / k_reuse






if __name__ == "__main__":



    stats = {
        "download_bandwidth": [1368.0, 1262.0, 1315.0, 1268.0, 1474.0, 1315.0, 1368.0, -1,  1452.0, 1474.0, 1421.0, 1441.0],
        "upload_bandwidth": [],
        "gf_bandwidth": [1165.741, 1172.424, 1165.741, 1152.72975, 1439.310625, 1165.741, 1152.72975, -1, 1165.741, 1439.310625, 1172.4248750000002, 1165.741]
    }


    src_ids = [1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12]      # start from 1
    coar_ids = [2, 5, 10, 11]
    K_PARAM = 8
    CHUNK_SIZE = 64                                     # MB
    ALPHA = get_alpha()   
    FAIL_CHUNKS = 4
    COLLECTOR_NUM = 4

    plan = schedule_coar_sr(src_ids, stats, K_PARAM, CHUNK_SIZE, ALPHA, FAIL_CHUNKS, COLLECTOR_NUM, coar_ids)

    print(f"{'Chunk':<6} | {'Node':<5} | {'Type'}")
    print("-" * 40)
    for task in plan:
        print(f"{task['chunk_idx']:<6} | {task['node_id']:<5} | {task['type']}")