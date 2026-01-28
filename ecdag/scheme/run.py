from util import stats
from util import rs
from redis import Redis
import subprocess
import logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(filename)s:%(lineno)d] %(levelname)s  - %(message)s'
)

def run_cr(filename, failed_node_id, src_node_ids, new_ids, all_node_ids, row_ids, obj_ids, object_size, ec_info, output):
    all_stats = stats.CollectStats(True, False, False, True)
    download_jobs, upload_jobs = stats.CollectJobs()
    n, k, matrix = ReadECInfo(ec_info)

    nd = -1
    min_download_time = float("inf")
    for nid in all_node_ids:
        est_time = (download_jobs[nid-1] + k) * object_size / all_stats["download_bandwidth"][nid-1]
        if est_time < min_download_time:
            min_download_time = est_time
            nd = nid

    potential_srcs = [nid for nid in all_node_ids if nid != nd]
    potential_srcs.sort(key=lambda nid: (upload_jobs[nid-1] + 1) * object_size / all_stats["upload_bandwidth"][nid-1])
    selected_srcs = potential_srcs[:k]

    node_id_2_coefs = rs.GetCoefVector(matrix, all_node_ids, row_ids, selected_srcs, nd, k, 8)
    
    download_selector = {nd: k}
    upload_selector = {nid: 1 for nid in selected_srcs}
    
    result = []
    tmp_obj_ids, tmp_coefs = [], []
    for sid in selected_srcs:
        oid, coef = obj_ids[sid-1], node_id_2_coefs[sid]
        result.append(("FETCH", sid - 1, oid, oid))
        result.append(("SEND", sid - 1, nd - 1, oid))
        result.append(("RECEIVE", nd - 1, sid - 1, oid, oid))
        tmp_obj_ids.append(oid); tmp_coefs.append(coef)

    global_temp_obj_id = 2047
    result.append(("ENCODE_PARTIAL", nd - 1, k, tmp_obj_ids, global_temp_obj_id, tmp_coefs))
    result.append(("PERSIST", nd - 1, global_temp_obj_id, global_temp_obj_id, 0))

    DumpOutput(result, output)
    ExecECDAG(filename, nd, upload_selector, output)
    return


def run_ppr(filename, failed_node_id, src_node_ids, new_ids, all_node_ids, row_ids, obj_ids, object_size, ec_info, output):
    all_stats = stats.CollectStats(True, False, False, True)
    download_jobs, upload_jobs = stats.CollectJobs()
    n, k, matrix = ReadECInfo(ec_info)

    nd = min(all_node_ids, key=lambda nid: (download_jobs[nid-1] + 1) * object_size / all_stats["download_bandwidth"][nid-1])
    
    src_candidates = [nid for nid in all_node_ids if nid != nd]
    def score_node(nid):
        up = all_stats["upload_bandwidth"][nid-1] / (upload_jobs[nid-1] + 1)
        dl = all_stats["download_bandwidth"][nid-1] / (download_jobs[nid-1] + 1)
        return min(up, dl)

    src_candidates.sort(key=score_node, reverse=True)
    selected_srcs = src_candidates[:k]
    selected_srcs.sort(key=score_node) 
    
    node_id_2_coefs = rs.GetCoefVector(matrix, all_node_ids, row_ids, selected_srcs, nd, k, 8)

    result = []
    for sid in selected_srcs:
        result.append(("FETCH", sid - 1, obj_ids[sid-1], obj_ids[sid-1]))

    tree_queue = [(nid, obj_ids[nid-1], node_id_2_coefs[nid]) for nid in selected_srcs]
    global_temp_obj_id = 2047

    while len(tree_queue) > 1:
        tree_queue.sort(key=lambda x: score_node(x[0]))
        
        next_level = []
        half = len(tree_queue) // 2
        for i in range(half):
            (n_src, o_src, c_src) = tree_queue[i]
            (n_dst, o_dst, c_dst) = tree_queue[-(i+1)]
            
            result.append(("SEND", n_src-1, n_dst-1, o_src))
            result.append(("RECEIVE", n_dst-1, n_src-1, o_src, o_src))
            
            new_tmp_id = global_temp_obj_id
            global_temp_obj_id -= 1
            result.append(("ENCODE_PARTIAL", n_dst-1, 2, [o_src, o_dst], new_tmp_id, [c_src, c_dst]))
            
            next_level.append((n_dst, new_tmp_id, 1))
        
        if len(tree_queue) % 2 != 0:
            next_level.append(tree_queue[half])
            
        tree_queue = next_level

    last_node, last_obj, _ = tree_queue[0]
    if last_node != nd:
        result.append(("SEND", last_node-1, nd-1, last_obj))
        result.append(("RECEIVE", nd-1, last_node-1, last_obj, last_obj))
    result.append(("PERSIST", nd-1, last_obj, last_obj, nd - 1))

    DumpOutput(result, output)
    ExecECDAG(filename, nd, {n:1 for n in selected_srcs}, output)




def run_rp(filename, failed_node_id, src_node_ids, new_ids, all_node_ids, row_ids, obj_ids, object_size, ec_info, output):
    all_stats = stats.CollectStats(True, False, False, True)
    download_jobs, upload_jobs = stats.CollectJobs()
    n, k, matrix = ReadECInfo(ec_info)

    nd = min(all_node_ids, key=lambda n: (download_jobs[n-1]+1)*object_size/all_stats["download_bandwidth"][n-1])
    
    src_candidates = [nid for nid in all_node_ids if nid != nd]
    # src_candidates.sort(key=lambda n: (all_stats["upload_bandwidth"][n-1] + all_stats["download_bandwidth"][n-1]), reverse=True)
    src_candidates.sort(key=lambda n: (min(all_stats["upload_bandwidth"][n-1], all_stats["download_bandwidth"][n-1])), reverse=True)
    pipe_nodes = src_candidates[:k]
    pipe_nodes.reverse()
    node_id_2_coefs = rs.GetCoefVector(matrix, all_node_ids, row_ids, pipe_nodes, nd, k, 8)

    result = []
    global_temp_obj_id = 2047
    prev_oid, prev_nid = -1, -1

    for i, curr_nid in enumerate(pipe_nodes):
        curr_oid = obj_ids[curr_nid-1]
        coef = node_id_2_coefs[curr_nid]
        result.append(("FETCH", curr_nid-1, curr_oid, curr_oid))
        
        if i == 0:
            nxt_nid = pipe_nodes[i+1]
            result.append(("SEND", curr_nid-1, nxt_nid-1, curr_oid))
            prev_oid, prev_nid = curr_oid, curr_nid
        else:
            result.append(("RECEIVE", curr_nid-1, prev_nid-1, prev_oid, prev_oid))
            new_id = global_temp_obj_id; global_temp_obj_id -= 1
            result.append(("ENCODE_PARTIAL", curr_nid-1, 2, [prev_oid, curr_oid], new_id, [1, coef]))
            
            target = pipe_nodes[i+1] if i < k-1 else nd
            result.append(("SEND", curr_nid-1, target-1, new_id))
            prev_oid, prev_nid = new_id, curr_nid

    result.append(("RECEIVE", nd-1, pipe_nodes[-1]-1, prev_oid, prev_oid))
    result.append(("PERSIST", nd-1, prev_oid, prev_oid, 0))

    DumpOutput(result, output)
    ExecECDAG(filename, nd, {n:1 for n in pipe_nodes}, output)


  
def ReadECInfo(ec_info):
    with open(ec_info, 'r') as file:
        first_line = file.readline().strip().split()
        n = int(first_line[0])
        k = int(first_line[1])

        matrix = []
        for _ in range(n):
            line = file.readline().strip().split()
            row = [int(x) for x in line]
            assert len(row) == k
            matrix.append(row)
        assert len(matrix) == n
    return n, k, matrix


def DumpOutput(result, output):

    with open(output, 'w') as file:
        for item in result:
            line_parts = []
            for part in item:
                if isinstance(part, list):
                    line_parts.append(' '.join(map(str, part)))
                else:
                    line_parts.append(str(part))
            file.write(' '.join(line_parts) + '\n')
    return 


def ExecECDAG(filename, failed_node_id, upload_selector, output):
    # nodes_str = [str(x - 1) for x in upload_selector]  
    # nodes_str.append(str(failed_node_id - 1))
    # print(f"node_str{nodes_str}")
    
    ssh_cmd = "ssh node01"
    cmd = "/root/lmq_openec/build/ECClient decode " + filename + " " + output +  " 0 2 3 4087 1"
    print(f"ssh_cmd: {ssh_cmd}, cmd: {cmd}")
    ret = subprocess.getstatusoutput(ssh_cmd + " " + cmd)
    print(f"ret: {ret}")
    return 