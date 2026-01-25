from util import stats
from util import rs
from coar_coarse import plan
from redis import Redis
import subprocess
import logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(pathname)s:%(lineno)d] %(levelname)s  - %(message)s'
)

# collect download, upload bandwidth, gf_bandwidth
# generate a ecdag
# return ecdag to dump to a file
# node_ids start from 1
# src_node_ids includes all surving nodes
def run(filename, failed_node_id, src_node_ids, new_ids, all_node_ids, row_ids, obj_ids, object_size, ec_info, output, w, recorders):
    coor_connect = Redis(host="localhost", port=6379, db=0)

    all_stats = stats.CollectStats(True, False, False, True, True, w, recorders)    # download bandwidth, upload bandwidth, gf bandwidth of all worker nodes
    
    
    # select a nd node as requestor
    # nd = SelectNd(new_ids, object_size, all_stats, download_jobs)
    # failed_node_id = nd

    nd = failed_node_id
    # collect bandwidths for src_node_ids/surving nodes
    download_bandwidths = [all_stats["download_bandwidth"][i - 1] for i in src_node_ids]
    upload_bandwidths = [all_stats["upload_bandwidth"][i - 1] for i in src_node_ids]
    gf_bandwidths = [all_stats["gf_bandwidth"][i - 1] for i in src_node_ids]

    # get bandwidth for failed node/request/nd
    nd_download_bandwidth = all_stats["download_bandwidth"][failed_node_id - 1]
    nd_upload_bandwidth = all_stats["upload_bandwidth"][failed_node_id - 1]
    nd_gf_bandwidth = all_stats["gf_bandwidth"][failed_node_id - 1]

    n, k, matrix = ReadECInfo(ec_info)                                              # read ec info from file
    logging.info(f"run, n: {n}, k: {k}, matrix: {matrix}")
    


    download_tasks, upload_tasks = plan.solve_ilp_problem(len(src_node_ids), k, object_size / 1024 / 1024, \
    download_bandwidths + [nd_download_bandwidth], upload_bandwidths + [nd_upload_bandwidth], gf_bandwidths + [nd_gf_bandwidth])
    logging.info(f"src_node_ids: {src_node_ids}, failed_node_id: {nd}, download_tasks: {download_tasks}, upload_tasks: {upload_tasks}")
    node_download_tasks = {}
    node_upload_tasks = {}
    for i in range(len(src_node_ids)):
        node_id = src_node_ids[i]
        if (download_tasks[i] != 0):
            node_download_tasks[node_id] = download_tasks[i]
        if (upload_tasks[i] != 0):
            node_upload_tasks[node_id] = upload_tasks[i]
    node_download_tasks[failed_node_id] = download_tasks[-1]


    logging.info(f"node_download tasks: {node_download_tasks}, node_upload_tasks: {node_upload_tasks}")

    node_id_2_coefs = rs.GetCoefVector(matrix, all_node_ids, row_ids, node_upload_tasks.keys(), nd, k, 8)


    # generate ecdag according to repair ratio and help ratio
    GenerateECDAG(all_node_ids, obj_ids, row_ids, node_id_2_coefs, node_download_tasks, node_upload_tasks, nd, matrix, n, k, output)  
    
    # exec ec repair
    ExecECDAG(filename, failed_node_id, None, output)
    return 
    
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



"""
generate ecdag according rapair_ratios and help_ratios
for each grain
1. get grain left and right bound
2. get candidate_node_ids
3. get coefs
4. push to output
NOTE: 
all_node_ids, src_node_ids start, nd start from 1
rs func need ids start from 1
ecdag file need ids start from 0
"""
def GenerateECDAGFG(all_node_ids, obj_ids, row_ids, nd, src_node_ids, repair_ratios, help_ratios, matrix, n, k, output):

    repair_slices = [round(r * plan.SLICE_NUM) for r in repair_ratios]
    help_slices = [round(h * plan.SLICE_NUM) for h in help_ratios]

    # repair_slices = [3, 0, 3, 3, 3, 3, 1, 0]
    # help_slices = [5, 8, 5, 5, 5, 5, 7, 8]   

    logging.info(f"src_node_ids: {src_node_ids}, repair slices: {repair_slices}, help slices: {help_slices}")
    ec_plan = generate_repair_allocation(len(src_node_ids), k - 1, repair_slices, help_slices)
    if (ec_plan == None):
        return
    

    global global_temp_obj_id
    global_temp_obj_id = 2047

    result = []
    cur_grain_left_bound = 0
    for grain in ec_plan:
        repair_node_id = src_node_ids[grain[0]]                         # start from 1
        grain_size = grain[2]
        cur_grain_right_bound = cur_grain_left_bound + int(grain_size) - 1

        help_obj_ids = []
        help_node_ids = []
        for select_help in grain[1]:
            help_node_id = src_node_ids[select_help[0]]                 # start from 1
            obj_id = obj_ids[help_node_id - 1]

            # help node FETCH
            result.append(("FETCH", help_node_id - 1, obj_id, global_temp_obj_id, cur_grain_left_bound, cur_grain_right_bound))
            # help node SEND to repair node
            result.append(("SEND", help_node_id - 1, repair_node_id - 1, global_temp_obj_id, cur_grain_left_bound, cur_grain_right_bound))
            # repair node RECEIVE from help node
            result.append(("RECEIVE", repair_node_id - 1, help_node_id - 1, global_temp_obj_id, global_temp_obj_id, cur_grain_left_bound, cur_grain_right_bound))
            
            help_obj_ids.append(global_temp_obj_id)
            help_node_ids.append(help_node_id)
            global_temp_obj_id -= 1
        

        # repair node FETCH
        obj_id = obj_ids[repair_node_id - 1]
        result.append(("FETCH", repair_node_id - 1, obj_id, global_temp_obj_id, cur_grain_left_bound, cur_grain_right_bound))
        help_obj_ids.append(global_temp_obj_id)
        help_node_ids.append(repair_node_id)
        global_temp_obj_id -= 1

        # repair node ENCODE_PARTIAL
        node_id_2_coefs = rs.GetCoefVector(matrix, all_node_ids, row_ids, help_node_ids, nd, k, 8)
        tmp_coefs = []
        for help_node_id in help_node_ids:
            tmp_coefs.append(node_id_2_coefs[help_node_id])
        result.append(("ENCODE_PARTIAL", repair_node_id - 1, len(help_obj_ids), help_obj_ids, global_temp_obj_id, tmp_coefs, cur_grain_left_bound, cur_grain_right_bound))

        # repair node SEND to nd
        result.append(("SEND", repair_node_id - 1, nd - 1, global_temp_obj_id, cur_grain_left_bound, cur_grain_right_bound))
        # nd RECEIVE from repair node
        result.append(("RECEIVE", nd - 1, repair_node_id - 1, global_temp_obj_id, global_temp_obj_id, cur_grain_left_bound, cur_grain_right_bound))
        # nd PERSIST 
        result.append(("PERSIST", nd - 1, global_temp_obj_id, global_temp_obj_id, row_ids[nd - 1], cur_grain_left_bound, cur_grain_right_bound))
        
        global_temp_obj_id -= 1
        cur_grain_left_bound = cur_grain_right_bound + 1
    
    # output to file
    DumpOutput(result, output)
    return 

    
################################################################################################################################################################


def GenerateECDAG(all_node_ids, obj_ids, row_ids, node_id_2_coefs, download_selector, upload_selector, nd, matrix, n, k, output):

    dag = {}
    for i in set(download_selector.keys()).union(set(upload_selector.keys())):
        dag[i] = []
    # init E, do not has unmatched download task, just has upload task(must be 1)
    candidate_nodes = set()
    for item in upload_selector.items():
        node_id = item[0]
        if node_id not in download_selector:
            candidate_nodes.add(node_id)
    
    while True:
        ny = -1
        unmatched_download_tasks = 2**31
        for item in download_selector.items():
            if item[0] == nd:
                continue
            if item[1] < unmatched_download_tasks:
                unmatched_download_tasks = item[1]
                ny = item[0] 
        if ny == -1:                                # all download tasks except nd are matched
            break
        
        nx = candidate_nodes.pop()

        download_selector[ny] -= 1
        if download_selector[ny] == 0:
            del download_selector[ny]
            if ny in upload_selector:
                candidate_nodes.add(ny)
        logging.info(f"GenerateECDAG, {nx}-->{ny}")
        dag[ny].append((nx, ny))

    # for remaining unmatech upload tasks
    while candidate_nodes:
        nx = candidate_nodes.pop()
        logging.info(f"GenerateECDAG, {nx}-->{nd}")
        dag[nd].append((nx, nd))
        
    result = []
    logging.info(f"obj_ids: {obj_ids}, row_ids: {row_ids}, dag: {dag}")
    global global_temp_obj_id
    global_temp_obj_id = 2047


    GetEdge(dag, nd, obj_ids, row_ids, node_id_2_coefs, nd, -1, result)
    logging.info(f"result: {result}")
    DumpOutput(result, output)
    
    return 


def ExecECDAG(filename, failed_node_id, upload_selector, output):
    # nodes_str = [str(x - 1) for x in upload_selector]  
    # nodes_str.append(str(failed_node_id - 1))
    # print(f"node_str{nodes_str}")
    
    ssh_cmd = "ssh node01"
    cmd = "./build/ECClient decode " + filename + " " + output +  " 0 2 3 4087 1"
    print(f"ssh_cmd: {ssh_cmd}, cmd: {cmd}")
    ret = subprocess.getstatusoutput(ssh_cmd + " " + cmd)
    print(f"ret: {ret}")
    return 


# leaf node: FETCH, SEND
# middle node: RECEIVE, FETCH, ENCODE, SEND
# root node:: RECEIVE, ENCODE, PERSIST
def GetEdge(ecdag, nd, obj_ids, row_ids, node_id_2_coefs, node_id, nxt_node_id, result):

    # leaf node, return temp id, row id, obj_id must be a origin obj id
    if len(ecdag.get(node_id)) == 0: 
        # add fetch
        obj_id = obj_ids[node_id - 1]
        coef = node_id_2_coefs[node_id]
        result.append(("FETCH", node_id - 1, obj_id, obj_id))
        # add send      
        result.append(("SEND", node_id - 1, nxt_node_id - 1, obj_id))
        return obj_id, coef
    
    # middle node, return temp id, row_id
    if len(ecdag.get(node_id)) != 0 and node_id != nd:

        tmp_obj_ids = []
        tmp_coefs = []
        
        # add fetch
        tmp_obj_id = obj_ids[node_id - 1]
        tmp_coef = node_id_2_coefs[node_id]
        result.append(("FETCH", node_id - 1, tmp_obj_id, tmp_obj_id))
        tmp_obj_ids.append(tmp_obj_id)
        tmp_coefs.append(tmp_coef)
        
        # add receive
        for line in ecdag[node_id]:                                         # for each pre node
            pre_node_id = line[0]
            tmp_obj_id, tmp_coef = GetEdge(ecdag, nd, obj_ids, row_ids, node_id_2_coefs, pre_node_id, node_id, result)
            tmp_obj_ids.append(tmp_obj_id)
            tmp_coefs.append(tmp_coef)
            result.append(("RECEIVE", node_id - 1, pre_node_id - 1, tmp_obj_id, tmp_obj_id))
        
        # add encode
        global global_temp_obj_id
        result.append(("ENCODE_PARTIAL", node_id - 1, len(tmp_obj_ids), tmp_obj_ids, global_temp_obj_id, tmp_coefs))
        obj_id = global_temp_obj_id
        global_temp_obj_id -= 1
        # add send
        result.append(("SEND", node_id - 1, nxt_node_id - 1, obj_id))
        return obj_id, 1

    # root node
    if ecdag.get(node_id) is not None and node_id == nd:
        tmp_obj_ids = []
        tmp_coefs = []
        # add receive
        for line in ecdag[node_id]:                                         # for each pre node
            pre_node_id = line[0]
            tmp_obj_id, tmp_coef = GetEdge(ecdag, nd, obj_ids, row_ids, node_id_2_coefs, pre_node_id, node_id, result)
            tmp_obj_ids.append(tmp_obj_id)
            tmp_coefs.append(tmp_coef)
            result.append(("RECEIVE", node_id - 1, pre_node_id - 1, tmp_obj_id, tmp_obj_id))
        
        # add encode
        result.append(("ENCODE_PARTIAL", node_id - 1, len(tmp_obj_ids), tmp_obj_ids, global_temp_obj_id, tmp_coefs))
        obj_id = global_temp_obj_id
        global_temp_obj_id -= 1

        # add persist
        result.append(("PERSIST", node_id - 1, obj_id, obj_id, 0))
        return -1, -1
    assert False and "should not reach here"

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

