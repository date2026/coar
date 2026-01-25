from util import stats
from util import rs
from redis import Redis
import subprocess
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(filename)s:%(lineno)d] %(levelname)s  - %(message)s'
)

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
select node just by load avg
"""
# def run(filename, failed_node_id, src_node_ids, new_ids, all_node_ids, row_ids, obj_ids, object_size, ec_info, output):
#     logging.info(f"run heuristic")

#     all_stats = stats.CollectStats(True, False, False, True)                        # download bandwidth and upload bandwidth of each node
#     download_jobs, upload_jobs = stats.CollectJobs()                                # job count of each node
#     download_selector, upload_selector = {}, {}                                     # selected nodes for download and upload

#     # select nd
#     nd = SelectNd(new_ids, object_size, all_stats, download_jobs)
#     failed_node_id = nd
#     src_node_ids = all_node_ids.copy()
#     src_node_ids.remove(nd)
#     download_jobs[nd - 1] += 1
#     download_selector[nd] = download_selector.get(nd, 0) + 1
#     n, k, matrix = ReadECInfo(ec_info)                                              # read ec info from file
#     logging.info(f"run, n: {n}, k: {k}, matrix: {matrix}")
#     # select download node(and corresponding upload node)
#     SelectDownloadNode(src_node_ids, object_size, all_stats, download_jobs, upload_jobs, \
#                        k, nd, download_selector, upload_selector)                   # select download node
#     SelectUploadNode(src_node_ids, object_size, all_stats, upload_jobs, download_jobs, \
#                      upload_selector, k)                                            # select upload node
#     stats.UpdateTasks(download_jobs, upload_jobs)
#     logging.info(f"nd: {nd}")
#     logging.info(f"download_selector: {download_selector}")
#     logging.info(f"upload_selector: {upload_selector}")
#     logging.info(f"download_jobs: {download_jobs}")
#     logging.info(f"upload_jobs: {upload_jobs}")

#     old_download_selector = download_selector.copy()
#     old_upload_selector = upload_selector.copy()

#     node_id_2_coefs = rs.GetCoefVector(matrix, all_node_ids, row_ids, upload_selector.keys(), failed_node_id, k, 8)
#     logging.info(f"node_id_2_coefs: {node_id_2_coefs}")
#     # generate ecdag for this ec repair
#     GenerateECDAG(all_node_ids, obj_ids, row_ids, node_id_2_coefs, download_selector, upload_selector, nd, matrix, n, k, output)
#     # exec ec repair
#     ExecECDAG(filename, failed_node_id, upload_selector, output)
#     stats.ReStoreTasks(old_download_selector, old_upload_selector)
#     return 
    

def SelectNd(node_ids, object_size, stats, download_tasks):
    logging.info(f"SelectNd, source node_ids: {node_ids}, download_tasks: {download_tasks}, load avg: {stats['load_avg']}")
    nd = -1
    min_load_avg = float("inf")
    for node_id in node_ids:
        load_avg = stats["load_avg"][node_id - 1]
        if load_avg < min_load_avg:
            min_load_avg = load_avg
            nd = node_id
    logging.info(f"SelectNd done, selected nd: {nd}")
    return nd

def SelectDownloadNode(node_ids, object_size, stats, download_tasks, upload_tasks, \
                       k, nd, download_selector, upload_selector):
    logging.info(f"SelectDownloadNode start, source node_ids: {node_ids}, with download_tasks: {download_tasks}, upload_tasks: {upload_tasks}, load_avg: {stats['load_avg']}")

    for i in range(k - 1):

        min_load_avg = float("inf")
        select = -1
        for node_id in node_ids:
            if node_id in download_selector:
                continue
            load_avg = stats['load_avg'][node_id - 1]
            if load_avg < min_load_avg:
                min_load_avg = load_avg
                select = node_id

        download_selector[select] = download_selector.get(select, 0) + 1
        download_tasks[select - 1] += 1
        if select != nd:
            upload_tasks[select - 1] += 1
            upload_selector[select] = upload_selector.get(select, 0) + 1


    logging.info(f"SelectDownloadNode done, selected download tasks: {download_tasks}, upload tasks: {upload_tasks}, download selector: {download_selector}, upload selector: {upload_selector}")   

def SelectUploadNode(node_ids, object_size, stats, upload_tasks, download_tasks, upload_selector, k):
    logging.info(f"SelectUploadNode, source node_ids: {node_ids}, upload_tasks: {upload_tasks}, upload_selector: {upload_selector}, load_avg: {stats['load_avg']}")
    remain_upload_tasks_cnt = k - len(upload_selector)
    
    for i in range(remain_upload_tasks_cnt):
        select = -1
        min_load_avg = float("inf")
        for node_id in node_ids:
            if node_id in upload_selector:
                continue
            
            load_avg = stats['load_avg'][node_id - 1]
            if load_avg < min_load_avg:
                min_load_avg = load_avg
                select = node_id
        upload_selector[select] = upload_selector.get(select, 0) + 1
        upload_tasks[select - 1] += 1

    logging.info(f"SelectUploadNode done, selected download tasks: {download_tasks}, upload tasks: {upload_tasks}")

"""
select node by load avg, group node to low, middle, high load
"""
def run(filename, failed_node_id, src_node_ids, new_ids, all_node_ids, row_ids, obj_ids, object_size, ec_info, output):
    logging.info(f"run heuristic")

    all_stats = stats.CollectStats(True, False, False, True)                        # download bandwidth and upload bandwidth of each node
    download_jobs, upload_jobs = stats.CollectJobs()                                # job count of each node
    download_selector, upload_selector = {}, {}                                     # selected nodes for download and upload
    n, k, matrix = ReadECInfo(ec_info)                                              # read ec info from file


    """
    group node to low, middle, high load
    """
    load_avgs = all_stats["load_avg"]
    logging.info(f"load_avgs: {load_avgs}")
    low_load_nodes, middle_load_nodes, high_load_nodes = ClassifyNode(all_node_ids, load_avgs)

    # SelectNodeAndExecECPipe(all_node_ids, all_stats["load_avg"], k, matrix, all_node_ids, obj_ids, row_ids, output)
    # SelectNodeAndExecPPR(low_load_nodes, middle_load_nodes, load_avgs, k, matrix, all_node_ids, obj_ids, row_ids, output)
    logging.info(f"low load nodes: {low_load_nodes}, middle load nodes: {middle_load_nodes}, high load nodes: {high_load_nodes}")
    if len(low_load_nodes) != 0:
        if len(low_load_nodes) >= k + 1:                            # use low load node, ecpipe
            logging.info(f"low load node is higher than k + 1, all use low load node, ecpipe")
            SelectNodeAndExecECPipe(low_load_nodes, load_avgs, k, matrix, all_node_ids, obj_ids, row_ids, output)
        elif len(low_load_nodes) + len(middle_load_nodes) >= k + 1: # use low load node and middle load node, ppr      
            logging.info(f"low load node is not enouth k + 1, use low load node and middle load node, ppr")
            SelectNodeAndExecPPR(low_load_nodes, middle_load_nodes, load_avgs, k, matrix, all_node_ids, obj_ids, row_ids, output)
        else:                                                       # low load node and middle load node is not enough, wait
            logging.info(f"low load node and middle load node is not enough, wait")
            return
    elif len(middle_load_nodes) >= k + 1:                               # use middle load node, ecpipe
        logging.info(f"low load nodes is 0, middle load node is enough k + 1, use middle load node, ecpipe")
        SelectNodeAndExecECPipe(middle_load_nodes, load_avgs, k, matrix, all_node_ids, obj_ids, row_ids, output)
    else:
        logging.info(f"all high load node, wait")
        return

    ExecECDAG(filename, -1, [], output)
    # stats.ReStoreTasks(old_download_selector, old_upload_selector)
    return 


def ClassifyNode(node_ids, load_avgs):
    low_load_nodes = []
    middle_load_nodes = []
    high_load_nodes = []
    # low_threshold = 0.7 * sum(load_avgs) / len(load_avgs)
    # high_threshold = 1.2 * sum(load_avgs) / len(load_avgs)
    low_threshold = 1.0
    high_threshold = 2.0
    for node_id in node_ids:
        load_avg = load_avgs[node_id - 1]
        if load_avg < low_threshold:
            low_load_nodes.append(node_id)
        elif load_avg > high_threshold:
            high_load_nodes.append(node_id)
        else:
            middle_load_nodes.append(node_id)
    return low_load_nodes, middle_load_nodes, high_load_nodes



"""
k-1 middle node
1 root node
1 leaf node
"""
def SelectNodeAndExecECPipe(candidate_nodes, load_avgs, k, matrix, all_node_ids, obj_ids, row_ids, output):
    logging.info(f"SelectNodeAndExecECPipe, candidate_nodes: {candidate_nodes}, load_avgs: {load_avgs}")
    middle_nodes = []
    root_node_id = -1
    leaf_node_id = -1
    # select k-1 middle node
    for i in range(k - 1):
        select = -1
        min_load_avg = float("inf")
        for node_id in candidate_nodes:
            if node_id in middle_nodes:
                continue

            load_avg = load_avgs[node_id - 1]
            if load_avg < min_load_avg:
                min_load_avg = load_avg
                select = node_id
        middle_nodes.append(select)
    logging.info(f"middle_nodes: {middle_nodes}")
    
    # select root node
    min_load_avg = float("inf")
    for node_id in candidate_nodes:
        if node_id in middle_nodes:
            continue
        if load_avgs[node_id - 1] < min_load_avg:
            min_load_avg = load_avgs[node_id - 1]
            root_node_id = node_id
    logging.info(f"root_node_id: {root_node_id}")
    
    # select leaf node
    min_load_avg = float("inf")
    for node_id in candidate_nodes:
        if node_id in middle_nodes or node_id == root_node_id:
            continue
        if load_avgs[node_id - 1] < min_load_avg:
            min_load_avg = load_avgs[node_id - 1]
            leaf_node_id = node_id
    logging.info(f"leaf_node_id: {leaf_node_id}")
    
    # generate ecpipe ecdag
    sorted_node_ids = sorted(middle_nodes, key = lambda x: load_avgs[x - 1], reverse = True)
    logging.info(f"sorted_node_ids: {sorted_node_ids}")
    
    dag = {}
    for node_id in all_node_ids:
        dag[node_id] = []
    dag[sorted_node_ids[0]].append((leaf_node_id, sorted_node_ids[0]))
    logging.info(f"add edge: {leaf_node_id}->{sorted_node_ids[0]}")
    for i in range(1, len(sorted_node_ids)):
        nx = sorted_node_ids[i - 1]
        ny = sorted_node_ids[i]
        dag[ny].append((nx, ny))
        logging.info(f"add edge: {nx}->{ny}")
    dag[root_node_id].append((sorted_node_ids[-1], root_node_id))
    logging.info(f"add edge: {sorted_node_ids[-1]}->{root_node_id}")
    
    result = []
    global global_temp_obj_id
    global_temp_obj_id = 2047

    select_node_ids = middle_nodes
    select_node_ids.append(leaf_node_id)
    node_id_2_coefs = rs.GetCoefVector(matrix, all_node_ids, row_ids, select_node_ids, root_node_id, k, 8)
    GetEdge(dag, root_node_id, obj_ids, row_ids, node_id_2_coefs, root_node_id, -1, result)
    DumpOutput(result, output)
    
    return 


def SelectNodeAndExecPPR(low_candidates, middle_candidates, load_avgs, k, matrix, all_node_ids, obj_ids, row_ids, output):
    logging.info(f"SelectNodeAndExecPPR, low_candidates: {low_candidates}, middle_candidates: {middle_candidates}, load_avgs: {load_avgs}")
    middle_nodes = []
    leaf_nodes = []
    root_node_id = -1
    dag = {}
    for node_id in all_node_ids:
        dag[node_id] = []
    select_node_ids = []
    result = []
    global global_temp_obj_id
    global_temp_obj_id = 2047


    # low load node shoule be middle node or root node
    middle_nodes = low_candidates
    middle_nodes.sort(key = lambda x: load_avgs[x - 1], reverse = True)
    # select root node
    root_node_id = middle_nodes[0]
    logging.info(f"root_node_id: {root_node_id}")
    # select middle nodes
    middle_nodes.remove(root_node_id)
    logging.info(f"middle_nodes: {middle_nodes}")

    if len(middle_nodes) < k:                       # middle load node is not enough, select leaf nodes from middle load node
        for i in range(k - len(middle_nodes)):
            min_load_avg = float("inf")
            select = -1
            for node_id in middle_candidates:
                if node_id in leaf_nodes:
                    continue
                load_avg = load_avgs[node_id - 1]
                if load_avg < min_load_avg:
                    min_load_avg = load_avg
                    select = node_id
            leaf_nodes.append(select)
        logging.info(f"leaf_nodes: {leaf_nodes}")


        # add middle node to root node
        target_queue = []
        target_queue.append(root_node_id)
        target_queue.append(root_node_id)
        for node_id in middle_nodes:
            target_node_id = target_queue.pop(0)
            dag[target_node_id].append((node_id, target_node_id))
            logging.info(f"add edge: {node_id}->{target_node_id}")
            target_queue.append(node_id)
            target_queue.append(node_id)
        middle_node_num = len(middle_nodes)

        # add leaf node to middle node
        for i, node_id in enumerate(leaf_nodes):
            target_node_id = middle_nodes[i % (middle_node_num + 1)] if i % (middle_node_num + 1) < middle_node_num else root_node_id
            dag[target_node_id].append((node_id, target_node_id))
            logging.info(f"add edge: {node_id}->{target_node_id}")
        logging.info(f"dag: {dag}")
        select_node_ids = middle_nodes + leaf_nodes

    else:                                           # middle load node is enough
        target_queue = []
        target_queue.append(root_node_id)
        target_queue.append(root_node_id)
        for i in range(k):
            node_id = middle_nodes[i]
            select_node_ids.append(node_id)
            target_node_id = target_queue.pop(0)
            dag[target_node_id].append((node_id, target_node_id))
            logging.info(f"add edge: {node_id}->{target_node_id}")
            target_queue.append(node_id)
            target_queue.append(node_id)

    node_id_2_coefs = rs.GetCoefVector(matrix, all_node_ids, row_ids, select_node_ids, root_node_id, k, 8)
    GetEdge(dag, root_node_id, obj_ids, row_ids, node_id_2_coefs, root_node_id, -1, result)
    DumpOutput(result, output)
    
    return 


def GenerateECDAG(all_node_ids, obj_ids, row_ids, node_id_2_coefs, download_selector, upload_selector, nd, matrix, n, k, output):
    logging.info(f"download_selector: {download_selector}, upload_selector: {upload_selector}")

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
        # select ny to download from nx
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
    # output to file
    DumpOutput(result, output)
    
    return 


def ExecECDAG(filename, failed_node_id, upload_selector, output):
    
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