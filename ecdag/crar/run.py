from util import stats
from util import rs
from crar import plan
from redis import Redis
import subprocess
import heapq
import logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(filename)s:%(lineno)d] %(levelname)s  - %(message)s'
)

# collect download, upload bandwidth, gf_bandwidth
# generate a ecdag
# return ecdag to dump to a file
def run(filename, failed_node_id, src_node_ids, new_ids, all_node_ids, row_ids, obj_ids, object_size, ec_info, output, w, recorders):
    coor_connect = Redis(host="localhost", port=6379, db=0)

    all_stats = stats.CollectStats(True, False, False, True, True, w, recorders)    # download bandwidth, upload bandwidth, gf bandwidth of all worker nodes
    
    
    # select a nd node as requestor
    # nd = SelectNd(new_ids, object_size, all_stats, download_jobs)
    # failed_node_id = nd

    nd = failed_node_id
    download_bandwidths = [all_stats["download_bandwidth"][i - 1] for i in src_node_ids]
    upload_bandwidths = [all_stats["upload_bandwidth"][i - 1] for i in src_node_ids]
    gf_bandwidths = [all_stats["gf_bandwidth"][i - 1] for i in src_node_ids]
    # gf_bandwidths = [10240.0 for i in src_node_ids]                                 # used by full repair

    n, k, matrix = ReadECInfo(ec_info)                                              # read ec info from file
    logging.info(f"run, n: {n}, k: {k}, matrix: {matrix}")
    
    # repair ratio and help ratio of all surviving nodes
    repair_ratios, help_ratios = plan.solve_lp_problem(len(src_node_ids), k, object_size / 1024 / 1024, download_bandwidths, gf_bandwidths, upload_bandwidths)
    logging.info(f"repair ratios: {repair_ratios}, help_ratios: {help_ratios}")

    # generate ecdag according to repair ratio and help ratio
    GenerateECDAGFG(all_node_ids, obj_ids, row_ids, nd, src_node_ids, repair_ratios, help_ratios, matrix, n, k, output)
    
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

    

"""
generate transmission plan for each slice
repair and help means slice num node responsible for
k means num of blocks a slice need from other node to repair, equally k - 1 in RS
"""
# allocate repair task from max
# def generate_repair_allocation(n, k, repair, help):
#     logging.info(f"generate repair allocation, src node num: {n}, help slice num to receive: {k}, repair ratio: {repair}, help ratio: {help}")
#     assert(len(repair) == n and len(help) == n)
#     assert(sum(repair) * k == sum(help))
    
#     remaining_repair = repair.copy()
#     remaining_help = help.copy()
    
#     distribution = [[0 for _ in range(n)] for _ in range(n)]
#     ec_plan = []
#     while True:
#         max_repair = -1
#         target_node = -1
        
#         # select target node
#         for i in range(n):
#             if remaining_repair[i] > max_repair:
#                 max_repair = remaining_repair[i]
#                 target_node = i
        
#         if max_repair == 0:
#             break
        
#         # find k largest help nodes (not including target_node)
#         possible_helpers = []
#         for j in range(n):
#             if j != target_node and remaining_help[j] > 0:
#                 possible_helpers.append((j, remaining_help[j]))
        
#         possible_helpers.sort(key=lambda x: -x[1])

#         if (len(possible_helpers) < k):
#             logging.info(f"no enough helpers, unmateched repair: {sum(remaining_repair) / sum(repair)}")
#             break
        

#         selected_helpers = possible_helpers[:k]
#         h = selected_helpers[-1][1]
        
#         actual_h = min(h, remaining_repair[target_node])
#         logging.info(f"target node: {target_node}, selected helpers: {selected_helpers}, grain: {actual_h}")
        
#         for j, _ in selected_helpers:
#             distribution[target_node][j] += actual_h
#             remaining_help[j] -= actual_h
#         remaining_repair[target_node] -= actual_h          

#         ec_plan.append((target_node, selected_helpers, actual_h))
  
#     return ec_plan


class RepairItem:
    def __init__(self, size, node_id):
        self.size = size
        self.node_id = node_id
    
    def __lt__(self, other):
        if self.size != other.size:
            return self.size > other.size
        else:
            return self.node_id < other.node_id
        
class HelpItem:
    def __init__(self, size, node_id):
        self.size = size
        self.node_id = node_id
    
    def __lt__(self, other):
        if self.size != other.size:
            return self.size > other.size
        else:
            return self.node_id > other.node_id

def generate_repair_allocation(n, k, repair, help):
    logging.info(f"generate repair allocation, src node num: {n}, help slice num to receive: {k}, repair ratio: {repair}, help ratio: {help}")
    assert(len(repair) == n and len(help) == n)
    assert(sum(repair) * k == sum(help))
    
    repair_queue = []
    help_queue = []

    for i in range(n):
        heapq.heappush(repair_queue, RepairItem(repair[i], i))
        heapq.heappush(help_queue, HelpItem(help[i], i))
    
    distribution = [[0 for _ in range(n)] for _ in range(n)]
    ec_plan = []
       
    while True:
        max_repair = -1
        
        # select target node 
        repair_item = heapq.heappop(repair_queue)
        max_repair = repair_item.size
        target_node_id = repair_item.node_id
        
        if max_repair == 0:
            break
        
        # find k largest help nodes (not including target_node)
        possible_helpers = []
        self_item = None
        for _ in range(n):
            help_item = heapq.heappop(help_queue)
            if help_item.node_id != target_node_id:
                possible_helpers.append((help_item.node_id, help_item.size))
            else:
                self_item = help_item
            if len(possible_helpers) == k:
                break
        if self_item is not None:
            heapq.heappush(help_queue, self_item)
        
        possible_helpers.sort(key=lambda x: -x[1])

        if (len(possible_helpers) < k):
            logging.info(f"no enough helpers")
            break
        

        selected_helpers = possible_helpers[:k]
        h = selected_helpers[-1][1]
        
        actual_h = min(h, max_repair)
        logging.info(f"target node: {target_node_id}, selected helpers: {selected_helpers}, grain: {actual_h}")
        
        for help_node_id, help_size in selected_helpers:
            distribution[target_node_id][help_node_id] += actual_h
            help_size -= actual_h
            
            if help_size != 0:
                heapq.heappush(help_queue, HelpItem(help_size, help_node_id))

        repair_item.size -= actual_h
        # if repair_item.size != 0:
        heapq.heappush(repair_queue, repair_item)
        ec_plan.append((target_node_id, selected_helpers, actual_h))
  
    return ec_plan



################################################################################################################################################################

# node_ids: new node id for repaired obj
# stats: download and upload bandwidth of each node
# jobs: already allocated download and upload tasks of each nodes
def SelectNd(node_ids, object_size, stats, jobs):
    logging.info(f"SelectNd, source node_ids: {node_ids}, jobs: {jobs}, stats: {stats}")
    nd = -1
    min_download_time = float("inf")
    download_bandwidths = stats["download_bandwidth"]
    for node_id in node_ids:
        download_time = (jobs[node_id - 1] + 1) * (object_size) / download_bandwidths[node_id - 1]
        if download_time < min_download_time:
            min_download_time = download_time
            nd = node_id
    logging.info(f"SelectNd done, selected nd: {nd}")
    return nd


def SelectDownloadNode(node_ids, object_size, stats, download_tasks, upload_tasks, \
                       k, nd, download_selector, upload_selector):
    logging.info(f"SelectDownloadNode start, source node_ids: {node_ids}, with download_tasks: {download_tasks}, upload_tasks: {upload_tasks}, stats: {stats}")
    
    # one download task has been allocated to nd, now allocate k - 1 download tasks
    for i in range(k - 1):
        min_download_time = float("inf")
        select = -1
        # try nd
        download_time = max(
            upload_tasks[nd - 1] * object_size / stats["upload_bandwidth"][nd - 1],
            (download_tasks[nd - 1] + 1) * (object_size) / stats["download_bandwidth"][nd - 1]
        )
        if download_time < min_download_time:
            min_download_time = download_time
            select = nd

        # try node in node_ids(n - 1)
        for node_id in node_ids:
            if node_id in download_selector:                # already selected with a download task
                download_time = max((upload_tasks[node_id - 1] + 1) * object_size / stats["upload_bandwidth"][node_id - 1],
                                    (download_tasks[node_id - 1] + 1) * (object_size) / stats["download_bandwidth"][node_id - 1])
            else:
                download_time = max(upload_tasks[node_id - 1] * object_size / stats["upload_bandwidth"][node_id - 1],
                                    (download_tasks[node_id - 1] + 1) * (object_size) / stats["download_bandwidth"][node_id - 1])
            if download_time < min_download_time:
                min_download_time = download_time
                select = node_id
        download_selector[select] = download_selector.get(select, 0) + 1
        download_tasks[select - 1] += 1
        if select != nd:
            upload_tasks[select - 1] += 1
            upload_selector[select] = upload_selector.get(select, 0) + 1

        logging.info(f"SelectDownloadNode, for i.th: {i}, select {select}")
        logging.info(f"after select, download_tasks: {download_tasks}, upload_tasks: {upload_tasks}")


    logging.info(f"SelectDownloadNode done, selected download tasks: {download_tasks}, upload tasks: {upload_tasks}, \
                 download selector: {download_selector}, upload selector: {upload_selector}")   


# allocated remaining k - 1 upload tasks
def SelectUploadNode(node_ids, object_size, stats, upload_tasks, download_tasks, upload_selector, k):
    logging.info(f"SelectUploadNode, source node_ids: {node_ids}, upload_tasks: {upload_tasks}, upload_selector: {upload_selector}, stats: {stats}")
    remain_upload_tasks_cnt = k - len(upload_selector)
    for i in range(remain_upload_tasks_cnt):
        select = -1
        min_upload_time = float("inf")
        for node_id in node_ids:
            if node_id in upload_selector:
                continue

            upload_time = max((upload_tasks[node_id - 1] + 1) * object_size / stats["upload_bandwidth"][node_id - 1],
                               download_tasks[node_id - 1] * object_size / stats["download_bandwidth"][node_id - 1])
            if upload_time < min_upload_time:
                min_upload_time = upload_time
                select = node_id
        upload_selector[select] = upload_selector.get(select, 0) + 1
        upload_tasks[select - 1] += 1
    logging.info(f"SelectUploadNode done, selected download tasks: {download_tasks}, upload tasks: {upload_tasks}")


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


    # dag = {
    #     1: [],
    #     3: [],
    #     6: [],
    #     4: [(1, 4), (3, 4)],
    #     2: [(6, 2), (4, 2)]
    # }
    # nd = 2


    GetEdge(dag, nd, obj_ids, row_ids, node_id_2_coefs, nd, -1, result)
    logging.info(f"result: {result}")
    # output to file
    DumpOutput(result, output)
    
    return 


def ExecECDAG(filename, failed_node_id, upload_selector, output):
    # nodes_str = [str(x - 1) for x in upload_selector]  
    # nodes_str.append(str(failed_node_id - 1))
    # print(f"node_str{nodes_str}")
    
    ssh_cmd = "ssh node01"
    cmd = "/root/coar/build/ECClient decode " + filename + " " + output +  " 0 2 3 4087 1"
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

