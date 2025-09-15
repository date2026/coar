from util import stats
from util import rs
from redis import Redis
import subprocess
import logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(filename)s:%(lineno)d] %(levelname)s  - %(message)s'
)

# collect download and upload bandwidth
# generate a ecdag
# return ecdag to dump to a file
def run(filename, failed_node_id, src_node_ids, new_ids, all_node_ids, row_ids, obj_ids, object_size, ec_info, output):
    coor_connect = Redis(host="localhost", port=6379, db=0)

    all_stats = stats.CollectStats(True, False, False, True)                        # download bandwidth and upload bandwidth of each node
    download_jobs, upload_jobs = stats.CollectJobs()                                # job count of each node
    download_selector, upload_selector = {}, {}                                     # selected nodes for download and upload
    # select nd
    # nd = SelectNd(new_ids, object_size, all_stats, download_jobs)
    nd = failed_node_id
    failed_node_id = nd
    
    # used in single node
    # src_node_ids = all_node_ids.copy()
    # src_node_ids.remove(nd)
    download_jobs[nd - 1] += 1
    download_selector[nd] = download_selector.get(nd, 0) + 1
    n, k, matrix = ReadECInfo(ec_info)                                              # read ec info from file
    logging.info(f"run, n: {n}, k: {k}, matrix: {matrix}")
    # select download node(and corresponding upload node)
    SelectDownloadNode(src_node_ids, object_size, all_stats, download_jobs, upload_jobs, \
                       k, nd, download_selector, upload_selector)                   # select download node
    SelectUploadNode(src_node_ids, object_size, all_stats, upload_jobs, download_jobs, \
                     upload_selector, k)                                            # select upload node
    stats.UpdateTasks(download_jobs, upload_jobs)
    logging.info(f"nd: {nd}")
    logging.info(f"download_selector: {download_selector}")
    logging.info(f"upload_selector: {upload_selector}")
    logging.info(f"download_jobs: {download_jobs}")
    logging.info(f"upload_jobs: {upload_jobs}")

    old_download_selector = download_selector.copy()
    old_upload_selector = upload_selector.copy()
    # download_selector = {
    #     2: 2,
    #     4: 2
    # }
    # upload_selector = {
    #     1: 1,
    #     3: 1,
    #     4: 1,
    #     6: 1
    # }
    # nd = 2
    node_id_2_coefs = rs.GetCoefVector(matrix, all_node_ids, row_ids, upload_selector.keys(), failed_node_id, k, 8)
    logging.info(f"node_id_2_coefs: {node_id_2_coefs}")
    # generate ecdag for this ec repair
    GenerateECDAG(all_node_ids, obj_ids, row_ids, node_id_2_coefs, download_selector, upload_selector, nd, matrix, n, k, output)
    # exec ec repair
    ExecECDAG(filename, failed_node_id, upload_selector, output)
    stats.ReStoreTasks(old_download_selector, old_upload_selector)
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