# -*- coding: utf-8 -*-

from util.stats import *
from hpca25 import run as hpca25_run
from tree import run as tree_run
from load_avg import run as load_avg_run
import argparse
from coar import run as crar_run
from coar_coarse import run as coar_coarse_run
from scheme import run as scheme_run
# from util import gf_cost_recorder
from util import rcb_predictor as gf_cost_recorder                                  # use GBDT to predict repair computation throughput
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(filename)s:%(lineno)d] %(levelname)s  - %(message)s'
)

def GetParse():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--type', type = str, help='ecdag type', default='hpca25')
    parser.add_argument('--filename', type = str, help='file to repair', default='')
    parser.add_argument('--failed_node_id', type = int, help='failed node id', default = -1)
    parser.add_argument('--src_node_ids', type = int, nargs='+', default = [], help = "node id can be used to repair")
    parser.add_argument('--new_ids', type = int, nargs='+', default = [], help = "node id can be used to store repaired object")
    parser.add_argument('--all_node_ids', type = int, nargs='+', default = [], help = "all node id")
    parser.add_argument('--row_ids', type = int, nargs='+', default = [])
    parser.add_argument('--obj_ids', type = int, nargs='+', default = [])
    parser.add_argument('--object_size', type = int, help='object size', default = -1)
    parser.add_argument('--ec_info', type = str, help='ec info file path', default = '')
    parser.add_argument('--output', type = str, help='output file path', default = '')
    return parser.parse_args()

def parse_single_args(args_str):
    parser = argparse.ArgumentParser()
    parser.add_argument('--type', type=str, help='ecdag type', default='hpca25')
    parser.add_argument('--filename', type=str, help='file to repair', default='')
    parser.add_argument('--failed_node_id', type=int, help='failed node id', default=-1)
    parser.add_argument('--src_node_ids', type=int, nargs='+', default=[], help="node id can be used to repair")
    parser.add_argument('--new_ids', type=int, nargs='+', default=[], help="node id to store repaired object")
    parser.add_argument('--all_node_ids', type=int, nargs='+', default=[], help="all node id")
    parser.add_argument('--row_ids', type=int, nargs='+', default=[])
    parser.add_argument('--obj_ids', type=int, nargs='+', default=[])
    parser.add_argument('--object_size', type=int, help='object size', default=-1)
    parser.add_argument('--ec_info', type=str, help='ec info file path', default='')
    parser.add_argument('--output', type=str, help='output file path', default='')
    args_list = args_str.strip().split()
    return parser.parse_args(args_list)

# WORKER_NUM = 9
WORKER_NUM = 31
# WORKER_NUM = 63
CPU_THRESHOLD = 10.0
recorders = gf_cost_recorder.GFCostRecorderArray(WORKER_NUM, CPU_THRESHOLD)
W = 8

if __name__ == "__main__":

    refresh_gf_bandwidth_thd = threading.Thread(target=recorders.RecordGFOverhead)
    refresh_gf_bandwidth_thd.start()

    while True:
        print("==================================== input args ====================================")
        input_lines = []
        while True:
            line = input().strip()
            if line.endswith('\\'):
                input_lines.append(line[:-1].strip()) 
            else:
                input_lines.append(line)
                break
        input_str = ' '.join(input_lines).strip()

        if input_str.lower() in ['exit', 'quit']:
            sys.exit(0)

        parser = parse_single_args(input_str)

        if parser.type == "hpca25":
            hpca25_run.run(parser.filename, parser.failed_node_id, parser.src_node_ids, parser.new_ids, \
                        parser.all_node_ids, parser.row_ids, parser.obj_ids, parser.object_size, parser.ec_info, parser.output)
        elif parser.type == "tree":
            tree_run.run(parser.filename, parser.failed_node_id, parser.src_node_ids, parser.new_ids, \
                        parser.all_node_ids, parser.row_ids, parser.obj_ids, parser.object_size, parser.ec_info, parser.output)
        elif parser.type == "load_avg":
            load_avg_run.run(parser.filename, parser.failed_node_id, parser.src_node_ids, parser.new_ids, \
                            parser.all_node_ids, parser.row_ids, parser.obj_ids, parser.object_size, parser.ec_info, parser.output)
        elif parser.type == "coar":
            crar_run.run(parser.filename, parser.failed_node_id, parser.src_node_ids, parser.new_ids, \
                            parser.all_node_ids, parser.row_ids, parser.obj_ids, parser.object_size, parser.ec_info, parser.output, 8, recorders)
        elif parser.type == "coar_coarse":
            coar_coarse_run.run(parser.filename, parser.failed_node_id, parser.src_node_ids, parser.new_ids, \
                            parser.all_node_ids, parser.row_ids, parser.obj_ids, parser.object_size, parser.ec_info, parser.output, 8, recorders)
        elif parser.type == "cr":
            scheme_run.run_cr(parser.filename, parser.failed_node_id, parser.src_node_ids, parser.new_ids, \
                        parser.all_node_ids, parser.row_ids, parser.obj_ids, parser.object_size, parser.ec_info, parser.output)
        elif parser.type == "ppr":
            scheme_run.run_ppr(parser.filename, parser.failed_node_id, parser.src_node_ids, parser.new_ids, \
                        parser.all_node_ids, parser.row_ids, parser.obj_ids, parser.object_size, parser.ec_info, parser.output)
        elif parser.type == "rp":
            scheme_run.run_rp(parser.filename, parser.failed_node_id, parser.src_node_ids, parser.new_ids, \
                        parser.all_node_ids, parser.row_ids, parser.obj_ids, parser.object_size, parser.ec_info, parser.output)
        else:
            assert False and "undefined type"

        print("==================================== exec done ====================================")
    
    refresh_gf_bandwidth_thd.join()

    # for single-chunk repair
    # parser = GetParse()

    # if parser.type == "hpca25":
    #     hpca25_run.run(parser.filename, parser.failed_node_id, parser.src_node_ids, parser.new_ids, \
    #                    parser.all_node_ids, parser.row_ids, parser.obj_ids, parser.object_size, parser.ec_info, parser.output)
    # elif parser.type == "tree":
    #     tree_run.run(parser.filename, parser.failed_node_id, parser.src_node_ids, parser.new_ids, \
    #                 parser.all_node_ids, parser.row_ids, parser.obj_ids, parser.object_size, parser.ec_info, parser.output)
    # elif parser.type == "load_avg":
    #     load_avg_run.run(parser.filename, parser.failed_node_id, parser.src_node_ids, parser.new_ids, \
    #                       parser.all_node_ids, parser.row_ids, parser.obj_ids, parser.object_size, parser.ec_info, parser.output)
    # elif parser.type == "crar":
    #     crar_run.run(parser.filename, parser.failed_node_id, parser.src_node_ids, parser.new_ids, \
    #                       parser.all_node_ids, parser.row_ids, parser.obj_ids, parser.object_size, parser.ec_info, parser.output, 8, recorders)
    # else:
    #     assert False and "undefined type"

    # for single-node repair evaluation
    # parser = GetParse()

    # start_time = time.time()
    # task_params_list = [
    #     (parser.filename, 2, parser.src_node_ids, parser.new_ids,
    #      parser.all_node_ids, parser.row_ids, parser.obj_ids, parser.object_size, parser.ec_info, parser.output),
    #     (parser.filename + "_back", 3, parser.src_node_ids, parser.new_ids,
    #      parser.all_node_ids, parser.row_ids, parser.obj_ids, parser.object_size, parser.ec_info, parser.output  + "_back"),
    #     (parser.filename + "_back_back", 4, parser.src_node_ids, parser.new_ids,
    #      parser.all_node_ids, parser.row_ids, parser.obj_ids, parser.object_size, parser.ec_info, parser.output + "_back_back"),
    #     (parser.filename + "_back_back_back", 5, parser.src_node_ids, parser.new_ids,
    #      parser.all_node_ids, parser.row_ids, parser.obj_ids, parser.object_size, parser.ec_info, parser.output + "_back_back_back")
    # ]

    # with ThreadPoolExecutor(max_workers=4) as executor:
    #     # futures = [executor.submit(hpca25_run.run, *params) for params in task_params_list]
    #     futures = [executor.submit(crar_run.run, *params, 8, recorders) for params in task_params_list]
    #     for future in futures:
    #         future.result()
    # end_time = time.time()
    # total_time_ms = (end_time - start_time) * 1000
    
    # with open("repair.log", "a", encoding="utf-8") as f:
    #     f.write(f"{total_time_ms}\n")
    
    # print(f"total time: {total_time_ms} ms")