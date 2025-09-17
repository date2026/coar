# -*- coding: utf-8 -*-

from util.stats import *
from hpca25 import run as hpca25_run
from lmq import run as lmq_run
from load_avg import run as load_avg_run
import argparse
from crar import run as crar_run
from coar_coarse import run as coar_coarse_run
from util import gf_cost_recorder
import logging
import threading
import time

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

WORKER_NUM = 9
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
        elif parser.type == "lmq":
            lmq_run.run(parser.filename, parser.failed_node_id, parser.src_node_ids, parser.new_ids, \
                        parser.all_node_ids, parser.row_ids, parser.obj_ids, parser.object_size, parser.ec_info, parser.output)
        elif parser.type == "load_avg":
            load_avg_run.run(parser.filename, parser.failed_node_id, parser.src_node_ids, parser.new_ids, \
                            parser.all_node_ids, parser.row_ids, parser.obj_ids, parser.object_size, parser.ec_info, parser.output)
        elif parser.type == "crar":
            crar_run.run(parser.filename, parser.failed_node_id, parser.src_node_ids, parser.new_ids, \
                            parser.all_node_ids, parser.row_ids, parser.obj_ids, parser.object_size, parser.ec_info, parser.output, 8, recorders)
        elif parser.type == "coar_coarse":
            coar_coarse_run.run(parser.filename, parser.failed_node_id, parser.src_node_ids, parser.new_ids, \
                            parser.all_node_ids, parser.row_ids, parser.obj_ids, parser.object_size, parser.ec_info, parser.output, 8, recorders)
        else:
            assert False and "undefined type"

        print("==================================== exec done ====================================")
    
    refresh_gf_bandwidth_thd.join()


    # parser = GetParse()

    # if parser.type == "hpca25":
    #     hpca25_run.run(parser.filename, parser.failed_node_id, parser.src_node_ids, parser.new_ids, \
    #                    parser.all_node_ids, parser.row_ids, parser.obj_ids, parser.object_size, parser.ec_info, parser.output)
    # elif parser.type == "lmq":
    #     lmq_run.run(parser.filename, parser.failed_node_id, parser.src_node_ids, parser.new_ids, \
    #                 parser.all_node_ids, parser.row_ids, parser.obj_ids, parser.object_size, parser.ec_info, parser.output)
    # elif parser.type == "load_avg":
    #     load_avg_run.run(parser.filename, parser.failed_node_id, parser.src_node_ids, parser.new_ids, \
    #                       parser.all_node_ids, parser.row_ids, parser.obj_ids, parser.object_size, parser.ec_info, parser.output)
    # elif parser.type == "crar":
    #     crar_run.run(parser.filename, parser.failed_node_id, parser.src_node_ids, parser.new_ids, \
    #                       parser.all_node_ids, parser.row_ids, parser.obj_ids, parser.object_size, parser.ec_info, parser.output, 8, recorders)
    # else:
    #     assert False and "undefined type"