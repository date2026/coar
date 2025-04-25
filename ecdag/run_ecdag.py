# -*- coding: utf-8 -*-

from util.stats import *
from hpca25 import run as hpca25_run
from lmq import run as lmq_run
import argparse

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

"""
input: nodes can be used, 
output: repair ecdag(k download task, k upload task, and a ecdag)
python3 run_ecdag.py --type hpca25 --filename /input_1024MB --failed_node_id 2 --src_node_ids 1 3 4 5 6 --new_ids 2 7 8 9 --all_node_ids 1 2 3 4 5 6 \
    --obj_ids 0 1 2 3 4088 4087 --row_ids 1 2 3 4 5 6 --object_size 268435456 --ec_info /home/openec/lmq_openec/build/ec_info \
    --output /home/openec/lmq_openec/build/ecdag_temp
    
python3 run_ecdag.py --type hpca25 --filename /input_1020MB --failed_node_id 6 --src_node_ids 1 2 3 4 5 7 8 9 --new_ids 6 --all_node_ids 1 2 3 4 5 6 7 8 9 \
    --obj_ids 0 1 2 3 4 5 4089 4088 4087 --row_ids 1 2 3 4 5 6 7 8 9 --object_size 178257920 --ec_info /home/openec/lmq_openec/build/ec_info \
    --output /home/openec/lmq_openec/build/input_1020MB_ecdag_temp

python3 run_ecdag.py --type hpca25 --filename /input_1020MB_back --failed_node_id 6 --src_node_ids 1 2 3 4 5 7 8 9 --new_ids 6 --all_node_ids 1 2 3 4 5 6 7 8 9 \
    --obj_ids 0 1 2 3 4 5 4089 4088 4087 --row_ids 1 2 3 4 5 6 7 8 9 --object_size 178257920 --ec_info /home/openec/lmq_openec/build/ec_info \
    --output /home/openec/lmq_openec/build/input_1020MB_back_ecdag_temp

python3 run_ecdag.py --type hpca25 --filename /input_1020MB_back_back --failed_node_id 6 --src_node_ids 1 2 3 4 5 7 8 9 --new_ids 6 --all_node_ids 1 2 3 4 5 6 7 8 9 \
    --obj_ids 0 1 2 3 4 5 4089 4088 4087 --row_ids 1 2 3 4 5 6 7 8 9 --object_size 178257920 --ec_info /home/openec/lmq_openec/build/ec_info \
    --output /home/openec/lmq_openec/build/input_1020MB_back_back_ecdag_temp

python3 run_ecdag.py --type lmq --filename /input_1020MB --failed_node_id 6 --src_node_ids 1 2 3 4 5 7 8 9 --new_ids 6 --all_node_ids 1 2 3 4 5 6 7 8 9 \
    --obj_ids 0 1 2 3 4 5 4089 4088 4087 --row_ids 1 2 3 4 5 6 7 8 9 --object_size 178257920 --ec_info /home/openec/lmq_openec/build/ec_info \
    --output /home/openec/lmq_openec/build/input_1020MB_ecdag_temp
"""
if __name__ == "__main__":
    parser = GetParse()

    if parser.type == "hpca25":
        hpca25_run.run(parser.filename, parser.failed_node_id, parser.src_node_ids, parser.new_ids, \
                       parser.all_node_ids, parser.row_ids, parser.obj_ids, parser.object_size, parser.ec_info, parser.output)
    elif parser.type == "lmq":
        lmq_run.run(parser.filename, parser.failed_node_id, parser.src_node_ids, parser.new_ids, \
                    parser.all_node_ids, parser.row_ids, parser.obj_ids, parser.object_size, parser.ec_info, parser.output)
    else:
        assert False and "undefined type"