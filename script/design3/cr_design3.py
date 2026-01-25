#!/usr/bin/env python3
import subprocess
import time


# cr
# commands = [
# "/root/lmq_openec/build/ECClient decode /input_256MB /root/lmq_openec/conf/256_n_6_k_4/ecdag_decode_256_cr_3 0 1 2 3 4 4086 5", 
# "/root/lmq_openec/build/ECClient decode /input_256MB_back /root/lmq_openec/conf/256_n_6_k_4/ecdag_decode_256_cr_4 0 1 2 3 4 4086 5", 


# "/root/lmq_openec/build/ECClient decode /input_384MB /root/lmq_openec/conf/384_n_9_k_6/ecdag_decode_384_cr_5  0 1 2 3 4 4086 5", 
# "/root/lmq_openec/build/ECClient decode /input_384MB_back /root/lmq_openec/conf/384_n_9_k_6/ecdag_decode_384_cr_6  0 1 2 3 4 4086 5", 
# "/root/lmq_openec/build/ECClient decode /input_384MB_back_back /root/lmq_openec/conf/384_n_9_k_6/ecdag_decode_384_cr_7  0 1 2 3 4 4086 5", 


# "/root/lmq_openec/build/ECClient decode /input_512MB /root/lmq_openec/conf/512_n_12_k_8/ecdag_decode_512_cr  0 1 2 3 4 4086 5", 
# "/root/lmq_openec/build/ECClient decode /input_512MB_back /root/lmq_openec/conf/512_n_12_k_8/ecdag_decode_512_cr_8  0 1 2 3 4 4086 5", 
# "/root/lmq_openec/build/ECClient decode /input_512MB_back_back /root/lmq_openec/conf/512_n_12_k_8/ecdag_decode_512_cr_10  0 1 2 3 4 4086 5", 
# "/root/lmq_openec/build/ECClient decode /input_512MB_back_back_back /root/lmq_openec/conf/512_n_12_k_8/ecdag_decode_512_cr_11  0 1 2 3 4 4086 5", 

# "/root/lmq_openec/build/ECClient decode /input_640MB /root/lmq_openec/conf/640_n_14_k_10/ecdag_decode_640_cr_9 0 1 2 3 4 4095 5",
# "/root/lmq_openec/build/ECClient decode /input_640MB /root/lmq_openec/conf/640_n_14_k_10/ecdag_decode_640_cr_10 0 1 2 3 4 4095 5",
# "/root/lmq_openec/build/ECClient decode /input_640MB /root/lmq_openec/conf/640_n_14_k_10/ecdag_decode_640_cr_11 0 1 2 3 4 4095 5",
# "/root/lmq_openec/build/ECClient decode /input_640MB /root/lmq_openec/conf/640_n_14_k_10/ecdag_decode_640_cr_12 0 1 2 3 4 4095 5",
# ]

# ppr
# commands = [
# "/root/lmq_openec/build/ECClient decode /input_640MB /root/lmq_openec/conf/640_n_14_k_10/ecdag_decode_640_ppr_10  0 1 2 3 4 4086 5", 
# "/root/lmq_openec/build/ECClient decode /input_640MB_back /root/lmq_openec/conf/640_n_14_k_10/ecdag_decode_640_ppr_11  0 1 2 3 4 4086 5", 
# "/root/lmq_openec/build/ECClient decode /input_640MB_back_back /root/lmq_openec/conf/640_n_14_k_10/ecdag_decode_640_ppr_12  0 1 2 3 4 4086 5", 
# "/root/lmq_openec/build/ECClient decode /input_640MB_back_back_back /root/lmq_openec/conf/640_n_14_k_10/ecdag_decode_640_ppr_14  0 1 2 3 4 4086 5", 
# ]

# rp
commands = [
"/root/lmq_openec/build/ECClient decode /input_256MB /root/lmq_openec/conf/256_n_6_k_4/ecdag_decode_256_ecpipe  0 1 2 3 4 4086 5", 
"/root/lmq_openec/build/ECClient decode /input_256MB_back /root/lmq_openec/conf/256_n_6_k_4/ecdag_decode_256_ecpipe_4  0 1 2 3 4 4086 5", 
"/root/lmq_openec/build/ECClient decode /input_256MB_back_back /root/lmq_openec/conf/256_n_6_k_4/ecdag_decode_256_ecpipe_6  0 1 2 3 4 4086 5", 
"/root/lmq_openec/build/ECClient decode /input_256MB_back_back_back /root/lmq_openec/conf/256_n_6_k_4/ecdag_decode_256_ecpipe_7  0 1 2 3 4 4086 5", 
# "/root/lmq_openec/build/ECClient decode /input_640MB /root/lmq_openec/conf/640_n_14_k_10/ecdag_decode_640_pipe  0 1 2 3 4 4086 5",
# "/root/lmq_openec/build/ECClient decode /input_640MB_back /root/lmq_openec/conf/640_n_14_k_10/ecdag_decode_640_pipe_11  0 1 2 3 4 4086 5",
# "/root/lmq_openec/build/ECClient decode /input_640MB_back_back /root/lmq_openec/conf/640_n_14_k_10/ecdag_decode_640_pipe_12  0 1 2 3 4 4086 5",
# "/root/lmq_openec/build/ECClient decode /input_640MB_back_back_back /root/lmq_openec/conf/640_n_14_k_10/ecdag_decode_640_pipe_14  0 1 2 3 4 4086 5",
]

# fullrepair & coar & hpca25
# commands = [




    
# "/root/lmq_openec/build/ECClient decode /input_256MB /root/lmq_openec/build/input_256MB_ecdag_temp  0 1 2 3 4 4086 5", 
# "/root/lmq_openec/build/ECClient decode /input_256MB_back /root/lmq_openec/build/input_256MB_ecdag_temp_back  0 1 2 3 4 4086 5", 

# "/root/lmq_openec/build/ECClient decode /input_384MB /root/lmq_openec/build/input_384MB_ecdag_temp  0 1 2 3 4 4086 5", 
# "/root/lmq_openec/build/ECClient decode /input_384MB_back /root/lmq_openec/build/input_384MB_ecdag_temp_back  0 1 2 3 4 4086 5", 
# "/root/lmq_openec/build/ECClient decode /input_384MB_back_back /root/lmq_openec/build/input_384MB_ecdag_temp_back_back  0 1 2 3 4 4086 5", 


# "/root/lmq_openec/build/ECClient decode /input_512MB /root/lmq_openec/build/input_512MB_ecdag_temp  0 1 2 3 4 4086 5", 
# "/root/lmq_openec/build/ECClient decode /input_512MB_back /root/lmq_openec/build/input_512MB_ecdag_temp_back  0 1 2 3 4 4086 5", 
# "/root/lmq_openec/build/ECClient decode /input_512MB_back_back /root/lmq_openec/build/input_512MB_ecdag_temp_back_back  0 1 2 3 4 4086 5", 
# "/root/lmq_openec/build/ECClient decode /input_512MB_back_back_back /root/lmq_openec/build/input_512MB_ecdag_temp_back_back_back  0 1 2 3 4 4086 5", 

# "/root/lmq_openec/build/ECClient decode /input_640MB /root/lmq_openec/build/input_640MB_ecdag_temp  0 1 2 3 4 4086 5", 
# "/root/lmq_openec/build/ECClient decode /input_640MB_back /root/lmq_openec/build/input_640MB_ecdag_temp_back  0 1 2 3 4 4086 5", 
# "/root/lmq_openec/build/ECClient decode /input_640MB_back_back /root/lmq_openec/build/input_640MB_ecdag_temp_back_back  0 1 2 3 4 4086 5", 
# "/root/lmq_openec/build/ECClient decode /input_640MB_back_back_back /root/lmq_openec/build/input_640MB_ecdag_temp_back_back_back  0 1 2 3 4 4086 5", 
# ]



# commands = [
#     """
#     python3 run_ecdag.py  --type hpca25 --filename /input_512MB --failed_node_id 8 --src_node_ids 1 2 3 4 5 6 7 9 10 11 12 --new_ids 1 2 3 4 5 6 7 8 9 10 11 12 --all_node_ids 1 2 3 4 5 6 7 8 9 10 11 12 \
#     --obj_ids 0 1 2 3 4 5 6 7 4095 4094 4093 4092  --row_ids 1 2 3 4 5 6 7 8 9 10 11 12 --object_size 67108864 --ec_info /root/lmq_openec/build/ec_info \
#     --output /root/lmq_openec/build/input_512MB_ecdag_temp
#     """,
#     """
#     python3 run_ecdag.py  --type hpca25 --filename /input_512MB_back --failed_node_id 8 --src_node_ids 1 2 3 4 5 6 7 9 10 11 12 --new_ids 1 2 3 4 5 6 7 8 9 10 11 12 --all_node_ids 1 2 3 4 5 6 7 8 9 10 11 12 \
#     --obj_ids 0 1 2 3 4 5 6 7 4095 4094 4093 4092  --row_ids 1 2 3 4 5 6 7 8 9 10 11 12 --object_size 67108864 --ec_info /root/lmq_openec/build/ec_info \
#     --output /root/lmq_openec/build/input_512MB_ecdag_temp_back
#     """,
#     """
#     python3 run_ecdag.py  --type hpca25 --filename /input_512MB_back_back --failed_node_id 8 --src_node_ids 1 2 3 4 5 6 7 9 10 11 12 --new_ids 1 2 3 4 5 6 7 8 9 10 11 12 --all_node_ids 1 2 3 4 5 6 7 8 9 10 11 12 \
#     --obj_ids 0 1 2 3 4 5 6 7 4095 4094 4093 4092  --row_ids 1 2 3 4 5 6 7 8 9 10 11 12 --object_size 67108864 --ec_info /root/lmq_openec/build/ec_info \
#     --output /root/lmq_openec/build/input_512MB_ecdag_temp_back_back
#     """,
#         """
#     python3 run_ecdag.py  --type hpca25 --filename /input_512MB_back_back_back  --failed_node_id 8 --src_node_ids 1 2 3 4 5 6 7 9 10 11 12 --new_ids 1 2 3 4 5 6 7 8 9 10 11 12 --all_node_ids 1 2 3 4 5 6 7 8 9 10 11 12 \
#     --obj_ids 0 1 2 3 4 5 6 7 4095 4094 4093 4092  --row_ids 1 2 3 4 5 6 7 8 9 10 11 12 --object_size 67108864 --ec_info /root/lmq_openec/build/ec_info \
#     --output /root/lmq_openec/build/input_512MB_ecdag_temp_back_back_back
#     """,
# ]
log_file = "design3_repair.log"

start_time_ms = time.time() * 1000

processes = []
for cmd in commands:
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    processes.append(proc)

for proc in processes:
    proc.wait()

end_time_ms = time.time() * 1000
total_ms = end_time_ms - start_time_ms
print(f"total: {total_ms}")
with open(log_file, 'a', encoding='utf-8') as f:
    f.write(f"{total_ms}\n")