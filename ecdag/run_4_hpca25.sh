start=$(date +%s%N)

# 6+3
# python3 run_ecdag.py --type hpca25 --filename /input_384MB --failed_node_id 6 --src_node_ids 1 2 3 4 5 7 --new_ids 1 2 3 4 5 6 7 8 9 --all_node_ids 1 2 3 4 5 6 7 8 9 \
#     --obj_ids 0 1 2 3 4 5 4095 4094 4093 --row_ids 1 2 3 4 5 6 7 8 9 --object_size 67108864 --ec_info /root/coar/build/ec_info \
#     --output /root/coar/build/input_384MB_ecdag_temp &

# python3 run_ecdag.py --type hpca25 --filename /input_384MB_back --failed_node_id 6 --src_node_ids 1 2 3 4 5 7 --new_ids 1 2 3 4 5 6 7 8 9 --all_node_ids 1 2 3 4 5 6 7 8 9 \
#     --obj_ids 0 1 2 3 4 5 4095 4094 4093 --row_ids 1 2 3 4 5 6 7 8 9 --object_size 67108864 --ec_info /root/coar/build/ec_info \
#     --output /root/coar/build/input_384MB_ecdag_temp_back &

# python3 run_ecdag.py --type hpca25 --filename /input_384MB_back_back --failed_node_id 6 --src_node_ids 1 2 3 4 5 7 --new_ids 1 2 3 4 5 6 7 8 9 --all_node_ids 1 2 3 4 5 6 7 8 9 \
#     --obj_ids 0 1 2 3 4 5 4095 4094 4093 --row_ids 1 2 3 4 5 6 7 8 9 --object_size 67108864 --ec_info /root/coar/build/ec_info \
#     --output /root/coar/build/input_384MB_ecdag_temp_back_back &

# python3 run_ecdag.py --type hpca25 --filename /input_384MB_back_back_back --failed_node_id 6 --src_node_ids 1 2 3 4 5 7 --new_ids 1 2 3 4 5 6 7 8 9 --all_node_ids 1 2 3 4 5 6 7 8 9 \
#     --obj_ids 0 1 2 3 4 5 4095 4094 4093 --row_ids 1 2 3 4 5 6 7 8 9 --object_size 67108864 --ec_info /root/coar/build/ec_info \
#     --output /root/coar/build/input_384MB_ecdag_temp_back_back_back &


python3 run_ecdag.py --type hpca25 --filename /input_256MB --failed_node_id 4 --src_node_ids 1 2 3 5 6 --new_ids 1 2 3 4 5 6 --all_node_ids 1 2 3 4 5 6 \
    --obj_ids 0 1 2 3 4095 4094 --row_ids 1 2 3 4 5 6 --object_size 268435456 --ec_info /root/coar/build/ec_info \
    --output /root/coar/build/input_256MB_ecdag_temp &

python3 run_ecdag.py --type hpca25 --filename /input_256MB_back --failed_node_id 4 --src_node_ids 1 2 3 5 6 --new_ids 1 2 3 4 5 6 --all_node_ids 1 2 3 4 5 6 \
    --obj_ids 0 1 2 3 4095 4094 --row_ids 1 2 3 4 5 6 --object_size 268435456 --ec_info /root/coar/build/ec_info \
    --output /root/coar/build/input_256MB_ecdag_temp_back &

# python3 run_ecdag.py --type hpca25 --filename /input_256MB_back_back --failed_node_id 4 --src_node_ids 1 2 3 5 6 --new_ids 1 2 3 4 5 6 --all_node_ids 1 2 3 4 5 6 \
#     --obj_ids 0 1 2 3 4095 4094 --row_ids 1 2 3 4 5 6 --object_size 268435456 --ec_info /root/coar/build/ec_info \
#     --output /root/coar/build/input_256MB_ecdag_temp_back_back &

# python3 run_ecdag.py --type hpca25 --filename /input_256MB_back_back_back --failed_node_id 4 --src_node_ids 1 2 3 5 6 --new_ids 1 2 3 4 5 6 --all_node_ids 1 2 3 4 5 6 \
#     --obj_ids 0 1 2 3 4095 4094 --row_ids 1 2 3 4 5 6 --object_size 268435456 --ec_info /root/coar/build/ec_info \
#     --output /root/coar/build/input_256MB_ecdag_temp_back_back_back &


wait

end=$(date +%s%N)
duration=$((end - start))
milliseconds=$(echo "scale=3; $duration / 1000000" | bc)
echo $milliseconds >> /root/coar/build/repair.log


