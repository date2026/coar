start=$(date +%s%N)

# /root/coar/build/ECClient decode /input_384MB /root/coar/conf/384_n_9_k_6/ecdag_decode_384_pipe_5  0 1 2 3 4 4086 5 &
# /root/coar/build/ECClient decode /input_384MB_back /root/coar/conf/384_n_9_k_6/ecdag_decode_384_pipe_5  0 1 2 3 4 4086 5 &
# /root/coar/build/ECClient decode /input_384MB_back_back /root/coar/conf/384_n_9_k_6/ecdag_decode_384_pipe_5  0 1 2 3 4 4086 5 &
# /root/coar/build/ECClient decode /input_384MB_back_back_back /root/coar/conf/384_n_9_k_6/ecdag_decode_384_pipe_5  0 1 2 3 4 4086 5 &

/root/coar/build/ECClient decode /input_256MB /root/coar/conf/256_n_6_k_4/ecdag_decode_256_ecpipe  0 1 2 3 4 4086 5 &
/root/coar/build/ECClient decode /input_256MB_back /root/coar/conf/256_n_6_k_4/ecdag_decode_256_ecpipe  0 1 2 3 4 4086 5 &
/root/coar/build/ECClient decode /input_256MB_back_back /root/coar/conf/256_n_6_k_4/ecdag_decode_256_ecpipe  0 1 2 3 4 4086 5 &
/root/coar/build/ECClient decode /input_256MB_back_back_back /root/coar/conf/256_n_6_k_4/ecdag_decode_256_ecpipe  0 1 2 3 4 4086 5 &



wait

end=$(date +%s%N)
duration=$((end - start))
milliseconds=$(echo "scale=3; $duration / 1000000" | bc)
echo $milliseconds >> /root/coar/build/single_node_repair.log