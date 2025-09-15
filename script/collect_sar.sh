OUTPUT_DIR="/root/coar/script/sysstat"
mkdir -p $OUTPUT_DIR
rm -rf $OUTPUT_DIR/*.sar

step_num=160
for i in $(seq 1 1 $step_num); do
    # collect_stats
    sar -A 2 1 -p -o $OUTPUT_DIR/$i.sar
    # uptime >> $OUTPUT_DIR/$i_load_avg
done
