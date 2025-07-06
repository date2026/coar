OUTPUT_DIR="/home/openec/lmq_openec/script/sysstat"
mkdir -p $OUTPUT_DIR
rm -rf $OUTPUT_DIR/*.sar

for i in $(seq 1 1 400); do
    # collect_stats
    sar -A 2 1 -p -o $OUTPUT_DIR/$i.sar
    uptime >> $OUTPUT_DIR/$i_load_avg
done
