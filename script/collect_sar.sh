OUTPUT_DIR="/home/openec/lmq_openec/script/sysstat"
mkdir -p $OUTPUT_DIR
rm -rf $OUTPUT_DIR/*.sar

for i in $(seq 1 1 80); do
    # collect_stats
    sar -A 10 1 -p -o $OUTPUT_DIR/$i.sar
done
