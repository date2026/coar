#!/bin/bash
# collect network bandwidth and repair computation throughput across nodes under analytics workloads
host_num=9
USER=root
NODE_NAME=node
DIR=[your path]

REMOTE_DIR=[your path]/script


func_collect() {
    mkdir -p ${REMOTE_DIR}/profile_data

    for((i=1;i<=$host_num;i++))
    do
    {
        if [[ $i -gt 0 && $i -lt 10 ]]
        then
            host=${NODE_NAME}0${i}
        else
            host=${NODE_NAME}$i
        fi
        scp $USER@$host:${REMOTE_DIR}/system_metrics.csv ${REMOTE_DIR}/sysstat/throughput_profile_data/${host}.csv
        scp $USER@$host:${REMOTE_DIR}/resource_usage.csv ${REMOTE_DIR}/sysstat/resource_profile_data/${host}.csv
    }
    done
}


func_start() {
    for((i=1;i<=$host_num;i++))
    do
    {
        if [[ $i -gt 0 && $i -lt 10 ]]
        then
            host=${NODE_NAME}0${i}
        else
            host=${NODE_NAME}$i
        fi
        ssh $USER@$host "cd $DIR/script; python3 profile.py"
    } &
    done
    wait
}


case "$1" in
    start)
        func_start
        ;;
    collect)
        func_collect
        ;;
    *)
        echo "Usage: $0 {start|collect}"
        exit 1
        ;;
esac