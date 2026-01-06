#!/bin/bash

host_num=31
USER=root
NODE_NAME=node
DIR=/root/lmq_openec

# collect all nodes memory bandwidth usages
for((i=1;i<=$host_num;i++))
do
{
    if [[ $i -gt 0 && $i -lt 10 ]]
    then
        host=${NODE_NAME}0${i}
    else
        host=${NODE_NAME}$i
    fi
    ssh $USER@$host "cd $DIR/script; python3 mbw_monitor.py"
} &
done
wait
