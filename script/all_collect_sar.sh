#!/bin/bash

host_num=9
USER=root
NODE_NAME=node
DIR=/root/coar


for((i=1;i<=$host_num;i++))
do
{
    if [[ $i -gt 0 && $i -lt 10 ]]
    then
        host=${NODE_NAME}0${i}
    else
        host=${NODE_NAME}$i
    fi
    ssh $USER@$host "cd $DIR/script; bash collect_sar.sh; python3 parse_sar.py"
    # ssh $USER@$host "cd $DIR/script; python3 parse_sar.py"
} &
done
wait
