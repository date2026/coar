#!/bin/bash

NODE_NUM=9
NODE_NAME=node
HOME=/home/openec/lmq_openec

ip_list=(
    192.168.220.161 
    192.168.220.162 
	192.168.220.163
	192.168.220.164
    192.168.220.165
    192.168.220.166 
	192.168.220.167
	192.168.220.168
    192.168.220.169 
)


for((i=1;i<=$NODE_NUM;i++))
do
{
    if [[ $i -gt 0 && $i -lt 10 ]]
    then
        host=${NODE_NAME}0${i}
    else
        host=${NODE_NAME}$i
    fi

    ssh $USER@$host "sudo tc qdisc delete dev ens33 root;chmod +x $HOME/script/tc.sh; bash $HOME/script/tc.sh"
} &
done

wait