#!/bin/bash

NODE_NUM=9
NODE_NAME=node
HOME=/root/coar
USER=root
ip_list=(
    192.168.0.220 
	192.168.0.221
	192.168.0.222
    192.168.0.223
    192.168.0.224 
	192.168.0.225
	192.168.0.226
    192.168.0.227
    192.168.0.228
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

    ssh $USER@$host "sudo tc qdisc delete dev eth0 root;chmod +x $HOME/script/tc.sh; bash $HOME/script/tc.sh"
} &
done

wait