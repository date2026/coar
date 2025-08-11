#!/bin/bash

USER=root
HOME=/home/$USER
NODE_NAME=node
NODE_NUM=9

stop-dfs.sh
rm -rf /root/hadoop-3.0.0-src/hadoop-dist/target/hadoop-3.0.0/data
for((i=1;i<=$NODE_NUM;i++));
do
{
    if [[ $i -gt 0 && $i -lt 10 ]]
	then
		host=${NODE_NAME}0${i}
	else
		host=${NODE_NAME}$i
	fi
    ssh $USER@$host "rm -rf /root/hadoop-3.0.0-src/hadoop-dist/target/hadoop-3.0.0/data"
} &
done

wait

hdfs namenode -format

start-dfs.sh

hdfs dfsadmin -report