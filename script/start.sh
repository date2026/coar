#!/bin/bash

NODE_NUM=9
NODE_NAME=node
USER=root
DIR=/root/coar

cd $DIR/build; ./ECCoordinator > ./ECCoordinator.log &
for((i=1;i<=$NODE_NUM;i++))
do
{
    if [[ $i -gt 0 && $i -lt 10 ]]
    then
        host=${NODE_NAME}0${i}
    else
        host=${NODE_NAME}$i
    fi

    ssh $USER@$host "cd $DIR/build; bash ../script/start_agent.sh"
} &
done