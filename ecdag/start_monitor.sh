#!/bin/bash

# NODE_NUM=9
NODE_NUM=31
# NODE_NUM=63
NODE_NAME=node
DIR=[your path]

for((i=1;i<=$NODE_NUM;i++))
do
{
    if [[ $i -gt 0 && $i -lt 10 ]]
    then
        host=${NODE_NAME}0${i}
    else
        host=${NODE_NAME}$i
    fi
    ssh $USER@$host "cd $DIR/ecdag; python3 monitor.py > monitor.log" &
} 
done