host_num=9
USER=openec
NODE_NAME=node
DIR=/home/openec/lmq_openec
namenode_ip=192.168.220.160



rm -rf $DIR/build/ECCoordinate.log $DIR/build/fileMeta

for((i=1;i<=$host_num;i++));
do
{
	if [[ $i -ge 0 && $i -lt 10 ]]
	then
		host=${NODE_NAME}0${i}
		ip=192.168.220.16$i
	else
		host=${NODE_NAME}$i
		ip=192.168.220.16$i
	fi

    ssh $USER@$host "rm -rf /home/openec/lmq_openec/build/repair.log"
    ssh $USER@$host "rm -rf /home/openec/lmq_openec/build/ECAgent.log"
} &
done
wait
