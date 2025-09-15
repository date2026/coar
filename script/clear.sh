host_num=9
USER=root
NODE_NAME=node
DIR=/root/coar

rm -rf $DIR/build/ECCoordinator.log $DIR/build/fileMeta

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

    ssh $USER@$host "rm -rf $DIR/build/repair.log"
    ssh $USER@$host "rm -rf $DIR/build/ECAgent.log"
    ssh $USER@$host "sudo tc qdisc delete dev eth0 root"
} &
done
wait
