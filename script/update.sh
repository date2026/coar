host_num=9
USER=openec
NODE_NAME=node
DIR=/home/openec/lmq_openec
namenode_ip=192.168.220.160
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

	# scp -r $DIR $USER@$host:/home/openec/


	scp $DIR/build/ECAgent $DIR/build/ECClient $DIR/build/HDFSDemo $DIR/build/ECTest $DIR/build/input_16MB_random $USER@$host:$DIR/build/
	scp $DIR/conf/1.json $USER@$host:$DIR/conf/
	ssh $USER@$host "sed -i 's#\"local_ip\": \"192\.168\.220\.160\"#\"local_ip\": \"'\"$ip\"'\"#g' $DIR/conf/1.json"
} &
done
wait
