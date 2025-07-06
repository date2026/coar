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

    # scp -r ~/spark-2.4.0/conf/spark-env.sh $USER@$host:~/spark-2.4.0/conf/
    # scp -r ~/spark-2.4.0/conf/spark-defaults.conf $USER@$host:~/spark-2.4.0/conf/
    scp $DIR/script/collect_sar.sh $USER@$host:$DIR/script/
    # scp -r $DIR/ecdag $USER@$host:$DIR/
    scp $DIR/ecdag/monitor.py $USER@$host:$DIR/ecdag/
    scp $DIR/script/tc.sh $USER@$host:$DIR/script/
    scp $DIR/build/ECTest  $USER@$host:$DIR/build/
	scp $DIR/build/ECAgent $DIR/build/ECClient  $USER@$host:$DIR/build/
	scp $DIR/conf/1.json $USER@$host:$DIR/conf/
	ssh $USER@$host "sed -i 's#\"local_ip\": \"192\.168\.220\.160\"#\"local_ip\": \"'\"$ip\"'\"#g' $DIR/conf/1.json"
} &
done
wait
