# host_num=9
host_num=31
# host_num=63
USER=root
NODE_NAME=node
DIR=/root/lmq_openec
namenode_ip=192.168.0.219
# ips=(192.168.0.220 192.168.0.221 192.168.0.222 192.168.0.223 192.168.0.224 192.168.0.225 192.168.0.226 192.168.0.227 192.168.0.228)

ips=(\
192.168.0.220 192.168.0.221 192.168.0.222 192.168.0.223 192.168.0.224 192.168.0.225 192.168.0.226 192.168.0.227 192.168.0.228 192.168.0.229 \
192.168.0.230 192.168.0.231 192.168.0.232 192.168.0.233 192.168.0.234 192.168.0.235 192.168.0.236 192.168.0.237 192.168.0.238 192.168.0.239 \
192.168.0.240 192.168.0.241 192.168.0.242 192.168.0.243 192.168.0.244 192.168.0.245 192.168.0.246 192.168.0.247 192.168.0.248 192.168.0.249 \
192.168.0.250)

# ips=(\
# 192.168.0.220 192.168.0.221 192.168.0.222 192.168.0.223 192.168.0.224 192.168.0.225 192.168.0.226 192.168.0.227 192.168.0.228 192.168.0.229 \
# 192.168.0.230 192.168.0.231 192.168.0.232 192.168.0.233 192.168.0.234 192.168.0.235 192.168.0.236 192.168.0.237 192.168.0.238 192.168.0.239 \
# 192.168.0.240 192.168.0.241 192.168.0.242 192.168.0.243 192.168.0.244 192.168.0.245 192.168.0.246 192.168.0.247 192.168.0.248 192.168.0.249 \
# 192.168.0.250 \
# 192.168.0.1 192.168.0.2 192.168.0.3 192.168.0.4 192.168.0.5 192.168.0.6 192.168.0.7 192.168.0.8 192.168.0.9 192.168.0.10 \
# 192.168.0.11 192.168.0.12 192.168.0.13 192.168.0.14 192.168.0.15 192.168.0.16 192.168.0.17 192.168.0.18 192.168.0.19 192.168.0.20 \
# 192.168.0.21 192.168.0.22 192.168.0.23 192.168.0.24 192.168.0.25 192.168.0.26 192.168.0.27 192.168.0.28 192.168.0.29 192.168.0.30 \
# 192.168.0.251 192.168.0.252)


for((i=1;i<=$host_num;i++));
do
{
	if [[ $i -ge 0 && $i -lt 10 ]]
	then
		host=${NODE_NAME}0${i}
	else
		host=${NODE_NAME}$i
	fi
    ip=${ips[i-1]}
    # scp /root/hadoop-3.0.0-src/hadoop-dist/target/hadoop-3.0.0/etc/hadoop/workers $USER@$host:/root/hadoop-3.0.0-src/hadoop-dist/target/hadoop-3.0.0/etc/hadoop/
    # scp -r ~/spark-2.4.0/conf/slaves $USER@$host:~/spark-2.4.0/conf/
    # scp /root/hadoop-3.0.0-src/hadoop-dist/target/hadoop-3.0.0/etc/hadoop/yarn-site.xml $USER@$host:/root/hadoop-3.0.0-src/hadoop-dist/target/hadoop-3.0.0/etc/hadoop/
    # scp /root/hadoop-3.0.0-src/hadoop-dist/target/hadoop-3.0.0/etc/hadoop/hdfs-site.xml $USER@$host:/root/hadoop-3.0.0-src/hadoop-dist/target/hadoop-3.0.0/etc/hadoop/
    # scp -r ~/spark-2.4.0/conf/spark-defaults.conf $USER@$host:~/spark-2.4.0/conf/
    # scp -r /root/apache-maven-3.8.5.tar.gz /root/gf-complete.tar.gz /root/hiredis-1.0.2.tar.gz /root/jdk-8u333-linux-x64.tar.gz /root/protobuf-2.5.0.tar.gz /root/cmake-3.22.4.tar.gz /root/hadoop-3.0.0-src.tar.gz /root/isa-l-2.30.0.tar.gz /root/redis-3.2.8.tar.gz lmq_openec  $USER@$host:/root/
    # scp setup.sh $USER@$host:/root/
    # scp /root/spark-2.4.0.tar.gz $USER@$host:/root/
    # scp /root/hadoop-3.0.0-src/hadoop-dist/target/hadoop-3.0.0/etc/hadoop/hadoop-env.sh /root/hadoop-3.0.0-src/hadoop-dist/target/hadoop-3.0.0/etc/hadoop/core-site.xml /root/hadoop-3.0.0-src/hadoop-dist/target/hadoop-3.0.0/etc/hadoop/hdfs-site.xml /root/hadoop-3.0.0-src/hadoop-dist/target/hadoop-3.0.0/etc/hadoop/workers   /root/hadoop-3.0.0-src/hadoop-dist/target/hadoop-3.0.0/etc/hadoop/yarn-site.xml  /root/hadoop-3.0.0-src/hadoop-dist/target/hadoop-3.0.0/etc/hadoop/mapred-site.xml   $USER@$host:/root/hadoop-3.0.0-src/hadoop-dist/target/hadoop-3.0.0/etc/hadoop/
    
    scp $DIR/script/collect_sar.sh $USER@$host:$DIR/script/
    scp $DIR/script/start_agent.sh $USER@$host:$DIR/script/
    # scp $DIR/script/parse_sar.py $USER@$host:$DIR/script/
    # scp -r $DIR/ecdag $USER@$host:$DIR/
    scp $DIR/ecdag/monitor.py $USER@$host:$DIR/ecdag/
    scp $DIR/script/tc.sh $USER@$host:$DIR/script/
    # scp $DIR/build/ECTest  $USER@$host:$DIR/build/
	scp $DIR/build/ECAgent $DIR/build/ECClient  $USER@$host:$DIR/build/
	scp $DIR/conf/1.json $USER@$host:$DIR/conf/
	ssh $USER@$host "sed -i 's#\"local_ip\": \"192\.168\.0\.219\"#\"local_ip\": \"'\"$ip\"'\"#g' $DIR/conf/1.json"
    # ssh $USER@$host "pip3 install psutil"
} &
done
wait

scp $DIR/script/design3/* node01:$DIR/build/