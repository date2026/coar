host_num=9
USER=root
NODE_NAME=node
redis-cli flushall; killall ECCoordinator
for((i=1;i<=$host_num;i++));
do
{
	if [[ $i -ge 0 && $i -lt 10 ]]
	then
        	host=${NODE_NAME}0${i}
    	else
            host=${NODE_NAME}$i
    	fi
	ssh $USER@$host "killall ECAgent; redis-cli flushall"
} &
done
wait


for((i=1;i<=1;i++));
do
{
	if [[ $i -ge 0 && $i -lt 10 ]]
	then
        	host=${NODE_NAME}0${i}
    	else
            host=${NODE_NAME}$i
    	fi
	ssh $USER@$host "killall ECClient"
} &
done
wait

