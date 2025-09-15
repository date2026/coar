host_num=9
USER=root
NODE_NAME=node
for((i=1;i<=$host_num;i++));
do
{
	if [[ $i -ge 0 && $i -lt 10 ]]
	then
        	host=${NODE_NAME}0${i}
    	else
            host=${NODE_NAME}$i
    	fi
	ssh $USER@$host "killall python3"
} &
done
wait
