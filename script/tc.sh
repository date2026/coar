#! /bin/bash
 
host_num=9
host_address=192.168.220.16          
host_hostname=node     
host_username=openec                 

sudo tc qdisc add dev ens33 root handle 1:0 htb default 2

# sudo tc class add dev ens33 parent 1:0 classid 1:1 htb rate 1000Mbit ceil 1000Mbit burst 0
# sudo tc class add dev ens33 parent 1:0 classid 1:1 htb rate 2000Mbit ceil 2000Mbit burst 0
# sudo tc class add dev ens33 parent 1:0 classid 1:1 htb rate 4000Mbit ceil 4000Mbit burst 0
sudo tc class add dev ens33 parent 1:0 classid 1:1 htb rate 80000Mbit ceil 80000Mbit burst 0

for((i=1;i<=$host_num;i++)); do
    host_ip=${host_address}${i}
    sudo tc filter add dev ens33 parent 1:0 prior 1 protocol ip u32 match ip dst ${host_ip} classid 1:1
done
wait