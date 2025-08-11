#! /bin/bash
 
host_num=9
host_address=192.168.0.219          
host_hostname=node     
host_username=root           
ips=(192.168.0.220 192.168.0.221 192.168.0.222 192.168.0.223 192.168.0.224 192.168.0.225 192.168.0.226 192.168.0.227 192.168.0.228)
sudo tc qdisc add dev eth0 root handle 1:0 htb default 2

# sudo tc class add dev eth0 parent 1:0 classid 1:1 htb rate 500Mbit ceil 500Mbit burst 0
# sudo tc class add dev eth0 parent 1:0 classid 1:1 htb rate 1000Mbit ceil 1000Mbit burst 0
# sudo tc class add dev eth0 parent 1:0 classid 1:1 htb rate 2000Mbit ceil 2000Mbit burst 0
# sudo tc class add dev eth0 parent 1:0 classid 1:1 htb rate 4000Mbit ceil 4000Mbit burst 0
# sudo tc class add dev eth0 parent 1:0 classid 1:1 htb rate 6000Mbit ceil 6000Mbit burst 0
# sudo tc class add dev eth0 parent 1:0 classid 1:1 htb rate 8000Mbit ceil 4000Mbit burst 0
# sudo tc class add dev eth0 parent 1:0 classid 1:1 htb rate 10000Mbit ceil 10000Mbit burst 0
sudo tc class add dev eth0 parent 1:0 classid 1:1 htb rate 80000Mbit ceil 80000Mbit burst 0

for((i=1;i<=$host_num;i++)); do
    host_ip=${ips[i-1]}
    sudo tc filter add dev eth0 parent 1:0 prior 1 protocol ip u32 match ip dst ${host_ip} classid 1:1
done
wait