# Preparation

- cmake v3.1 or higher

  `sudo apt-get install cmake`

- g++ v4.8.4

  `sudo apt-get install g++`

- Redis v3.2.8 or higher

  Download and install **redis-3.2.8.tar.gz**.

  `tar -zxvf redis-3.2.8.tar.gz`

  `cd redis-3.2.8.tar.gz`

  `make`

  `sudo make install`

  Configure redis to be remotely  accessible.

  `sudo service redis_6379 stop`

  Edit */etc/redis/6379.conf*. Find the line with <u>bind 127.0.0.0</u> and modify it with <u>bind 0.0.0.0</u>

  `sudo service redis_6379 start`

- hiredis

  Download and install hiredis

  `tar -zxvf hiredis.tar.gz`

  `cd hiredis`
  `make` 

  `sudo make install`

- gf-complete v1.03
    Download and install gf-complete.tar.gz.

  `tar -zxvf gf-complete.tar.gz`

  `cd gf-complete`

  `./autogen.sh`

  `./configure`

  `make`

  `sudo make install`

- ISA-L v2.14.0 or higher

  Download and install isa-2.14.0.tar.gz.

  `tar -zxvf isa-l-2.14.0.tar.gz`

  `./autogen.sh`

  `./configure`

  `make`

  `sudo make install` 

- maven v3.5.0 or higher 

  Download and install apache-maven-3.5.0-bin.tar.gz.

  `tar -zxvf apache-maven-3.5.0-bin.tar.gz`

  Set the environment variables.

  `export M2_HOME=/home/hybridlazy/apache-maven-3.5.0`

  `export PATH=$PATH:$M2_HOME/bin`

- java8

- HDFS

  Download and install HDFS

  Set the environment variables.

  `export HADOOP_SRC_DIR=[your_path_to_hadoop]`

  `export HADOOP_HOME=$HADOOP_SRC_DIR/hadoop-dist/target/hadoop-3.0.0`

  `export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH`

  `export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar:$HADOOP_CLASSPATH`

  `export CLASSPATH=hadoop classpath -globa`

  `export LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:$JAVA_HOME/jre/lib/`

  `amd64/server/:/usr/local/lib:$LD_LIBRARY_PATH`


- Spark

  Download and install spark-2.4.0

  Set the environment variables.

  `export SPARK_HOME=[your_path_to_spark-2.4.0]`

  `export SPARK_MASTER_HOST=yarn`




# HDFS Configuration

Example Architecture:

| IP/hostname |    HDFS   |      ROLE     |
| ----------- | --------- | --------------|
| master      | NameNode  | Coordinator   |
| node01      | DataNode  | Agent         |
| node02      | DataNode  | Agent         |
| nodexx      | DataNode  | Agent         |

We provide sample configuration files under conf for HDFS. Here, we show some of the fields related to the integration. You may configure HDFS by modifying files under `$HADOOP_HOME/etc/hadoop` and distribute the configuration files to all the nodes in the testbed.

- core-site.xml

| Field   |     Default     | Description      |
| ------- | ----------------| -----------------|
| fs.defaultFS   | hdfs://192.168.0.1:9000   | NameNode configuration. |
| hadoop.tmp.dir | /root/hadoop-3.0.0-src/hadoop-dist/target/hadoop-3.0.0/data/ | Base directory for hdfs3 temporary directories. |


- workers


| Field   | Default        | Description        |
| --------| -------------- |------------------- |
|  \      |   192.168.0.x  | IPs of NameNodes.  |



To start HDFS, we run the following commands in the NameNode.

`hdfs namenode -format`

`start-dfs.sh`


# Spark Configuration


Example Architecture:

| IP/hostname |    Spark  |      ROLE     |
| ----------- | --------- | --------------|
| master      | Master    | Coordinator   |
| node01      | Worker    | Agent         |
| node02      | Worker    | Agent         |
| nodexx      | Worker    | Agent         |


We provide sample configuration files under conf for Spark. You may configure Spark by modifying files under `$SPARK_HOME/conf`.



- slaves

| Field   | Default        | Description        |
| --------| -------------- |------------------- |
|  \      |  192.168.x.x  | IPs of Workers.     |





To start Spark, we run the following commands in the Master.

`start-yarn.sh`

`bash [your_path_to_spark]/sbin/start-all.sh`




# Run

Please make sure that you have configured HDFS and Spark successfully and correctly.


### 1. Start foreground workloads

Start foreground distribute data analytics workloads by Hibench. Use K-means clustering as an example.

`bash [your_path_to_hibench]/bin/workloads/ml/kmeans/spark/run.sh`



### 2. Start monitor

start monitors in agents to monitor node's network and computational resources.

`bash [your_path]/ecdag/start_monitor.sh`

### 3. Start coordinator and agents


Then you can run coordinator and agents by the script `start.sh`.

`bash [your_path]/script/start.sh`

### 4. Write

Write file to HDFS by the follwing command.

`[your_path]/build/ECClient write [your_path_to_file] [your_path_in_hdfs] [your_poolname] [your_file_size_in_MB]`

After the file is write, you can read it by the following command.

`[your_path]/build/ECClient read [your_path_in_hdfs] [your_path]/build/[your_path_to_fetch]`


### 5. Encode

After the file is write, encode by the following command. Herer,  `your_path_of_encode_config` is your file path to the config file of encode operations, see `[your_path]/conf/640_n_14_k_10/ecdag_encode_640_0` for example.


`[your_path]/build/ECClient encode [your_path_in_hdfs] [your_path_of_encode_config]`


### 6. Repair/Decode

After the file is encoded, run the following command to trigger a repair operation with coar.

```
python3 run_ecdag.py
```

Then input the repair command parameters.
```
--type [repair_scheme] 
--filename [your_file_path_in_hdfs] 
--failed_node_id [fail_node_id]
--src_node_ids [node_ids_of_this_stripe] 
--new_ids [node_ids_as_a_new_node] 
--all_node_ids [all_node_ids] \
--obj_ids [object_ids_of_this_stripe] 
--row_ids [row_ids_of_the_objects]
--object_size [object_size_in_byte] 
--ec_info [your_path]/build/ec_info \
--output [your_path]/build/input_640MB_ecdag_temp

```

Parameter meanings are as follows

| field           |  description                                                                            |           
| -----------     | ---------                                                                               |
| `type`          | Repair scheme, e.g., coar.                                                                          |
| `filename`      | Input file path stored in HDFS.                                                         |
| `failed_node_id`| The failed node id.                                                                     |
| `new_ids`| New node id to store the repaired chunk.                                                                     |
| `src_node_ids`  | Surviving node ids.                                                                     |
| `all_node_ids`  | All nodes of this stripe.                                                               |
| `obj_ids`       | Object ids of this stripe. They are determined when written                             |
| `row_ids`       | Row ids of the objects of this stripe. Start from 1 and increase by 1 for each object.  |
| `object_size`   | Object size in byte.                                                                    |
| `ec_info`       | File path that stores EC parameters.                                                    |
| `output`        | File path that decode command dumps to. Just used by Coordinator                        |

A example is as follows.

```
--type crar \
--filename /input_640MB \
--failed_node_id 10 \
--src_node_ids 1 2 3 4 5 6 7 8 9 11 12 13 14 \
--new_ids 15 \
--all_node_ids 1 2 3 4 5 6 7 8 9 10 11 12 13 14 \
--obj_ids 0 1 2 3 4 5 6 7 8 9 4095 4094 4093 4092 \
--row_ids 1 2 3 4 5 6 7 8 9 10 11 12 13 14 \
--object_size 67108864 \
--ec_info ./build/ec_info \
--output ./build/input_640MB_ecdag_temp
```


### 7. Stop

You can stop the coordinator and all agents by the script *stop.sh*. 

`bash [your_path]/script/stop.sh`
