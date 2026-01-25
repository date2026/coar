export JAVA_HOME=/usr/lib/jvm/jdk1.8.0_333
export SPARK_MASTER_HOST=master
export SPARK_WORKER_MEMORY=6g
export SPARK_WORKER_CORES=4
export SPARK_EXECUTOR_MEMORY=5g
export HADOOP_HOME=/root/hadoop-3.0.0-src/hadoop-dist/target/hadoop-3.0.0
export HADOOP_CONF_DIR=/root/hadoop-3.0.0-src/hadoop-dist/target/hadoop-3.0.0/etc/hadoop
export YARN_CONF_DIR=/root/hadoop-3.0.0-src/hadoop-dist/target/hadoop-3.0.0/etc/hadoop
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/lib/jvm/jdk1.8.0_333/jre/lib/amd64
export SPARK_HISTORY_OPTS="-Dspark.history.ui.port=18080 -Dspark.history.retainedApplications=3 -Dspark.history.fs.logDirectory=hdfs:///history"