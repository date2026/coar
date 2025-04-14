export LD_LIBRARY_PATH=/usr/local/ssl/include/openssl:/usr/lib:/usr/local/lib:/usr/lib/pkgconfig:/usr/local/include/wx-2.8/wx:$LD_LIBRARY_PATH
export PKG_CONFIG_PATH=/usr/lib/pkgconfig
export OPENSSL_ROOT_DIR=/usr/local/ssl
export OPENSSL_LIBRARIES=/usr/local/ssl/lib/

PATH=/usr/local/ssl/bin:$PATH

export PATH=$PATH:/usr/local/cmake/bin

# apache maven
export M2_HOME=/home/openec/apache-maven-3.8.5
export PATH=$PATH:$M2_HOME/bin

# java
export JAVA_HOME=/usr/lib/jvm/jdk1.8.0_333
export JRE_HOME=${JAVA_HOME}/jre
export CLASSPATH=.:${JAVA_HOME}/lib:${JRE_HOME}/lib
export PATH=${JAVA_HOME}/bin:$PATH

# hadoop
export HADOOP_SRC_DIR=/home/openec/hadoop-3.0.0-src
export HADOOP_HOME=$HADOOP_SRC_DIR/hadoop-dist/target/hadoop-3.0.0
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar:$HADOOP_CLASSPATH
export CLASSPATH=`hadoop classpath --glob`
export LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:$JAVA_HOME/jre/lib/amd64/server/:/usr/local/lib:$LD_LIBRARY_PATH
export PATH=$PATH:/home/openec/hbase-2.4.18/bin


/home/openec/lmq_openec/build/ECAgent > ./ECAgent.log &