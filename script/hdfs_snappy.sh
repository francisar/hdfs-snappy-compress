#!/bin/bash

JAVA_HOME=
SNAPPY_LOG_FILE=hdfs-snappy-compress.log
HADOOP_HOME=
LOG_LEVEL=WARN
FILE_PREFIX=
function Usage()
{
    echo "Usage:$0 [-c|-d] [input hdfs dir] [output hdfs file]"
    echo "      eg:$0 /user   /tmp/access.snappy"
}

if [ $# -ne 3 ];then
    Usage
    exit 1
fi 

action=$1
input=$2
output=$3


[ "$action" != "-d" ] && [ "$action" != "-c" ] && Usage & exit 1

$JAVA_HOME/bin/java -Dfile.prefix=$FILE_PREFIX -Djava.library.path=:/usr/java/packages/lib/amd64:/usr/lib64:/lib64:/lib:/usr/lib:$HADOOP_HOME/lib/native -Dhadoop.home.dir=$HADOOP_HOME -Dhadoop.root.logger=$LOG_LEVEL,console,DRFA -Dhadoop.log.file=hdfs-snappy-compress.log -jar $HADOOP_HOME/lib/hdfs-snappy-compress-0.0.1.jar $1 $2 $3
