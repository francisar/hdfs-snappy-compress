# hdfs-snappy-compress
hdfs-snappy压缩


# 功能
将hdfs文件系统指定目录下，指定前缀（可不指定前缀）的所有文本文件，通过snappy压缩为一个文件，存放在指定路径



# 编译
mvn clean package


#安装

将包放置在hadoop_home下lib文件夹下
script目录中提供了执行的shell脚本，脚本要先进行如下环境变量的设置
<!--lang:shell-->

    JAVA_HOME=
    SNAPPY_LOG_FILE=hdfs-snappy-compress.log
    HADOOP_HOME=
    LOG_LEVEL=WARN
    FILE_PREFIX=
使用脚本，需要在hadoop 配置文件core-site.xml中加入如下配置

<!--lang:XML-->
    <property>
    <name>fs.hdfs.impl</name>
    <value>org.apache.hadoop.hdfs.DistributedFileSystem</value>
    <description>The FileSystem for hdfs: uris.</description>
    </property>


# 使用

- 使用jar包执行
<code>
hadoop jar hdfs-snappy-compress-0.0.1.jar <input hdfs path> <output hdfs file>
</code>
- 使用脚本执行
<code>
hdfs_snappy.sh [input hdfs dir] [output hdfs file]
</code>

