package com.hadoop.hdfs.snappy.compress;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.lang.System;




public class Main {

	private static final Logger LOG = Logger.getLogger(Main.class);

    private static final String SNAPPY = "org.apache.hadoop.io.compress.SnappyCodec";

    //private static String FS = "hdfs://localhost:8020";
    private static String HDFS_SCHEMA = "";
    
    
    private static String HELP_MSG = "";
    private static String hadoop_home = System.getProperty("hadoop.home.dir");
    private static String file_prefix = System.getProperty("file.prefix");
    
    //private static final String LZO_SUFFIX = ".lzo";
    static Configuration conf = new Configuration();
    static FileSystem fs;
    static {
    	if(hadoop_home == null){
        	hadoop_home = "/usr/local/hadoop";
        }
    	if (file_prefix == null){
    		file_prefix = "";
    	}
        String path = hadoop_home + "/etc/hadoop/";
        conf.addResource(new Path(path + "core-site.xml"));
        conf.addResource(new Path(path + "hdfs-site.xml"));
        conf.addResource(new Path(path + "mapred-site.xml"));
        HDFS_SCHEMA = conf.get("fs.defaultFS");
        //path = "/usr/java/hbase-0.90.3/conf/";
        //conf.addResource(new Path(path + "hbase-site.xml"));
        HELP_MSG = "--------------------------------------------------------------------------------------------" + "\n";
        HELP_MSG += "please input cmd 'hadoop jar hdfs-snappy-compress-0.0.1.jar <input hdfs path> <output hdfs file> '" + "\n";
        HELP_MSG += "-input:\t\t <hdfs path prepare compress dir or file> " + "\n";
        HELP_MSG += "-output:\t\t<hdfs path compressed must be a file path> " + "\n";
        HELP_MSG += "--------------------------------------------------------------------------------------------";
        PropertyConfigurator.configure(path + "log4j.properties");
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("hadoop_home:%s", hadoop_home));
            LOG.debug(String.format("hadoop_conf:%s", path));
            LOG.debug(String.format("fs.defaultFS:%s", HDFS_SCHEMA));
        }
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if (args != null && args.length < 2) {
            System.out.println(HELP_MSG);
            //System.out.println("[stop] application");
            return;
        }

        String in = "";
        String out = "";
        if (!StringUtils.startsWithIgnoreCase(args[0],HDFS_SCHEMA)){
            in = HDFS_SCHEMA + args[0];
        }
        if (!StringUtils.startsWithIgnoreCase(args[1],HDFS_SCHEMA)){
            out = HDFS_SCHEMA + args[1];
        }
        int ret = fileReader(in,out);
        if (ret > 0){
        	System.out.println("[success]");
        }
	}
	
	public static int fileReader(String filepath,String out){
		if (StringUtils.isBlank(filepath)){
			LOG.error(String.format("read path string is blank:%s",filepath));
            return -2;
        }
		//OutputStream outputStream =null;
		byte[] ioBuffer = new byte[64*1024];
		try {
            //FileSystem fs = FileSystem.get(conf);
            Path input = new Path(filepath);

            // 1. check input path exist
            if (!fs.exists(input)) {
            	LOG.error(String.format("read path is blank:%s",filepath));
                return -2;
            }
            if (fs.exists(new Path(out))){
            	LOG.error(String.format("outpus path is not blank:%s",out));
                return -2;
            }	
            FSDataOutputStream output = fs.create(new Path(out));
            // 3. check input path is dir ,if it is ,foreach write
            FileStatus stat = fs.getFileStatus(input);
            CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(Class.forName(SNAPPY), conf);
            CompressionOutputStream outputStream = codec.createOutputStream(new BufferedOutputStream(output));
            if (stat.isFile()) {
            	LOG.error(String.format("read path must be Directory:%s",input));
            	return -2;
            } else if (stat.isDirectory()) {
                FileStatus[] subInputFile = fs.listStatus(input);
                for (FileStatus fileStatus : subInputFile) {
                	if(!fileStatus.getPath().getName().startsWith(file_prefix)){
                		continue;
                	}
                	System.out.println(String.format("%s",fileStatus.getPath().getName()));
                    if (fileStatus.isFile()) {
                    	 //System.out.println(String.format("%s",fileStatus.getPath().getName()));
                    	 FSDataInputStream hdfsInStream = fs.open(fileStatus.getPath());
                    	 int readLen = hdfsInStream.read(ioBuffer);
                    	 //System.out.println(String.format("%d:%s",readLen,ioBuffer));
                    	 while(-1 != readLen){
                    		  outputStream.write(ioBuffer, 0, readLen);  
                    		  readLen = hdfsInStream.read(ioBuffer);
                    	 }
                    } 
                }
                return 1;
            }

        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
		return -2;
	}
	

}
