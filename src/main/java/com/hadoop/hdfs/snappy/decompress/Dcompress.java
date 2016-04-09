package com.hadoop.hdfs.snappy.decompress;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.hadoop.hdfs.snappy.compress.Main;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.lang.System;

public class Dcompress {

	private static final Logger LOG = Logger.getLogger(Main.class);

    private static final String SNAPPY = "org.apache.hadoop.io.compress.SnappyCodec";

    //private static String FS = "hdfs://localhost:8020";
    private static String HDFS_SCHEMA = "";
    
    private static String hadoop_home = System.getProperty("hadoop.home.dir");

    
    //private static final String LZO_SUFFIX = ".lzo";
    static Configuration conf = new Configuration();
    static FileSystem fs;
    static {
    	if(hadoop_home == null){
        	hadoop_home = "/usr/local/hadoop";
        }
        String path = hadoop_home + "/etc/hadoop/";
        conf.addResource(new Path(path + "core-site.xml"));
        conf.addResource(new Path(path + "hdfs-site.xml"));
        conf.addResource(new Path(path + "mapred-site.xml"));
        HDFS_SCHEMA = conf.get("fs.defaultFS");
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
    
    
    public static int decompress(String in,String out){
		if (StringUtils.isBlank(in)){
			LOG.error(String.format("read path string is blank:%s",in));
            return -2;
        }
		//OutputStream outputStream =null;
		byte[] ioBuffer = new byte[64*1024];
		try {
            //FileSystem fs = FileSystem.get(conf);
            Path input = new Path(in);
            Path output = new Path(out);

            // 1. check input path exist
            if (!fs.exists(input)) {
            	LOG.error(String.format("read path is blank:%s",in));
                return -2;
            }
            if (fs.exists(output)){
            	LOG.error(String.format("outpus path is not blank:%s",out));
                return -2;
            }	
            FileStatus stat = fs.getFileStatus(input);
            if (stat.isDirectory()) {
            	LOG.error(String.format("read path must be file:%s",input));
            	return -2;
            }
            if (stat.isFile()){
            	FSDataOutputStream outputStream = fs.create(output);
                FSDataInputStream FSinputStream = fs.open(input);
                // 3. check input path is dir ,if it is ,foreach write
               
                CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(Class.forName(SNAPPY), conf);
                CompressionInputStream inputStream = codec.createInputStream(new BufferedInputStream(FSinputStream));
                int readlen = 0;
                int max_len = 64*1024;
                readlen = inputStream.read(ioBuffer,0,max_len);
                while(readlen>0) {
                	outputStream.write(ioBuffer, 0, readlen);
                	readlen = inputStream.read(ioBuffer,0,max_len);
                }
            	return 1;
            }
            
 

        } catch (Exception e) {
        	LOG.error(e.getMessage());
        }
		return -2;
	}
    
}
