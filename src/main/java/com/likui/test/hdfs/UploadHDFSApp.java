package com.likui.test.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * @Auther: likui
 * @Date: 2019/4/23 21:19
 * @Description: 将本地文件上传至HDFS
 */
public class UploadHDFSApp {

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "D:\\jee_environment\\hadoop-2.6.0-cdh5.15.1");
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.191.1:8020");
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.191.1:8020"),configuration,"root");
        fileSystem.copyFromLocalFile(new Path("C:\\Users\\likui\\Desktop\\hadoop-test-1.0.jar"), new Path("/test/hadoop-test-1.0.jar"));
    }
}
