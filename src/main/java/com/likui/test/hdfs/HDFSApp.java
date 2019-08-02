package com.likui.test.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.net.URI;

/**
 * @Auther: likui
 * @Date: 2019/4/23 21:19
 * @Description:
 */
public class HDFSApp {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.191.1:8020"),configuration,"root");
        Boolean flag = fileSystem.mkdirs(new Path("/test"));
        System.out.println(flag);
    }
}
