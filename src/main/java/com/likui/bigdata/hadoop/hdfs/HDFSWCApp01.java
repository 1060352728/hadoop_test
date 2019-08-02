package com.likui.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.Set;

/**
 * @Auther: likui
 * @Date: 2019/5/5 20:07
 * @Description:
 * 使用HDFS API 完成wordcount统计
 * 1：读取文件
 * 2：统计词频：按行读取
 * 3：将处理结果缓存起来
 * 4：将结果输出HDFS中
 */
public class HDFSWCApp01 {

    public static void main(String[] args) throws Exception {
        //1：读取文件
        Path input = new Path("/hello.txt");
        //要操作的文件系统
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.191.1:8020"), new Configuration(), "root");
        //获取文件，不迭代
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(input, false);
        //用于缓存读取的数据，提供读写方法
        MapContext mapContext = new MapContext();
        while (iterator.hasNext()) {
            LocatedFileStatus file = iterator.next();
            FSDataInputStream fsDataInputStream = fs.open(file.getPath());
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream));
            String line = "";
            WordCountService wordCountService = new WordCountService();
            while ((line = bufferedReader.readLine()) != null) {
                //业务处理
                wordCountService.map(line, mapContext);
            }
            bufferedReader.close();
            fsDataInputStream.close();
        }
        //使用map将结果缓存起来
        Map<String, String> map = mapContext.getCacheMap();
        //4：将结果输出HDFS中
        Path output = new Path("/");
        FSDataOutputStream out = fs.create(new Path(output, new Path("wc.txt")));
        Set<Map.Entry<String, String>> entries = map.entrySet();
        for(Map.Entry<String, String> entry : entries){
            out.writeUTF(entry.getKey() + "\t" + entry.getValue() + "\n");
        }
        out.close();
        fs.close();
    }
}
