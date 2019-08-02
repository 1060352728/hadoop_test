package com.likui.bigdata.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * @Auther: likui
 * @Date: 2019/5/8 21:29
 * @Description:
 */
public class WordCountApp {

    public static void main(String[] args) throws Exception {

        System.setProperty("hadoop.home.dir", "D:\\jee_environment\\hadoop-2.6.0-cdh5.15.1");
        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.191.1:8020");

        Job job = Job.getInstance(configuration);

        job.setJarByClass(WordCountApp.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduces.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //判断hdfs输出路径是否存在，如果存在删除
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.191.1:8020"),configuration,"root");
        Path outputPath = new Path("/output");
        if(fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
            System.out.println("输出文件系统的路径已存在，删除该路径");
        }

        FileInputFormat.setInputPaths(job, new Path("/input"));
        FileOutputFormat.setOutputPath(job ,outputPath);

        boolean flag = job.waitForCompletion(true);

        System.out.println(flag);
    }
}
