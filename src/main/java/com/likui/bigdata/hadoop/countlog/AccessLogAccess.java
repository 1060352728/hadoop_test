package com.likui.bigdata.hadoop.countlog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.net.URI;

/**
 * @Auther: likui
 * @Date: 2019/5/13 21:53
 * @Description:
 */
public class AccessLogAccess {

    public static void main(String[] args) throws Exception {

        System.setProperty("hadoop.home.dir", "D:\\jee_environment\\hadoop-2.6.0-cdh5.15.1");
        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.191.1:8020");

        Job job = Job.getInstance(configuration);

        job.setJarByClass(AccessLogAccess.class);

        job.setMapperClass(AccessMapper.class);
        job.setReducerClass(AccessReduce.class);

        //job.setCombinerClass(AccessReduce.class); //添加Combiner操作，节省网络IO
        job.setPartitionerClass(AccessPartitioner.class); //设置自定义分区规则
        job.setNumReduceTasks(3); //设置reduce个数

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Access.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Access.class);

        //判断hdfs输出路径是否存在，如果存在删除
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.191.1:8020"),configuration,"root");
        Path outputPath = new Path("/outputlog");
        if(fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
            System.out.println("输出文件系统的路径已存在，删除该路径");
        }

        FileInputFormat.setInputPaths(job, new Path("/inputlog"));
        FileOutputFormat.setOutputPath(job ,outputPath);

        boolean flag = job.waitForCompletion(true);

        System.out.println(flag);
    }
}
