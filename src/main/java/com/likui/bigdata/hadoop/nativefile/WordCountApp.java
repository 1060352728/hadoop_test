package com.likui.bigdata.hadoop.nativefile;

import com.likui.bigdata.hadoop.mapreduce.WordCountMapper;
import com.likui.bigdata.hadoop.mapreduce.WordCountReduces;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Auther: likui
 * @Date: 2019/5/8 21:29
 * @Description:
 */
public class WordCountApp {

    public static void main(String[] args) throws Exception {

        System.setProperty("hadoop.home.dir", "D:\\jee_environment\\hadoop-2.6.0-cdh5.15.1");

        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);

        job.setJarByClass(WordCountApp.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduces.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\java_workspace\\bigdata_workspace\\hadoop_test\\input"));
        FileOutputFormat.setOutputPath(job ,new Path("D:\\java_workspace\\bigdata_workspace\\hadoop_test\\output"));

        boolean flag = job.waitForCompletion(true);

        System.out.println(flag);
    }
}
