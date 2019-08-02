package com.likui.bigdata.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Auther: likui
 * @Date: 2019/5/8 20:55
 * @Description:
 *
 * 由于需要网络传输，所以都用序列化的类型
 * LongWritable：输入的统计类型
 * Text：输入的行为字符串类型
 * Text：出现的单词输出
 * IntWritable：输出的次数，出现一次赋值一次1
 *
 * key：偏移量，从0开始，下一次的值为前面所有行的字符串长度之和
 * value：每一行的数据
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(",");
        for (String word : words) {
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
