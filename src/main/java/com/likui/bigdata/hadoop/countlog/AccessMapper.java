package com.likui.bigdata.hadoop.countlog;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Auther: likui
 * @Date: 2019/5/13 21:40
 * @Description:
 */
public class AccessMapper extends Mapper<LongWritable, Text, Text, Access> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().split("\t");
        String phone = lines[1];
        long up = Long.parseLong(lines[lines.length-3]);
        long down = Long.parseLong(lines[lines.length-2]);
        context.write(new Text(phone), new Access(phone, up, down));
    }
}
