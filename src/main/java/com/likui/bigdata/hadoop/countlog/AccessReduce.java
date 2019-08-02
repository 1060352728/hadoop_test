package com.likui.bigdata.hadoop.countlog;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * @Auther: likui
 * @Date: 2019/5/13 21:48
 * @Description:
 */
public class AccessReduce extends Reducer<Text, Access, NullWritable, Access> {

    @Override
    protected void reduce(Text key, Iterable<Access> values, Context context) throws IOException, InterruptedException {
        long ups = 0;
        long downs = 0;
        for (Access value : values) {
            ups += value.getUp();
            downs += value.getDown();
        }
        context.write(NullWritable.get(), new Access(key.toString(), ups, downs));
    }
}
