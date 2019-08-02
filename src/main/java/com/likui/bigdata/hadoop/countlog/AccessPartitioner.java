package com.likui.bigdata.hadoop.countlog;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Auther: likui
 * @Date: 2019/5/14 20:29
 * @Description:
 */
public class AccessPartitioner extends Partitioner<Text, Access> {

    @Override
    public int getPartition(Text text, Access access, int numPartitions) {
        if(text.toString().startsWith("13")){
            return 0;
        }else if(text.toString().startsWith("15")){
            return 1;
        }else{
            return 2;
        }
    }
}
