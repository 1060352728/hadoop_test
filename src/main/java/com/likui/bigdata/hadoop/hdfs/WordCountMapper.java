package com.likui.bigdata.hadoop.hdfs;

/**
 * @Auther: likui
 * @Date: 2019/5/5 20:36
 * @Description:
 */
public interface WordCountMapper {

    public void map (String line, MapContext mapContext);
}
