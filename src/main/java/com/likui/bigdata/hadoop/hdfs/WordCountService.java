package com.likui.bigdata.hadoop.hdfs;

import org.apache.commons.lang.StringUtils;

/**
 * @Auther: likui
 * @Date: 2019/5/5 20:37
 * @Description:
 */
public class WordCountService implements WordCountMapper {

    public void map(String line, MapContext mapContext) {
        String[] words = line.split(",");
        for (String word : words) {
            String value = mapContext.get(word);
            if (StringUtils.isBlank(value)) {
                mapContext.write(word, "1");
            } else {
                mapContext.write(word, String.valueOf(Integer.parseInt(value) + 1));
            }
        }
    }

}
