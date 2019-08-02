package com.likui.bigdata.hadoop.hdfs;

import java.io.IOException;
import java.util.Properties;

/**
 * @Auther: likui
 * @Date: 2019/5/5 21:01
 * @Description: 读取配置文件的属性
 */
public class ParamsUtils {

    public static Properties properties = new Properties();
    static {
        try {
            properties.load(ParamsUtils.class.getClassLoader().getResourceAsStream("wc.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Properties getProperties() throws Exception {
        return properties;
    }

    public static void main(String[] args) throws Exception {
        System.out.println(getProperties().get("INPUT_PATH"));
    }
}
