package com.likui.bigdata.hadoop.hdfs;

import java.util.HashMap;
import java.util.Map;

/**
 * @Auther: likui
 * @Date: 2019/5/5 20:31
 * @Description:
 */
public class MapContext {

    private Map<String, String> cacheMap = new HashMap<String, String>();

    public Map<String, String> getCacheMap() {
        return cacheMap;
    }

    public void write (String key, String value){
        cacheMap.put(key, value);
    }

    public String get (String key){
        return cacheMap.get(key);
    }
}
