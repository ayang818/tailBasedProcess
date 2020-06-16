package com.ayang818.middleware.tailbase.common;

import java.util.List;
import java.util.Map;

/**
 * <p>traceDetails</p>
 *
 * @author : chengyi
 * @date : 2020-06-16 21:03
 **/
public class Resp {
    int dataPos;
    Map<String, List<String>> data;

    public Resp(int dataPos, Map<String, List<String>> data) {
        this.dataPos = dataPos;
        this.data = data;
    }

    public int getDataPos() {
        return dataPos;
    }

    public Map<String, List<String>> getData() {
        return data;
    }
}
