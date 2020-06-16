package com.ayang818.middleware.tailbase.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ayang818.middleware.tailbase.common.Caller;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * <p></p>
 *
 * @author : chengyi
 * @date : 2020-06-16 20:48
 **/
public class Test {
    public static void main(String[] args) {
        List<Caller.PullDataBucket> list = new ArrayList<>();
        Set<String> set = new HashSet<>();
        set.add("asdassd");
        list.add(new Caller.PullDataBucket(set, 1));
        Caller caller = new Caller(1, list);
        String s = JSON.toJSONString(caller);
        System.out.println(s);

        Caller caller1 = JSON.parseObject(s, new TypeReference<Caller>() {
        });
    }
}
