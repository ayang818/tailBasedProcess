package com.ayang818.middleware.tailbase.utils;

import java.io.*;

/**
 * @author 杨丰畅
 * @description TODO
 * @date 2020/6/20 22:32
 **/
public class TagGene {
    public static void main(String[] args) throws IOException {
        System.out.println("start...");
        String path = "D:\\middlewaredata\\trace1.data";
        BufferedReader reader = new BufferedReader(new FileReader(path));
        String line;
        File file = new File("D:\\middlewaredata\\tags.data");
        file.createNewFile();
        BufferedWriter writer = new BufferedWriter(new FileWriter(file, true));
        while ((line = reader.readLine()) != null) {
            String[] data = line.split("\\|");
            writer.write(data[data.length - 1] + "\n");
        }
        System.out.println("end...");
    }
}
