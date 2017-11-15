package com.baidu.dcs.internal;

import com.baidu.dcs.KafkaSyncController;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

/**
 * Created by sunchenjiao on 2017/3/10.
 */
public class DynamicSysConf {

    public static HashMap<String, String> getProperty() {
        HashMap<String, String> topicDivideTime = new HashMap<String, String>();
        File file = new File(KafkaSyncController.dynamicConfPath);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            while ((tempString = reader.readLine()) != null) {

                // 过滤注释
                if (tempString.startsWith("#") || tempString.startsWith("//")) {
                    continue;
                }
                String[] topics = tempString.trim().split(":");

                // 异常数据
                if (topics.length != 2) {
                    continue;
                }
                topicDivideTime.put(topics[0].trim(), topics[1].trim());
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return topicDivideTime;
    }
}
