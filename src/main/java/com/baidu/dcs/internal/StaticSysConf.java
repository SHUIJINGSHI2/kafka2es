package com.baidu.dcs.internal;

import com.baidu.dcs.KafkaSyncController;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author: sunchenjiao
 */
public class StaticSysConf {
    private static  Properties properties = null;

    public static String getProperty(String key) throws IOException {
        if (null == properties) {
            InputStream in = new BufferedInputStream(new FileInputStream(KafkaSyncController.staticConfPath));
            properties = new Properties();
            properties.load(in);
        }
        return properties.getProperty(key);
    }
}
