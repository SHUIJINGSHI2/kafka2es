package com.baidu.dcs;

import com.baidu.dcs.internal.DynamicSysConf;
import com.baidu.dcs.internal.StaticSysConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;

/**
 * this
 *
 * @ClassName: RedisSyncController
 * @Description: 数据同步控制器
 * @author: sunchenjiao
 * @date: 2016年2月22日 下午5:07:47
 */
public class KafkaSyncController {


    public static Logger LOG = LoggerFactory.getLogger(KafkaSyncController.class);

    public static String staticConfPath = "./staticConf.properties";

    public static String dynamicConfPath = "./dynamicConf.properties";

    int mainRoundPeriod = 5;

    public static int countToEs = 5000;

    public static int writeToEsRoundSleep = 100;

    public static boolean needFilter = false;

    public static boolean needWaitAfterEveryRound = false;

    public static int kafkaRountCount = 10;

    public static int esTimeInterval = 0;


    public KafkaSyncController(String staticConfPath) {
        this.staticConfPath = staticConfPath;
        System.out.println(staticConfPath);
        try {
            this.dynamicConfPath = StaticSysConf.getProperty("dynamicConfPath");
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            this.countToEs = Integer.parseInt(StaticSysConf.getProperty("countToEs"));
            this.writeToEsRoundSleep = Integer.parseInt(StaticSysConf.getProperty("writeToEsRoundSleep"));
            this.needFilter = Boolean.parseBoolean(StaticSysConf.getProperty("needFilter"));
            this.needWaitAfterEveryRound = Boolean.parseBoolean(StaticSysConf.getProperty("needWaitAfterEveryRound"));
            this.kafkaRountCount = Integer.parseInt(StaticSysConf.getProperty("kafkaRountCount"));
            this.esTimeInterval = Integer.parseInt(StaticSysConf.getProperty("esTimeInterval"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    // 动态参数，每次主线程轮询时会进行刷新
    private Set<String> topicSet = new TreeSet<String>();

    public static HashMap<String, String> topicDivideTimeMap = new HashMap<>();

    public void startSync() throws IOException {
        // load static config
        mainRoundPeriod = Integer.parseInt(StaticSysConf.getProperty("mainRoundPeriod"));
        while (true) {
            // load dynamic config
            this.topicDivideTimeMap = DynamicSysConf.getProperty();
            int totalThreadCount = this.topicSet.size();
            LOG.info("scan port.conf [totalThreadCount=" + totalThreadCount + "]");

            // 需要同步es的meta
            for (String item : topicDivideTimeMap.keySet()) {
                if (!topicSet.contains(item)) {
                    new KafkaSyncToEsThread((item)).start();
                    this.topicSet.add(item);
                    LOG.info("new Sync Thread [metaName=" + item + "]");
                }
            }
            try {
                Thread.sleep(mainRoundPeriod * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }


    /**
     * @param args
     * @Title: main
     * @Description: 同步入口，命令demo: java -jar xxx.class xxx.jar ./staticConf.properties
     * @return: void
     */
    public static void main(String[] args) throws IOException, InterruptedException {

        KafkaSyncController kafkaSyncController = new KafkaSyncController(args[0]);
        kafkaSyncController.startSync();

    }
}
