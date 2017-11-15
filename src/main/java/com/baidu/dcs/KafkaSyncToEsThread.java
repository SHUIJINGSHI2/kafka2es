package com.baidu.dcs;

import com.baidu.dcs.internal.StaticSysConf;
import com.baidu.dcs.util.EsUtil;
import net.sf.json.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;

/**
 * redis同时同步到UDW和ES线程
 * TODO：需要进行ES同步和redis同步方法的拆分
 *
 * @Description: 同步线程
 * @author: sunchenjiao
 * @date: 2016年2月22日 下午5:07:27
 */
public class KafkaSyncToEsThread extends Thread {

    public static Logger LOG = LoggerFactory.getLogger(KafkaSyncToEsThread.class);

    private String metaName;

    public KafkaSyncToEsThread(String metaName) {
        LOG.info("create sync thread:[metaName= " + metaName + "]");
        this.metaName = metaName;
    }


    public void run() {
        KafkaConsumer<String, String> consumer;

        Properties props = new Properties();
        try {
            props.put("bootstrap.servers", StaticSysConf.getProperty("kafkaServers"));
            props.put("group.id", StaticSysConf.getProperty("kafkaGroupId"));
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("enable.auto.commit", "false");
            props.put("request.timeout.ms", "200000");
            props.put("session.timeout.ms", "300000");
            props.put("request.timeout.ms", "300001");
        } catch (IOException e) {
            LOG.error("Failed to parser StaticSysConf!");
            e.printStackTrace();
        }
        consumer = new KafkaConsumer(props);

        List<String> topics = new ArrayList<>();
        topics.add(metaName);

        consumer.subscribe(topics);
        int readKafkaTimeout = 20;
        while (true) {
            try {
                readKafkaTimeout = Integer.parseInt(StaticSysConf.getProperty("readKafkaTimeout"));
            } catch (IOException e) {
                e.printStackTrace();
            }
            List<String> contentList = new ArrayList<>();
            for (int i = 0; i < KafkaSyncController.kafkaRountCount; i++) {

                ConsumerRecords<String, String> records = consumer.poll(readKafkaTimeout);

                for (ConsumerRecord<String, String> record : records) {
                    // 去除logstash自带的参数，只保留用户输出的原始数据

                    if (KafkaSyncController.needFilter) {
                        JSONObject recordValue = JSONObject.fromObject(record.value());
                        if (recordValue.containsKey("message")) {
                            try {
                                String tmp = recordValue.getString("message");
                                JSONObject messageJsonTmp = JSONObject.fromObject(tmp);
                                contentList.add(messageJsonTmp.toString());
                            } catch (Exception e) {
                                LOG.error("Not valid json, abandon [data:" + recordValue + "]");
                                e.printStackTrace();
                            }
                        } else {
                            LOG.error("Log not contain key['message'], abandon [" + recordValue + "]");
                        }
                    } else {
                        contentList.add(record.value());
                    }
                }

                if (contentList.size() > KafkaSyncController.countToEs) {
                    save2es(contentList);
                    consumer.commitAsync();
                    contentList.clear();
                    break;
                }
            }
            if (contentList.size() > 0) {
                save2es(contentList);
                consumer.commitAsync();
                contentList.clear();
            }

            // 每个循环 sleep
            if (KafkaSyncController.needWaitAfterEveryRound) {
                try {
                    Thread.sleep(KafkaSyncController.writeToEsRoundSleep);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }


        }
    }

    public void save2es(List<String> contentList) {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, calendar.get(Calendar.HOUR_OF_DAY) + KafkaSyncController.esTimeInterval);
        String patt = KafkaSyncController.topicDivideTimeMap.get(metaName);
        SimpleDateFormat sdf = new SimpleDateFormat(patt);
        String esIndexName = metaName + "-" + sdf.format(calendar.getTime());
        EsUtil.createElasticBulkIndex(
                ElasticSearchFactory.getTransportClient(),
                contentList, esIndexName.toLowerCase());
        LOG.info("[syncCount=" + contentList.size() + "] [key=" + esIndexName + "]");
    }


}
