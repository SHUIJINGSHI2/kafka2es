package com.baidu.dcs.util;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by sunchenjiao on 17/1/5.
 */
public class EsUtil {

    public static Logger LOG = LoggerFactory.getLogger(EsUtil.class);

    /**
     * 批量写es
     *
     * @param client
     * @param logList
     * @param appName
     */
    public static void createElasticBulkIndex(TransportClient client,
                                              List<String> logList, String appName) {
        if (logList == null || logList.size() == 0) {
            return;
        }
        Date nowTime = new Date();
        SimpleDateFormat time = new SimpleDateFormat("yyyy-MM-dd");

        try {
            BulkRequestBuilder bulkRequest = client.prepareBulk();

            for (String log : logList) {
                // 业务对象
                IndexRequestBuilder indexRequest = client.prepareIndex(appName,
                        time.format(nowTime)).setSource(log);
                // 添加到builder中
                bulkRequest.add(indexRequest);
            }

            BulkResponse bulkResponse = bulkRequest.execute().actionGet();
            if (bulkResponse.hasFailures()) {
                LOG.error("insert into ES has failure, deal exception after 5 seconds [data:" + logList.size() + "]");
                try {
                    Thread.sleep(5000);
                    reInsertElasticErrorLogs(client, logList, bulkResponse, appName);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            LOG.error("insert into ES excption [data size:" + logList.size() + "]");
            e.printStackTrace();
        }
    }


    public static void reInsertElasticErrorLogs(TransportClient client,
                                                List<String> logList, BulkResponse bulkResponse, String appName) {
        String failureMessage = bulkResponse.buildFailureMessage();
        if (failureMessage == null && failureMessage.length() == 0) {
            LOG.error("[reinsert fail] empty failure message");
            return;
        }
        String[] iterms = failureMessage.split("\n");
        if (iterms.length == 0) {
            LOG.error("[reinsert fail] fail to parse failure message");
            return;
        }
        // 统计多少数据第一次写的时候异常
        int failCount = 0;
        for (int i = 0; i < iterms.length; i++) {
            String patternStr = "\\[(.*?)\\]: index \\[(.*?)\\], type \\[(.*?)\\], id \\[(.*?)\\](.*)";
            Pattern pattern = Pattern.compile(patternStr);
            Matcher matcher = pattern.matcher(iterms[i]);
            if (matcher.find()) {
                // list出错的位次
                int offset = Integer.parseInt(matcher.group(1));
                String index = matcher.group(2);
                String type = matcher.group(3);
                String id = matcher.group(4);
                try {
                    int status = bulkResponse.getItems()[offset].getFailure().getStatus().getStatus();
                    if (status < 300) {
                        // 错误信息小于300时，es已经获取数据，处理中，不操作
                        continue;
                    } else {
                        // 删除数据，重新insert，可能存在删除不成功的情况，但是可能性很小，这里为了提高效率，不进行检查,
                        client.prepareDelete(index, type, id).execute().actionGet();
                        createElasticIndex(client, logList.get(offset), appName);
                        failCount++;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        LOG.info("[reinsert success] insert into es size: " + failCount);
    }

    /**
     * 单条数据写es
     *
     * @param client
     * @param log
     * @param appName
     */
    public static void createElasticIndex(TransportClient client, String log,
                                          String appName) {
        Date nowTime = new Date();
        SimpleDateFormat time = new SimpleDateFormat("yyyy-MM-dd");
        try {
            IndexResponse response = client
                    .prepareIndex(appName, time.format(nowTime)).setSource(log)
                    .execute().actionGet();
            if (response != null && response.isCreated() == true) {
                return;
            } else {
                LOG.error("insert into ES failure, retry after 1 seconds [data:" + log + "]");

                // 这部分逻辑加了一次失败重试，但仍可能会丢失数据，TODO：后期需要优化
                try {
                    Thread.sleep(1000);
                    client.prepareIndex(appName, time.format(nowTime))
                            .setSource(log).execute().actionGet();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            LOG.error("insert into ES excption, abandon [data:" + log + "]");
            e.printStackTrace();
        }
    }

}
