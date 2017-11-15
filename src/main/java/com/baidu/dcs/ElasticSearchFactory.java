package com.baidu.dcs;

import com.baidu.dcs.internal.StaticSysConf;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

public class ElasticSearchFactory {


    private static TransportClient client;

    public static TransportClient getTransportClient() {
        if (null == client) {
            try {
                String clusterName = StaticSysConf.getProperty("clusterName");
                String hosts = StaticSysConf.getProperty("elasticHost");

                Class<?> clazz = Class.forName(TransportClient.class.getName());
                Constructor<?> constructor = clazz
                        .getDeclaredConstructor(Settings.class);
                constructor.setAccessible(true);
                Map<String, String> m = new HashMap<String, String>();
                Settings settings = ImmutableSettings.settingsBuilder().put(m)
                        .put("cluster.name", clusterName)
                        .put("client.transport.sniff", true).build();
                client = (TransportClient) constructor.newInstance(settings);
                String[] elasticHost = hosts.split(",");
                for (String item : elasticHost) {
                    client.addTransportAddress(new InetSocketTransportAddress(item.substring(0, item.indexOf(":")),
                            Integer.parseInt(item.substring(item.indexOf(":") + 1, item.length()))));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return client;
    }

}


