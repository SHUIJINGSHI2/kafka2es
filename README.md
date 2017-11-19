## 项目介绍
将kafka 中的数据，同步到es，其中ES index 可以根据时间写到不同index，实现分表的功能

## 使用方法
部署依赖jdk 1.8，将项目打包成jar
```
java -jar com.baidu.dcs.KafkaSyncController   xxx.jar ./staticConf.properties
```

## 同步实例配置说明
kafkaServers=xxxxx     # kafka地址
kafkaGroupId=kafkaTest  # kafka groupid
elasticHost=xxxx  # elastic集群信息, ip:port，多个之间用逗号隔开
clusterName=scep # elastic集群名称
dynamicConfPath=./dynamicConf.properties  
mainRoundPeriod=5 #主线程的轮询周期(S),每个周期加载动态文件
needWaitAfterEveryRound=false #是否在每个线程的每轮同步之后等待
readKafkaTimeout=2000 #kafka 超时时间
needFilter=false  # 去除logstash添加的参数，只保留用户输出的原始数据
countToEs=5000 #批量往ES写数据的阈值
writeToEsRoundSleep=100 #每次数据同步ES之后等待时间(ms)
kafkaRountCount=2  #merger 的kafka的个数， 从kafka多次读到的数据进行merger，一起同步到es，降低ES 的压力
esTimeInterval=1  # es表的时间间隔, 与dynamicConf.properties 文件对应，用于分表

## 版本历史
发布时间 |版本|说明
----|------|----
20171115 | 1.0.0|开源
