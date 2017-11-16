## 项目介绍
将kafka 中的数据，同步到es，其中ES index 可以根据时间写到不同index，实现分表的功能

## 使用方法
部署依赖jdk 1.8，将项目打包成jar
```
java -jar com.baidu.dcs.KafkaSyncController   xxx.jar ./staticConf.properties
```

## 版本历时
发布时间 |版本号
----|------
20171115 | 1.0.0
