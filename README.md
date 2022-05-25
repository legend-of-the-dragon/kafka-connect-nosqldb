# Kafka Connect JDBC Connector

[kafka-connect-nosqldb](http://gitlab.9f.cn/data-center/kafka-connect-nosqldb.git)
是一个由大数据部开发的为了解决从kafka消费数据sink到nosql的基于kafka connect的插件。目前通过java api 支持ignite和kudu。

# Documentation

## kafka consumer 参数

| 参数名称             | 默认值 | 说明                           |
|------------------|-----|------------------------------|
| max.poll.records | 500 | 一次从kafka最多拉取的消息条数，建议设置为10000 |

## 公共参数

| 参数名称                 | 默认值               | 说明                                                  |
|----------------------|-------------------|-----------------------------------------------------|
| topic.replace.prefix | ""                | 按照配置的内容作为前缀把topicName的值替换掉.                         |
| table.name.format    | "_"               | 把剩余的tableName中的'.'替换成配置的内容                          |
| table.name.prefix    | ""                | 按照配置的内容作为前缀加上把剩余的tableName作为新的tableName             |
| message.extract      | "SCHEMA_REGISTRY" | kafka中存储的数据行的数据结构，值的选项为"SCHEMA_REGISTRY"、"DEBEZIUM" |
| batch.size           | 10000             | 一次写数据库的最大条数。注：批量写入数据库有助于提供效率，但是太高了可能会导致可能奇葩的故障出现    |

## ignite 参数

| 参数名称                  | 默认值  | 说明                                                                                                                      |
|-----------------------|------|-------------------------------------------------------------------------------------------------------------------------|
| ignite.cfg            | 无默认值 | Path to the Ignite configuration file. $IGNITE_HOME/config/default-config.xml is used if no Ignite config is configured |
| shall.process.updates | true | 是否支持upsert，这个不建议改，目前只支持upsert                                                                                           |

## kudu 参数

| 参数名称                      | 默认值  | 说明                                   |
|---------------------------|------|--------------------------------------|
| kudu.masters              | 无默认值 | kafka master ip，多个master可以用","隔开     |
| default.partition.buckets | 5    | 自动创建kudu表的时候，主键的HashPartitions 设置的数值 |

# Development

You can build kafka-connect-nosqldb with Maven using the standard lifecycle phases.

```shell
mvn clean package -Dmaven.test.skip=true  -Dcheckstyle.skip=true -Dmaven.javadoc.skip=true -Pstandalone
```

