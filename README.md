# Kafka Connect nosqldb Connector

[kafka-connect-nosqldb](http://gitlab.9f.cn/data-center/kafka-connect-nosqldb.git)
是一个由大数据部开发的为了解决从kafka消费数据sink到nosql的基于kafka connect的插件。目前通过java api 支持ignite和kudu。

# Documentation

### 注意事项

1. ignite目前创建表结构的时候千万不能配错，一旦配错非常难定位问题，而且修复的时候删除不只是用sql删除表，还需要通过原生API删除cache。
2. kudu目前对alter语句支持比较有限，如果出现alter的时候，保险起见，删除kudu中的表，删除kafka中的topic，重新抽一遍当前表。

## kafka consumer 参数

| 参数名称             | 是否必填 | 默认值 | 说明                           |
|------------------|------|-----|------------------------------|
| consumer.max.poll.records | 否    | 500 | 一次从kafka最多拉取的消息条数，建议设置为5000 .这个参数设置在kafka connect 服务器的connect-avro-distributed.properties文件中. |

## 公共参数

| 参数名称                 | 是否必填 | 默认值               | 说明                                                                                                                                                                            |
|----------------------|------|-------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| topic.replace.prefix | 否    | null              | 按照配置的内容作为前缀把topicName的值替换掉. eg: topicName为db51044.sky_test.t_wk_quota_biz的时候,topic.replace.prefix为"db51044.sky_test."则会导致tableName变为t_wk_quota_biz.                           |
| table.name.format    | 否    | "_"               | 把剩余的tableName中的'.'替换成配置的内容. eg: topicName为db51044.sky_test.t_wk_quota_biz的时候,table.name.format为"_"则会导致tableName变为db51044_sky_test_t_wk_quota_biz。注意topic.replace.prefix会优先执行. |
| table.name.prefix    | 否    | null              | 按照配置的内容作为前缀加上把剩余的tableName作为新的tableName                                                                                                                                       |
| message.extract      | 否    | "SCHEMA_REGISTRY" | kafka中存储的数据行的数据结构，值的选项为"SCHEMA_REGISTRY"、"DEBEZIUM"                                                                                                                           |
| batch.size           | 否    | 10000             | 一次写数据库的最大条数。注：批量写入数据库有助于提供效率，但是太高了可能会导致可能奇葩的故障出现                                                                                                                              |

## ignite 参数

| 参数名称                  | 是否必填 | 默认值  | 说明                                                                                                                      |
|-----------------------|------|------|-------------------------------------------------------------------------------------------------------------------------|
| ignite.cfg            |  是   | 无默认值 | Path to the Ignite configuration file. $IGNITE_HOME/config/default-config.xml is used if no Ignite config is configured |
| shall.process.updates |  否   | true | 是否支持upsert，这个不建议改，目前只支持upsert                                                                                           |

```
注意: 创建ignite表结构的时候，需要在WITH设置以下几个参数：
CACHE_NAME=<${tableName}>; tableNamekafka connect中的，和ignite中的tableName可以保持一致，也可以不保持一致
KEY_TYPE=<${CACHE_NAME}.Key">
VALUE_TYPE=<$CACHE_NAME}.Value">
```

## kudu 参数

| 参数名称                      | 是否必填 | 默认值  | 说明                                   |
|---------------------------|------|------|--------------------------------------|
| kudu.masters              |  是   | 无默认值 | kafka master ip，多个master可以用","隔开     |
| default.partition.buckets |  否   | 5    | 自动创建kudu表的时候，主键的HashPartitions 设置的数值 |

## 公共数据类型与kafka connect的对照关系

## ignite和公共数据类型的对照关系

## kudu和公共数据类型的对照关系

# Development

You can build kafka-connect-nosqldb with Maven using the standard lifecycle phases.

```shell
mvn clean package -Dmaven.test.skip=true  -Dcheckstyle.skip=true -Dmaven.javadoc.skip=true -Pstandalone
```

