# Kafka Connect nosqldb Connector

[kafka-connect-nosqldb](https://github.com/legend-of-the-dragon/kafka-connect-nosqldb)
是一个为了解决从kafka消费数据sink到nosql的基于kafka connect的插件。目前通过java api 支持ignite和kudu。

# Documentation

### 注意事项
1. debezium写kafka的时候，尽可能一个topic只有一个分区，不然一些莫名其妙的问题都会因为分区出现，尤其是delete导致的问题。
2. ignite目前创建表结构的时候千万不能配错，一旦配错非常难定位问题，而且修复的时候删除不只是用sql删除表，还需要通过原生API删除cache。
3. kudu目前对alter语句支持比较有限，如果出现alter的时候，保险起见，删除kudu中的表，删除kafka中的topic，重新抽一遍当前表。

## kafka consumer 参数

| 参数名称             | 是否必填 | 默认值 | 说明                           |
|------------------|------|-----|------------------------------|
| consumer.max.poll.records | 否    | 500 | 一次从kafka最多拉取的消息条数，建议设置为1000 .这个参数设置在kafka connect 服务器的connect-avro-distributed.properties文件中. |

## 公共参数

| 参数名称                 | 是否必填 | 默认值               | 说明                                                                                                                                                                            |
|----------------------|------|-------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| topic.replace.prefix | 否    | null              | 按照配置的内容作为前缀把topicName的值替换掉. eg: topicName为db51044.sky_test.t_wk_quota_biz的时候,topic.replace.prefix为"db51044.sky_test."则会导致tableName变为t_wk_quota_biz.                           |
| table.name.format    | 否    | "_"               | 把剩余的tableName中的'.'替换成配置的内容. eg: topicName为db51044.sky_test.t_wk_quota_biz的时候,table.name.format为"_"则会导致tableName变为db51044_sky_test_t_wk_quota_biz。注意topic.replace.prefix会优先执行. |
| table.name.prefix    | 否    | null              | 按照配置的内容作为前缀加上把剩余的tableName作为新的tableName                                                                                                                                       |
| message.extract      | 否    | "SCHEMA_REGISTRY" | kafka中存储的数据行的数据结构，值的选项为"SCHEMA_REGISTRY"、"DEBEZIUM"                                                                                                                           |
| batch.size           | 否    | 1000             | 一次写数据库的最大条数。 注：批量写入数据库有助于提供效率，但是太高了可能会导致可能奇葩的故障出现.kudu超过有可能会导致buffer不够，还得调整buffer的大小.                                                                                                                             |
| delete.enabled       | 否    | true             | delete的操作是否启用，默认true.                                                                                                                             |
| allow-record-fields-less-than-table-fields       | 否    | false             | 是否允许记录比表字段少，默认false.                                                                                                                             |

## ignite 参数

| 参数名称                  | 是否必填 | 默认值  | 说明                                                                                                                      |
|-----------------------|------|------|-------------------------------------------------------------------------------------------------------------------------|
| ignite.cfg            |  是   | 无默认值 | Path to the Ignite configuration file. $IGNITE_HOME/config/default-config.xml is used if no Ignite config is configured |
| shall.process.updates |  否   | true | 是否支持upsert，这个不建议改，目前只支持upsert                                                                                           |

```shell
# 注意: 创建ignite表结构的时候，需要在WITH设置以下几个参数：
# CACHE_NAME=<${tableName}>; tableName是kafka connect中的topic经过配置后的最终表名，和ignite中的tableName可以保持一致，也可以不保持一致;
# KEY_TYPE=<${CACHE_NAME}.Key">;
# VALUE_TYPE=<${CACHE_NAME}.Value">;
```

## kudu 参数

| 参数名称                      | 是否必填 | 默认值  | 说明                                   |
|---------------------------|------|------|--------------------------------------|
| kudu.masters              |  是   | 无默认值 | kafka master ip，多个master可以用","隔开     |
| default.partition.buckets |  否   | 5    | 自动创建kudu表的时候，主键的HashPartitions 设置的数值 |

## 公共数据类型与kafka connect的对照关系 (注意:不在表中的数据类型暂不支持)
| nosqldb-kafka-connect 数据类型 | kafka connect数据类型 |kafka connect 类型名称| debezium 数据类型 | mysql 数据类型 |
|---------------------------|------|------|-------|------|
| TINYINT | INT8  | null |
| SHORT | INT16  | null |
| INT | INT32  | null |
| LONG | INT64  | null |
| FLOAT | FLOAT32  | null |
| DOUBLE | FLOAT64  | null | 
| BOOLEAN | BOOLEAN  | null | 
| STRING | STRING  | null | 
| DECIMAL | BYTES  | org.apache.kafka.connect.data.Decimal |
| DATE | INT32  | org.apache.kafka.connect.data.Date / io.debezium.time.Date |
| TIMESTAMP | STRING  | io.debezium.time.ZonedTimestamp |
| TIMESTAMP | INT64  | io.debezium.time.Timestamp / org.apache.kafka.connect.data.Timestamp |
| TIMESTAMP | INT64  | io.debezium.time.MicroTimestamp
| TIME | INT32  | io.debezium.time.MicroTime |
| INT | INT32  | io.debezium.time.Year |
| BYTES | BYTES  | io.debezium.data.Bits |
| STRING | STRING  | io.debezium.data.EnumSet |
| STRING | STRING  | io.debezium.data.Json |
| BYTES | BYTES  | io.debezium.data.Bits |

## ignite和公共数据类型的对照关系 (注意:不在表中的数据类型暂不支持)
| nosqldb-kafka-connect 数据类型 | ignite 数据类型 | 
|---------------------------|------|
| TINYINT | TINYINT  |
| SHORT | SHORT  | 
| INT | INT  | 
| LONG | LONG  | 
| FLOAT | FLOAT  | 
| DOUBLE | DOUBLE  | 
| BOOLEAN | BOOLEAN  | 
| STRING | STRING  | 
| BYTES | BYTES  | 
|TIMESTAMP| TIMESTAMP|
|DATE|DATE|
|TIME|TIME|
|DECIMAL|DECIMAL|
## kudu和公共数据类型的对照关系
| nosqldb-kafka-connect 数据类型 | kudu 数据类型 | 
|---------------------------|------|
| TINYINT | INT8  |
| SHORT | INT16  | 
| INT | INT32  | 
| LONG | INT64  | 
| FLOAT | FLOAT  | 
| DOUBLE | DOUBLE  | 
| BOOLEAN | BOOL  | 
| STRING | STRING  | 
| BYTES | BINARY  | 
|TIMESTAMP| UNIXTIME_MICROS|
|DATE|STRING|
|TIME|STRING|
|DECIMAL|DECIMAL|
# Development

You can build kafka-connect-nosqldb with Maven using the standard lifecycle phases.

```shell
mvn clean package -Dmaven.test.skip=true  -Dcheckstyle.skip=true -Dmaven.javadoc.skip=true -Pstandalone
```

