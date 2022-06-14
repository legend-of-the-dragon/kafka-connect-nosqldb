# Kafka Connect nosqldb Connector

[kafka-connect-nosqldb](https://github.com/legend-of-the-dragon/kafka-connect-nosqldb)
��һ��Ϊ�˽����kafka��������sink��nosql�Ļ���kafka connect�Ĳ����Ŀǰͨ��java api ֧��ignite��kudu��

# Documentation

### ע������
1��debeziumдkafka��ʱ�򣬾�����һ��topicֻ��һ����������ȻһЩĪ����������ⶼ����Ϊ�������֣�������delete���µ����⡣
2. igniteĿǰ������ṹ��ʱ��ǧ�������һ�����ǳ��Ѷ�λ���⣬�����޸���ʱ��ɾ����ֻ����sqlɾ��������Ҫͨ��ԭ��APIɾ��cache��
3. kuduĿǰ��alter���֧�ֱȽ����ޣ��������alter��ʱ�򣬱��������ɾ��kudu�еı�ɾ��kafka�е�topic�����³�һ�鵱ǰ��

## kafka consumer ����

| ��������             | �Ƿ���� | Ĭ��ֵ | ˵��                           |
|------------------|------|-----|------------------------------|
| consumer.max.poll.records | ��    | 500 | һ�δ�kafka�����ȡ����Ϣ��������������Ϊ1000 .�������������kafka connect ��������connect-avro-distributed.properties�ļ���. |

## ��������

| ��������                 | �Ƿ���� | Ĭ��ֵ               | ˵��                                                                                                                                                                            |
|----------------------|------|-------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| topic.replace.prefix | ��    | null              | �������õ�������Ϊǰ׺��topicName��ֵ�滻��. eg: topicNameΪdb51044.sky_test.t_wk_quota_biz��ʱ��,topic.replace.prefixΪ"db51044.sky_test."��ᵼ��tableName��Ϊt_wk_quota_biz.                           |
| table.name.format    | ��    | "_"               | ��ʣ���tableName�е�'.'�滻�����õ�����. eg: topicNameΪdb51044.sky_test.t_wk_quota_biz��ʱ��,table.name.formatΪ"_"��ᵼ��tableName��Ϊdb51044_sky_test_t_wk_quota_biz��ע��topic.replace.prefix������ִ��. |
| table.name.prefix    | ��    | null              | �������õ�������Ϊǰ׺���ϰ�ʣ���tableName��Ϊ�µ�tableName                                                                                                                                       |
| message.extract      | ��    | "SCHEMA_REGISTRY" | kafka�д洢�������е����ݽṹ��ֵ��ѡ��Ϊ"SCHEMA_REGISTRY"��"DEBEZIUM"                                                                                                                           |
| batch.size           | ��    | 1000             | һ��д���ݿ����������� ע������д�����ݿ��������ṩЧ�ʣ�����̫���˿��ܻᵼ�¿�������Ĺ��ϳ���.kudu�����п��ܻᵼ��buffer���������õ���buffer�Ĵ�С.                                                                                                                             |

## ignite ����

| ��������                  | �Ƿ���� | Ĭ��ֵ  | ˵��                                                                                                                      |
|-----------------------|------|------|-------------------------------------------------------------------------------------------------------------------------|
| ignite.cfg            |  ��   | ��Ĭ��ֵ | Path to the Ignite configuration file. $IGNITE_HOME/config/default-config.xml is used if no Ignite config is configured |
| shall.process.updates |  ��   | true | �Ƿ�֧��upsert�����������ģ�Ŀǰֻ֧��upsert                                                                                           |

```shell
# ע��: ����ignite��ṹ��ʱ����Ҫ��WITH�������¼���������
# CACHE_NAME=<${tableName}>; tableName��kafka connect�е�topic�������ú�����ձ�������ignite�е�tableName���Ա���һ�£�Ҳ���Բ�����һ��;
# KEY_TYPE=<${CACHE_NAME}.Key">;
# VALUE_TYPE=<${CACHE_NAME}.Value">;
```

## kudu ����

| ��������                      | �Ƿ���� | Ĭ��ֵ  | ˵��                                   |
|---------------------------|------|------|--------------------------------------|
| kudu.masters              |  ��   | ��Ĭ��ֵ | kafka master ip�����master������","����     |
| default.partition.buckets |  ��   | 5    | �Զ�����kudu���ʱ��������HashPartitions ���õ���ֵ |

## ��������������kafka connect�Ķ��չ�ϵ (ע��:���ڱ��е����������ݲ�֧��)
| nosqldb-kafka-connect �������� | kafka connect�������� |kafka connect ��������| debezium �������� | mysql �������� |
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

## ignite�͹����������͵Ķ��չ�ϵ (ע��:���ڱ��е����������ݲ�֧��)
| nosqldb-kafka-connect �������� | ignite �������� | 
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
## kudu�͹����������͵Ķ��չ�ϵ
| nosqldb-kafka-connect �������� | kudu �������� | 
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

