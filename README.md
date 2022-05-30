# Kafka Connect nosqldb Connector

[kafka-connect-nosqldb](http://gitlab.9f.cn/data-center/kafka-connect-nosqldb.git)
��һ���ɴ����ݲ�������Ϊ�˽����kafka��������sink��nosql�Ļ���kafka connect�Ĳ����Ŀǰͨ��java api ֧��ignite��kudu��

# Documentation

### ע������

1. igniteĿǰ������ṹ��ʱ��ǧ�������һ�����ǳ��Ѷ�λ���⣬�����޸���ʱ��ɾ����ֻ����sqlɾ��������Ҫͨ��ԭ��APIɾ��cache��
2. kuduĿǰ��alter���֧�ֱȽ����ޣ��������alter��ʱ�򣬱��������ɾ��kudu�еı�ɾ��kafka�е�topic�����³�һ�鵱ǰ��

## kafka consumer ����

| ��������             | �Ƿ���� | Ĭ��ֵ | ˵��                           |
|------------------|------|-----|------------------------------|
| consumer.max.poll.records | ��    | 500 | һ�δ�kafka�����ȡ����Ϣ��������������Ϊ5000 .�������������kafka connect ��������connect-avro-distributed.properties�ļ���. |

## ��������

| ��������                 | �Ƿ���� | Ĭ��ֵ               | ˵��                                                                                                                                                                            |
|----------------------|------|-------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| topic.replace.prefix | ��    | null              | �������õ�������Ϊǰ׺��topicName��ֵ�滻��. eg: topicNameΪdb51044.sky_test.t_wk_quota_biz��ʱ��,topic.replace.prefixΪ"db51044.sky_test."��ᵼ��tableName��Ϊt_wk_quota_biz.                           |
| table.name.format    | ��    | "_"               | ��ʣ���tableName�е�'.'�滻�����õ�����. eg: topicNameΪdb51044.sky_test.t_wk_quota_biz��ʱ��,table.name.formatΪ"_"��ᵼ��tableName��Ϊdb51044_sky_test_t_wk_quota_biz��ע��topic.replace.prefix������ִ��. |
| table.name.prefix    | ��    | null              | �������õ�������Ϊǰ׺���ϰ�ʣ���tableName��Ϊ�µ�tableName                                                                                                                                       |
| message.extract      | ��    | "SCHEMA_REGISTRY" | kafka�д洢�������е����ݽṹ��ֵ��ѡ��Ϊ"SCHEMA_REGISTRY"��"DEBEZIUM"                                                                                                                           |
| batch.size           | ��    | 10000             | һ��д���ݿ�����������ע������д�����ݿ��������ṩЧ�ʣ�����̫���˿��ܻᵼ�¿�������Ĺ��ϳ���                                                                                                                              |

## ignite ����

| ��������                  | �Ƿ���� | Ĭ��ֵ  | ˵��                                                                                                                      |
|-----------------------|------|------|-------------------------------------------------------------------------------------------------------------------------|
| ignite.cfg            |  ��   | ��Ĭ��ֵ | Path to the Ignite configuration file. $IGNITE_HOME/config/default-config.xml is used if no Ignite config is configured |
| shall.process.updates |  ��   | true | �Ƿ�֧��upsert�����������ģ�Ŀǰֻ֧��upsert                                                                                           |

```
ע��: ����ignite��ṹ��ʱ����Ҫ��WITH�������¼���������
CACHE_NAME=<${tableName}>; tableNamekafka connect�еģ���ignite�е�tableName���Ա���һ�£�Ҳ���Բ�����һ��
KEY_TYPE=<${CACHE_NAME}.Key">
VALUE_TYPE=<$CACHE_NAME}.Value">
```

## kudu ����

| ��������                      | �Ƿ���� | Ĭ��ֵ  | ˵��                                   |
|---------------------------|------|------|--------------------------------------|
| kudu.masters              |  ��   | ��Ĭ��ֵ | kafka master ip�����master������","����     |
| default.partition.buckets |  ��   | 5    | �Զ�����kudu���ʱ��������HashPartitions ���õ���ֵ |

## ��������������kafka connect�Ķ��չ�ϵ

## ignite�͹����������͵Ķ��չ�ϵ

## kudu�͹����������͵Ķ��չ�ϵ

# Development

You can build kafka-connect-nosqldb with Maven using the standard lifecycle phases.

```shell
mvn clean package -Dmaven.test.skip=true  -Dcheckstyle.skip=true -Dmaven.javadoc.skip=true -Pstandalone
```

