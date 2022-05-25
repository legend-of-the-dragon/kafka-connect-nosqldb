# Kafka Connect JDBC Connector

[kafka-connect-nosqldb](http://gitlab.9f.cn/data-center/kafka-connect-nosqldb.git)
��һ���ɴ����ݲ�������Ϊ�˽����kafka��������sink��nosql�Ļ���kafka connect�Ĳ����Ŀǰͨ��java api ֧��ignite��kudu��

# Documentation

## kafka consumer ����

| ��������             | Ĭ��ֵ | ˵��                           |
|------------------|-----|------------------------------|
| max.poll.records | 500 | һ�δ�kafka�����ȡ����Ϣ��������������Ϊ10000 |

## ��������

| ��������                 | Ĭ��ֵ               | ˵��                                                  |
|----------------------|-------------------|-----------------------------------------------------|
| topic.replace.prefix | ""                | �������õ�������Ϊǰ׺��topicName��ֵ�滻��.                         |
| table.name.format    | "_"               | ��ʣ���tableName�е�'.'�滻�����õ�����                          |
| table.name.prefix    | ""                | �������õ�������Ϊǰ׺���ϰ�ʣ���tableName��Ϊ�µ�tableName             |
| message.extract      | "SCHEMA_REGISTRY" | kafka�д洢�������е����ݽṹ��ֵ��ѡ��Ϊ"SCHEMA_REGISTRY"��"DEBEZIUM" |
| batch.size           | 10000             | һ��д���ݿ�����������ע������д�����ݿ��������ṩЧ�ʣ�����̫���˿��ܻᵼ�¿�������Ĺ��ϳ���    |

## ignite ����

| ��������                  | Ĭ��ֵ  | ˵��                                                                                                                      |
|-----------------------|------|-------------------------------------------------------------------------------------------------------------------------|
| ignite.cfg            | ��Ĭ��ֵ | Path to the Ignite configuration file. $IGNITE_HOME/config/default-config.xml is used if no Ignite config is configured |
| shall.process.updates | true | �Ƿ�֧��upsert�����������ģ�Ŀǰֻ֧��upsert                                                                                           |

## kudu ����

| ��������                      | Ĭ��ֵ  | ˵��                                   |
|---------------------------|------|--------------------------------------|
| kudu.masters              | ��Ĭ��ֵ | kafka master ip�����master������","����     |
| default.partition.buckets | 5    | �Զ�����kudu���ʱ��������HashPartitions ���õ���ֵ |

# Development

You can build kafka-connect-nosqldb with Maven using the standard lifecycle phases.

```shell
mvn clean package -Dmaven.test.skip=true  -Dcheckstyle.skip=true -Dmaven.javadoc.skip=true -Pstandalone
```

