package org.datacenter.kafka.source.kafka;

import io.confluent.common.utils.Utils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.storage.Converter;

import java.util.Map;
import java.util.Properties;

/**
 * KafkaSourceConnectorConfig
 *
 * @author sky
 * @date 2023-10-10
 */
public class KafkaSourceConnectorConfig extends AbstractConfig {

    public static final String SRC_BOOTSTRAP_SERVERS_KEY = "src.bootstrap.servers";

    public static final String SRC_GROUP_ID_KEY = "src.group.id";
    public static final String SRC_GROUP_ID_DEFAULT = "data-center";

    public static final String SRC_MAX_POLL_RECORDS_KEY = "src.max.poll.records";
    public static final Integer SRC_MAX_POLL_RECORDS_DEFAULT = 10000;

    public static final String TOPIC_RENAME_FORMAT_KEY = "topic.rename.format";
    public static final String TOPIC_RENAME_FORMAT_DEFAULT = "${topic}";

    public static final String SRC_TOPICS_KEY = "topics";

    public static final String SRC_TOPICS_REGEX_KEY = "topics.regex";

    public static final String TOPIC_TIMESTAMP_TYPE_KEY = "topic.timestamp.type";
    public static final String TOPIC_TIMESTAMP_TYPE_DEFAULT = "";

    public static final String TOPIC_AUTO_CREATE_KEY = "topic.auto.create";
    public static final boolean TOPIC_AUTO_CREATE_DEFAULT = true;

    public static final String DEST_TOPIC_REPLICATION_PARTITION_NUM_KEY =
            "dest.topic.replication.partitionNum";
    public static final int DEST_TOPIC_REPLICATION_PARTITION_NUM_DEFAULT = 1;

    public static final String DEST_TOPIC_REPLICATION_FACTOR_KEY = "dest.topic.replication.factor";
    public static final int DEST_TOPIC_REPLICATION_FACTOR_DEFAULT = 3;

    public static final String DEST_TOPIC_CLEANUP_POLICY_KEY = "dest.topic.cleanup.policy";
    public static final String DEST_TOPIC_CLEANUP_POLICY_DEFAULT = "compact";

    public static final String TOPIC_PRESERVE_PARTITIONS_KEY = "topic.preserve.partitions";
    public static final boolean TOPIC_PRESERVE_PARTITIONS_DEFAULT = true;

    public static ConfigDef configDef() {

        return new ConfigDef()
                .define(SRC_BOOTSTRAP_SERVERS_KEY, Type.STRING, "", Importance.HIGH, "源kafka的地址.")
                .define(
                        SRC_GROUP_ID_KEY,
                        Type.STRING,
                        SRC_GROUP_ID_DEFAULT,
                        Importance.HIGH,
                        "消费源kafka的groupId.")
                .define(
                        SRC_MAX_POLL_RECORDS_KEY,
                        Type.INT,
                        SRC_MAX_POLL_RECORDS_DEFAULT,
                        Importance.LOW,
                        "一次poll最多拉取的记录数.")
                .define(
                        TOPIC_RENAME_FORMAT_KEY,
                        Type.STRING,
                        TOPIC_RENAME_FORMAT_DEFAULT,
                        Importance.LOW,
                        "")
                .define(
                        SRC_TOPICS_KEY,
                        Type.STRING,
                        null,
                        Importance.LOW,
                        "topics和topics.regex必须二选一填写.")
                .define(
                        SRC_TOPICS_REGEX_KEY,
                        Type.STRING,
                        null,
                        Importance.LOW,
                        "topics和topics.regex必须二选一填写.")
                .define(
                        "src.key.converter",
                        Type.CLASS,
                        "org.apache.kafka.common.serialization.ByteArrayDeserializer",
                        Importance.HIGH,
                        "")
                .define(
                        "src.value.converter",
                        Type.CLASS,
                        "org.apache.kafka.common.serialization.ByteArrayDeserializer",
                        Importance.HIGH,
                        "")
                .define(
                        TOPIC_TIMESTAMP_TYPE_KEY,
                        Type.STRING,
                        TOPIC_TIMESTAMP_TYPE_DEFAULT,
                        Importance.LOW,
                        "")
                .define(
                        TOPIC_AUTO_CREATE_KEY,
                        Type.BOOLEAN,
                        TOPIC_AUTO_CREATE_DEFAULT,
                        Importance.LOW,
                        "")
                .define(
                        DEST_TOPIC_REPLICATION_FACTOR_KEY,
                        Type.INT,
                        DEST_TOPIC_REPLICATION_FACTOR_DEFAULT,
                        Importance.LOW,
                        "")
                .define(
                        DEST_TOPIC_CLEANUP_POLICY_KEY,
                        Type.STRING,
                        DEST_TOPIC_CLEANUP_POLICY_DEFAULT,
                        Importance.LOW,
                        "")
                .define(
                        DEST_TOPIC_REPLICATION_PARTITION_NUM_KEY,
                        Type.INT,
                        DEST_TOPIC_REPLICATION_PARTITION_NUM_DEFAULT,
                        Importance.LOW,
                        "这个参数默认为1,暂时不要动.")
                .define(
                        TOPIC_PRESERVE_PARTITIONS_KEY,
                        Type.BOOLEAN,
                        TOPIC_PRESERVE_PARTITIONS_DEFAULT,
                        Importance.LOW,
                        "");
    }

    public String srcBootstrapServers;
    public String srcGroupId;
    public int srcMaxPollRecords;
    public final String srcTopics;
    public final String srcTopicPrefix;

    public final String topicRenameFormat;
    public final String topicTimestampType;
    public final boolean topicAutoCreate;
    public final int destTopicReplicationParitionNum;
    public final int destTopicReplicationFactor;
    public final String destTopicCleanupPolicy;
    public final boolean topicPreservePartitions;

    public KafkaSourceConnectorConfig(Map<String, String> props) {

        super(configDef(), props);

        this.srcBootstrapServers = getSrcBootstrapServers();
        this.srcGroupId = getSrcGroupId();
        this.srcMaxPollRecords = getSrcMaxPollRecords();
        this.srcTopics = getSrcTopics();
        this.srcTopicPrefix = getSrcTopicPrefix();
        this.topicRenameFormat = getTopicRenameFormat();
        this.topicTimestampType = getTopicTimestampType();
        this.topicAutoCreate = getTopicAutoCreate();
        this.destTopicReplicationParitionNum = getDestTopicReplicationParitionNum();
        this.destTopicReplicationFactor = getDestTopicReplicationFactor();
        this.destTopicCleanupPolicy = getDestTopicCleanupPolicy();
        this.topicPreservePartitions = getTopicPreservePartitions();
    }

    public String getSrcBootstrapServers() {
        return this.getString(SRC_BOOTSTRAP_SERVERS_KEY);
    }

    public String getSrcGroupId() {
        return this.getString(SRC_GROUP_ID_KEY);
    }

    public String getSrcTopics() {
        return this.getString(SRC_TOPICS_KEY);
    }

    public String getSrcTopicPrefix() {
        return this.getString(SRC_TOPICS_REGEX_KEY);
    }

    public int getSrcMaxPollRecords() {
        return this.getInt(SRC_MAX_POLL_RECORDS_KEY);
    }

    public String getTopicRenameFormat() {
        return this.getString(TOPIC_RENAME_FORMAT_KEY);
    }

    public String getTopicTimestampType() {

        return this.getString(TOPIC_TIMESTAMP_TYPE_KEY);
    }

    public boolean getTopicAutoCreate() {
        return this.getBoolean(TOPIC_AUTO_CREATE_KEY);
    }

    public int getDestTopicReplicationFactor() {
        return this.getInt(DEST_TOPIC_REPLICATION_FACTOR_KEY);
    }

    public int getDestTopicReplicationParitionNum() {
        return this.getInt(DEST_TOPIC_REPLICATION_PARTITION_NUM_KEY);
    }

    public String getDestTopicCleanupPolicy() {
        return this.getString(DEST_TOPIC_CLEANUP_POLICY_KEY);
    }

    public boolean getTopicPreservePartitions() {
        return this.getBoolean(TOPIC_PRESERVE_PARTITIONS_KEY);
    }

    public Map<String, Object> getSourceConsumerConfigs() {
        return originalsWithPrefix(KafkaConfigs.KafkaCluster.SOURCE.prefix());
    }

    public String getName() {
        return this.getString("name");
    }

    public Map<String, Object> destAdminClientConfig() {
        return originalsWithPrefix(KafkaConfigs.KafkaCluster.DESTINATION.prefix());
    }

    public Map<String, Object> srcAdminClientConfig() {
        return originalsWithPrefix(KafkaConfigs.KafkaCluster.SOURCE.prefix());
    }

    public Properties getKafkaConsumerConfig() {

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.srcBootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, this.srcGroupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        // 设置消费者从offset不存在时默认从最开始的位置开始消费
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // 设置消费者的enable.auto.commit参数为false，这样可以手动控制offset
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        properties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);

        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, this.srcMaxPollRecords);
        properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 134217728 * 2); // 128MB
        properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 7000); // 8s
        properties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 134217728 * 2); // 128MB
        properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 180000); // 180s
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 360000); // 180s
        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 180000); // 180s

        return properties;
    }

    public Converter getConverter(String prefix, boolean isKey) {

        Converter converter = getInstance(prefix, Converter.class);
        assert converter != null;
        Map<String, Object> originals = originalsWithPrefix(prefix + ".");
        converter.configure(originals, isKey);
        return converter;
    }

    private <T> T getInstance(String key, Class<T> t) {
        Class<?> c = getClass(key);
        if (c == null) return null;
        Object o = Utils.newInstance(c);
        if (!t.isInstance(o))
            throw new ConnectException(c.getName() + " is not an instance of " + t.getName());
        return t.cast(o);
    }
}
