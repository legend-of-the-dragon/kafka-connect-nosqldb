package org.datacenter.kafka.config.kudu;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

/**
 * KuduSinkConnectorConfig
 *
 * @author sky
 * @date 2022-05-10
 */
public class KuduSinkConnectorConfig extends AbstractConfig {

    public static final String KUDU_MASTERS_KEY = "kudu.masters";
    public static final String TOPIC_PREFIX_KEY = "topic.prefix";

    public static final String TABLE_PREFIX_KEY = "table.name.prefix";
    public static final String TABLE_NAME_PREFIX_DEFAULT = "";

    public static final String MESSAGE_EXTRACT_KEY = "message.extract";
    public static final String MESSAGE_EXTRACT_DEFAULT = "SCHEMA_REGISTRY";
    public static final String BATCH_SIZE_KEY = "batch.size";
    public static final Integer BATCH_SIZE_DEFAULT = 10000;

    public static final String DEFAULT_PARTITION_BUCKETS_KEY = "default.partition.buckets";
    public static final Integer DEFAULT_PARTITION_BUCKETS_DEFAULT = 5;
    private static final String BATCH_SIZE_DOC =
            "Maximum number of entries to send to Ignite in single batch.";
    private static final String TOPIC_PREFIX_DOC =
            "Kafka topic is built from this prefix and cache name.";

    public enum MessageExtract {
        DEBEZIUM,
        SCHEMA_REGISTRY;
    }

    public static ConfigDef configDef() {

        return (new ConfigDef())
                .define(KUDU_MASTERS_KEY, Type.STRING, "", Importance.HIGH, "")
                .define(TOPIC_PREFIX_KEY, Type.STRING, "", Importance.HIGH, TOPIC_PREFIX_DOC)
                .define(
                        TABLE_NAME_PREFIX_DEFAULT,
                        Type.STRING,
                        TABLE_NAME_PREFIX_DEFAULT,
                        Importance.LOW,
                        "")
                .define(
                        DEFAULT_PARTITION_BUCKETS_KEY,
                        Type.INT,
                        DEFAULT_PARTITION_BUCKETS_DEFAULT,
                        Importance.HIGH,
                        "")
                .define(
                        BATCH_SIZE_KEY,
                        Type.INT,
                        BATCH_SIZE_DEFAULT,
                        Importance.LOW,
                        BATCH_SIZE_DOC)
                .define(
                        MESSAGE_EXTRACT_KEY,
                        Type.STRING,
                        MESSAGE_EXTRACT_DEFAULT,
                        Importance.LOW,
                        "");
    }

    public Integer batchSize() {
        return this.getInt(BATCH_SIZE_KEY);
    }

    public String topicPrefix() {
        return this.getString(TOPIC_PREFIX_KEY);
    }

    public String tableNamePrefix() {
        return this.getString(TABLE_PREFIX_KEY);
    }

    private MessageExtract messageExtract() {
        final String schemaTypeString = getString(KuduSinkConnectorConfig.MESSAGE_EXTRACT_KEY);
        if (MessageExtract.DEBEZIUM.name().equals(schemaTypeString.toUpperCase())) {
            return MessageExtract.DEBEZIUM;
        } else {
            return MessageExtract.SCHEMA_REGISTRY;
        }
    }

    private String kuduMasters() {
        return this.getString(KUDU_MASTERS_KEY);
    }

    public int defaultPartitionBuckets() {
        return this.getInt(DEFAULT_PARTITION_BUCKETS_KEY);
    }

    public final String kudu_masters;
    public final String topicPrefix;
    public final String tableNamePrefix;
    public final int batchSize;

    public final int defaultPartitionBuckets;
    public final MessageExtract messageExtract;

    public KuduSinkConnectorConfig(Map<String, String> props) {

        super(configDef(), props);

        this.kudu_masters = kuduMasters();
        this.topicPrefix = topicPrefix();
        this.tableNamePrefix = tableNamePrefix();
        this.defaultPartitionBuckets = defaultPartitionBuckets();
        this.batchSize = batchSize();
        this.messageExtract = messageExtract();
    }
}
