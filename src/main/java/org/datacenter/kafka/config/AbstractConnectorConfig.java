package org.datacenter.kafka.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

/**
 * AbstractConnectorConfig
 *
 * @author sky
 * @date 2022-05-10
 */
public class AbstractConnectorConfig extends AbstractConfig {

    public static final String TOPIC_REPLACE_PREFIX_KEY = "topic.replace.prefix";

    public static final String TABLE_NAME_FORMAT_KEY = "table.name.format";
    public static final String TABLE_NAME_FORMAT_DEFAULT = "_";
    public static final String TABLE_NAME_PREFIX_KEY = "table.name.prefix";
    public static final String TABLE_NAME_PREFIX_DEFAULT = "";

    public static final String MESSAGE_EXTRACT_KEY = "message.extract";
    public static final String MESSAGE_EXTRACT_DEFAULT = "SCHEMA_REGISTRY";
    public static final String BATCH_SIZE_KEY = "batch.size";
    public static final Integer BATCH_SIZE_DEFAULT = 10000;

    private static final String BATCH_SIZE_DOC =
            "Maximum number of entries to send to Ignite in single batch.";
    private static final String TOPIC_PREFIX_DOC =
            "Kafka topic is built from this prefix and cache name.";

    public enum MessageExtract {
        DEBEZIUM,
        SCHEMA_REGISTRY;
    }

    public static ConfigDef configDef() {

        return new ConfigDef()
                .define(
                        TOPIC_REPLACE_PREFIX_KEY,
                        Type.STRING,
                        "",
                        Importance.HIGH,
                        TOPIC_PREFIX_DOC)
                .define(
                        TABLE_NAME_PREFIX_KEY,
                        Type.STRING,
                        TABLE_NAME_PREFIX_DEFAULT,
                        Importance.LOW,
                        "")
                .define(
                        TABLE_NAME_FORMAT_KEY,
                        Type.STRING,
                        TABLE_NAME_FORMAT_DEFAULT,
                        Importance.LOW,
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

    public String topicReplacePrefix() {
        return this.getString(TOPIC_REPLACE_PREFIX_KEY);
    }

    private String table_name_format() {
        return this.getString(TABLE_NAME_FORMAT_KEY);
    }

    public String tableNamePrefix() {
        return this.getString(TABLE_NAME_PREFIX_KEY);
    }

    private MessageExtract messageExtract() {
        final String schemaTypeString = getString(AbstractConnectorConfig.MESSAGE_EXTRACT_KEY);
        if (MessageExtract.DEBEZIUM.name().equals(schemaTypeString.toUpperCase())) {
            return MessageExtract.DEBEZIUM;
        } else {
            return MessageExtract.SCHEMA_REGISTRY;
        }
    }

    public final String topicReplacePrefix;
    public final String table_name_format;
    public final String tableNamePrefix;
    public final int batchSize;
    public final MessageExtract messageExtract;

    public AbstractConnectorConfig(ConfigDef configDef, Map<String, String> props) {

        super(configDef, props);

        this.topicReplacePrefix = topicReplacePrefix();
        this.table_name_format = table_name_format();
        this.tableNamePrefix = tableNamePrefix();
        this.batchSize = batchSize();
        this.messageExtract = messageExtract();
    }
}
