package org.datacenter.kafka.sink;

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
    public static final String TOPIC_REPLACE_PREFIX_DEFAULT = null;
    private static final String TOPIC_REPLACE_PREFIX_DOC =
            "按照配置的内容作为前缀把topicName的值替换掉. eg: topicName为db51044.sky_test.t_wk_quota_biz的时候,topic.replace.prefix为\"db51044.sky_test.\"则会导致tableName变为t_wk_quota_biz.";

    public static final String TABLE_NAME_FORMAT_KEY = "table.name.format";
    public static final String TABLE_NAME_FORMAT_DEFAULT = "_";
    private static final String TABLE_NAME_FORMAT_DOC =
            "把剩余的tableName中的'.'替换成配置的内容. eg: topicName为db51044.sky_test.t_wk_quota_biz的时候,table.name.format为\"_\"则会导致tableName变为db51044_sky_test_t_wk_quota_biz。注意topic.replace.prefix会优先执行.";

    public static final String TABLE_NAME_PREFIX_KEY = "table.name.prefix";
    public static final String TABLE_NAME_PREFIX_DEFAULT = null;
    private static final String TABLE_NAME_PREFIX_DOC =
            "按照配置的内容作为前缀加上把剩余的tableName作为新的tableName.eg:tableName已经为t_wk_quota_biz的时候，table.name.prefix配置为\"test_\",最终的tableName则为test_t_wk_quota_biz.";

    public static final String MESSAGE_EXTRACT_KEY = "message.extract";
    public static final String MESSAGE_EXTRACT_DEFAULT = "SCHEMA_REGISTRY";

    private static final String MESSAGE_EXTRACT_DOC =
            "kafka中存储的数据行的数据结构，值的选项为\"SCHEMA_REGISTRY\"、\"DEBEZIUM\"";
    public static final String BATCH_SIZE_KEY = "batch.size";
    public static final Integer BATCH_SIZE_DEFAULT = 1000;
    private static final String BATCH_SIZE_DOC =
            "一次写数据库的最大条数。注：批量写入数据库有助于提供效率，但是太高了可能会导致可能奇葩的故障出现.";

    public static final String DELETE_ENABLED = "delete.enabled";
    private static final Boolean DELETE_ENABLED_DEFAULT = true;
    private static final String DELETE_ENABLED_DOC =
            "Whether to treat ``null`` record values as deletes. Requires ``pk.mode`` "
                    + "to be ``record_key``.";

    public static final String RATE_LIMIT_PROMETHEUS_SERVER_KEY = "rate.limit.prometheus.server";
    public static final String RATE_LIMIT_PROMETHEUS_SERVER_DEFAULT = "http://172.30.80.70:9090/";
    private static final String RATE_LIMIT_PROMETHEUS_SERVER_DOC = "用于限速计算的prometheus地址.";

    public static final String RATE_LIMIT_ENABLED = "limit.enabled";
    private static final Boolean RATE_LIMIT_ENABLED_DEFAULT = true;
    private static final String RATE_LIMIT_ENABLED_DOC = "是否限速，默认不限速.";

    public static final String ALLOW_RECORD_FIELDS_LESSTHAN_TABLE_FIELDS_KEY =
            "allow-record-fields-less-than-table-fields";
    public static final Boolean ALLOW_RECORD_FIELDS_LESSTHAN_TABLE_FIELDS_DEFAULT = false;
    public static final String ALLOW_RECORD_FIELDS_LESSTHAN_TABLE_FIELDS_DOC =
            "允许record的数据字段比目标表的字段少,默认允许,就是说只要不新增字段,可以只插入或者更新表中的一部分字段.";

    public static final String IGNORE_NULL_VALUES_KEY = "ignore-null-values";
    public static final Boolean IGNORE_NULL_VALUES_DEFAULT = true;
    public static final String IGNORE_NULL_VALUES_DOC =
            "是否允许record中的空字段覆盖数据库中的数据，默认允许，以record输入的为准.";

    public static final String TIME_ZONED_SOURCE_KEY = "timezoned.source";
    private static final Integer TIME_ZONED_SOURCE_DEFAULT = 8;
    private static final String TIME_ZONED_SOURCE_DOC = "源数据库的时区.";

    public static final String TIME_ZONED_TARGET_KEY = "timezoned.target";
    private static final Integer TIME_ZONED_TARGET_DEFAULT = 8;
    private static final String TIME_ZONED_TARGET_DOC = "目标数据库的时区.";

    public static final String RATE_LIMITING_VALUE_KEY = "rate.limiting.value";
    public static final Integer RATE_LIMITING_VALUE_DEFAULT = -1;
    private static final String RATE_LIMITING_VALUE_DOC = "控制写入速度，单位records/s，最小为1，最大-1.默认-1";

    public static final String RATE_LIMITING_TYPE_KEY = "rate.limiting.type";
    public static final String RATE_LIMITING_TYPE_DEFAULT = "catalog";
    private static final String RATE_LIMITING_TYPE_DOC =
            "控制写入速度的控制单位，可选值为：catalog、database、custom-lable";

    public static final String RATE_LIMITING_LABEL_KEY = "rate.limiting.custom-lable";
    public static final String RATE_LIMITING_LABEL_DEFAULT = null;
    private static final String RATE_LIMITING_LABEL_DOC = "控制写入速度的自定义标签.无默认值.";

    public enum MessageExtract {
        DEBEZIUM,
        SCHEMA_REGISTRY
    }

    public static ConfigDef configDef() {

        return new ConfigDef()
                .define(
                        TOPIC_REPLACE_PREFIX_KEY,
                        Type.STRING,
                        TOPIC_REPLACE_PREFIX_DEFAULT,
                        Importance.HIGH,
                        TOPIC_REPLACE_PREFIX_DOC)
                .define(
                        TABLE_NAME_PREFIX_KEY,
                        Type.STRING,
                        TABLE_NAME_PREFIX_DEFAULT,
                        Importance.LOW,
                        TABLE_NAME_PREFIX_DOC)
                .define(
                        TABLE_NAME_FORMAT_KEY,
                        Type.STRING,
                        TABLE_NAME_FORMAT_DEFAULT,
                        Importance.LOW,
                        TABLE_NAME_FORMAT_DOC)
                .define(
                        BATCH_SIZE_KEY,
                        Type.INT,
                        BATCH_SIZE_DEFAULT,
                        Importance.LOW,
                        BATCH_SIZE_DOC)
                .define(
                        DELETE_ENABLED,
                        Type.BOOLEAN,
                        DELETE_ENABLED_DEFAULT,
                        Importance.MEDIUM,
                        DELETE_ENABLED_DOC)
                .define(
                        RATE_LIMIT_PROMETHEUS_SERVER_KEY,
                        Type.STRING,
                        RATE_LIMIT_PROMETHEUS_SERVER_DEFAULT,
                        Importance.LOW,
                        RATE_LIMIT_PROMETHEUS_SERVER_DOC)
                .define(
                        RATE_LIMIT_ENABLED,
                        Type.BOOLEAN,
                        RATE_LIMIT_ENABLED_DEFAULT,
                        Importance.LOW,
                        RATE_LIMIT_ENABLED_DOC)
                .define(
                        ALLOW_RECORD_FIELDS_LESSTHAN_TABLE_FIELDS_KEY,
                        Type.BOOLEAN,
                        ALLOW_RECORD_FIELDS_LESSTHAN_TABLE_FIELDS_DEFAULT,
                        Importance.LOW,
                        ALLOW_RECORD_FIELDS_LESSTHAN_TABLE_FIELDS_DOC)
                .define(
                        IGNORE_NULL_VALUES_KEY,
                        Type.BOOLEAN,
                        IGNORE_NULL_VALUES_DEFAULT,
                        Importance.LOW,
                        IGNORE_NULL_VALUES_DOC)
                .define(
                        MESSAGE_EXTRACT_KEY,
                        Type.STRING,
                        MESSAGE_EXTRACT_DEFAULT,
                        Importance.LOW,
                        MESSAGE_EXTRACT_DOC)
                .define(
                        TIME_ZONED_SOURCE_KEY,
                        Type.INT,
                        TIME_ZONED_SOURCE_DEFAULT,
                        Importance.LOW,
                        TIME_ZONED_SOURCE_DOC)
                .define(
                        TIME_ZONED_TARGET_KEY,
                        Type.INT,
                        TIME_ZONED_TARGET_DEFAULT,
                        Importance.LOW,
                        TIME_ZONED_TARGET_DOC);
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

    public String rateLimitPrometheusServer() {
        return this.getString(RATE_LIMIT_PROMETHEUS_SERVER_KEY);
    }

    public boolean rateLimitEnabled() {
        return this.getBoolean(RATE_LIMIT_ENABLED);
    }

    public String tableNamePrefix() {
        return this.getString(TABLE_NAME_PREFIX_KEY);
    }

    public boolean deleteEnabled() {
        return this.getBoolean(DELETE_ENABLED);
    }

    public boolean allowRecordFieldsLessThanTableFields() {
        return this.getBoolean(ALLOW_RECORD_FIELDS_LESSTHAN_TABLE_FIELDS_KEY);
    }

    public boolean isIgnoreNullValues() {
        return this.getBoolean(IGNORE_NULL_VALUES_KEY);
    }

    private MessageExtract messageExtract() {
        final String schemaTypeString = getString(AbstractConnectorConfig.MESSAGE_EXTRACT_KEY);
        if (MessageExtract.DEBEZIUM.name().equals(schemaTypeString.toUpperCase())) {
            return MessageExtract.DEBEZIUM;
        } else {
            return MessageExtract.SCHEMA_REGISTRY;
        }
    }

    public Integer timeZonedSource() {
        return this.getInt(TIME_ZONED_SOURCE_KEY);
    }

    public Integer timeZonedTarget() {
        return this.getInt(TIME_ZONED_TARGET_KEY);
    }

    public final String topicReplacePrefix;
    public final String table_name_format;
    public final String tableNamePrefix;
    public final int batchSize;
    public final boolean deleteEnabled;
    public final String rateLimitPrometheusServer;
    public final boolean rateLimitEnabled;
    public final boolean allowRecordFieldsLessThanTableFields;
    public final MessageExtract messageExtract;
    public final int timeZonedSource;
    public final int timeZonedTarget;
    public final String connectorName;
    public final boolean ignoreNullValues;

    public AbstractConnectorConfig(ConfigDef configDef, Map<String, String> props) {

        super(configDef, props);

        this.connectorName = props.get("name");
        this.topicReplacePrefix = topicReplacePrefix();
        this.table_name_format = table_name_format();
        this.tableNamePrefix = tableNamePrefix();
        this.batchSize = batchSize();
        this.deleteEnabled = deleteEnabled();
        this.allowRecordFieldsLessThanTableFields = allowRecordFieldsLessThanTableFields();
        this.ignoreNullValues = isIgnoreNullValues();
        this.messageExtract = messageExtract();
        this.timeZonedSource = timeZonedSource();
        this.timeZonedTarget = timeZonedTarget();
        this.rateLimitEnabled = rateLimitEnabled();
        this.rateLimitPrometheusServer = rateLimitPrometheusServer();
    }
}
