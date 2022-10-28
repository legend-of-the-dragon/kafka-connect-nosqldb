package org.datacenter.kafka.config.kudu;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.datacenter.kafka.config.AbstractConnectorConfig;

import java.util.Map;

/**
 * KuduSinkConnectorConfig
 *
 * @author sky
 * @date 2022-05-10
 */
public class KuduSinkConnectorConfig extends AbstractConnectorConfig {

    public static final String KUDU_MASTERS_KEY = "kudu.masters";
    public static final String DEFAULT_PARTITION_BUCKETS_KEY = "default.partition.buckets";
    public static final Integer DEFAULT_PARTITION_BUCKETS_DEFAULT = 5;

    public static final String ALLOW_RECORD_FIELDS_LESSTHAN_TABLE_FIELDS_KEY =
            "allow-record-fields-less-than-table-fields";
    public static final Boolean ALLOW_RECORD_FIELDS_LESSTHAN_TABLE_FIELDS_DEFAULT = true;
    public static final String ALLOW_RECORD_FIELDS_LESSTHAN_TABLE_FIELDS_DOC =
            "允许record的数据字段比目标表的字段少,默认允许,就是说只要不新增字段,可以只插入或者更新表中的一部分字段.";

    public static ConfigDef configDef() {

        return AbstractConnectorConfig.configDef()
                .define(KUDU_MASTERS_KEY, Type.STRING, "", Importance.HIGH, "")
                .define(
                        DEFAULT_PARTITION_BUCKETS_KEY,
                        Type.INT,
                        DEFAULT_PARTITION_BUCKETS_DEFAULT,
                        Importance.HIGH,
                        "")
                .define(
                        ALLOW_RECORD_FIELDS_LESSTHAN_TABLE_FIELDS_KEY,
                        Type.BOOLEAN,
                        ALLOW_RECORD_FIELDS_LESSTHAN_TABLE_FIELDS_DEFAULT,
                        Importance.LOW,
                        ALLOW_RECORD_FIELDS_LESSTHAN_TABLE_FIELDS_DOC);
    }

    private String kuduMasters() {
        return this.getString(KUDU_MASTERS_KEY);
    }

    public int defaultPartitionBuckets() {
        return this.getInt(DEFAULT_PARTITION_BUCKETS_KEY);
    }

    public boolean allowRecordFieldsLessThanTableFields() {
        return this.getBoolean(ALLOW_RECORD_FIELDS_LESSTHAN_TABLE_FIELDS_KEY);
    }

    public final String kudu_masters;
    public final int defaultPartitionBuckets;
    public final boolean allowRecordFieldsLessThanTableFields;

    public KuduSinkConnectorConfig(Map<String, String> props) {

        super(configDef(), props);

        this.kudu_masters = kuduMasters();
        this.defaultPartitionBuckets = defaultPartitionBuckets();
        this.allowRecordFieldsLessThanTableFields = allowRecordFieldsLessThanTableFields();
    }
}
