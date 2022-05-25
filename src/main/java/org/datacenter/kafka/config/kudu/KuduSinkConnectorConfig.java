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

    public static ConfigDef configDef() {

        return AbstractConnectorConfig.configDef()
                .define(KUDU_MASTERS_KEY, Type.STRING, "", Importance.HIGH, "")
                .define(
                        DEFAULT_PARTITION_BUCKETS_KEY,
                        Type.INT,
                        DEFAULT_PARTITION_BUCKETS_DEFAULT,
                        Importance.HIGH,
                        "");
    }

    private String kuduMasters() {
        return this.getString(KUDU_MASTERS_KEY);
    }

    public int defaultPartitionBuckets() {
        return this.getInt(DEFAULT_PARTITION_BUCKETS_KEY);
    }

    public final String kudu_masters;

    public final int defaultPartitionBuckets;

    public KuduSinkConnectorConfig(Map<String, String> props) {

        super(configDef(), props);

        this.kudu_masters = kuduMasters();
        this.defaultPartitionBuckets = defaultPartitionBuckets();
    }
}
