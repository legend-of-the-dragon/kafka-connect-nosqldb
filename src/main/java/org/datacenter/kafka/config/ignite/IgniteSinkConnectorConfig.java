package org.datacenter.kafka.config.ignite;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.time.ZoneId;
import java.util.Map;
import java.util.TimeZone;

/**
 * IgniteSinkConnectorConfig
 *
 * @author sky
 * @date 2022-05-10
 */
public class IgniteSinkConnectorConfig extends AbstractConfig {

    public static final String IGNITE_CFG_KEY = "ignite.cfg";
    public static final String TOPIC_PREFIX_KEY = "topic.prefix";
    public static final String CACHE_PREFIX_KEY = "cache.prefix";
    public static final String BATCH_SIZE_KEY = "batch.size";
    public static final String SHALL_PROCESS_UPDATES_KEY = "shall.process.updates";
    public static final Boolean SHALL_PROCESS_UPDATES_DEFAULT = true;
    public static final String CACHE_FILTER_KEY = "cache.filter";
    private static final String IGNITE_CFG_DOC =
            "Path to the Ignite configuration file. $IGNITE_HOME/config/default-config.xml is used if no Ignite config is configured";
    private static final String BATCH_SIZE_DOC =
            "Maximum number of entries to send to Ignite in single batch.";
    private static final String TOPIC_PREFIX_DOC =
            "Kafka topic is built from this prefix and cache name.";
    private static final String CACHE_PREFIX_DOC =
            "Sink cache name is built from this prefix and kafka topic without topic prefix. For example, if topic is 'ignite.person', topicPrefix is 'ignite.' and cachePrefix is 'ignite-' then sink cache name is 'ignite-person'.";
    private static final String SHALL_PROCESS_UPDATES_DOC =
            "Indicates if overwriting or removing existing values in the sink cache is enabled. Sink connector performs better if this flag is disabled.";
    private static final String DB_TIMEZONE_KEY = "db.timezone";

    public static ConfigDef configDef() {

        return (new ConfigDef())
                .define(IGNITE_CFG_KEY, Type.STRING, "", Importance.HIGH, IGNITE_CFG_DOC)
                .define(TOPIC_PREFIX_KEY, Type.STRING, "", Importance.HIGH, TOPIC_PREFIX_DOC)
                .define(BATCH_SIZE_KEY, Type.INT, 10000, Importance.LOW, BATCH_SIZE_DOC)
                .define(CACHE_PREFIX_KEY, Type.STRING, "", Importance.LOW, CACHE_PREFIX_DOC)
                .define(
                        SHALL_PROCESS_UPDATES_KEY,
                        Type.BOOLEAN,
                        SHALL_PROCESS_UPDATES_DEFAULT,
                        Importance.MEDIUM,
                        SHALL_PROCESS_UPDATES_DOC);
    }

    public String igniteCfg() {
        return this.getString(IGNITE_CFG_KEY);
    }

    public Integer batchSize() {
        return this.getInt(BATCH_SIZE_KEY);
    }

    public String topicPrefix() {
        return this.getString(TOPIC_PREFIX_KEY);
    }

    public String cachePrefix() {
        return this.getString(CACHE_PREFIX_KEY);
    }

    public Boolean shallProcessUpdates() {
        return this.getBoolean(SHALL_PROCESS_UPDATES_KEY);
    }

    public TimeZone timeZone() {
        String dbTimeZone = getString(DB_TIMEZONE_KEY);
        return TimeZone.getTimeZone(ZoneId.of(dbTimeZone));
    }

    public final String topicPrefix;
    public final int batchSize;

    public final String cachePrefix;

    public final boolean shallProcessUpdates;

    public IgniteSinkConnectorConfig(Map<String, String> props) {

        super(configDef(), props);

        this.topicPrefix = topicPrefix();
        this.cachePrefix = cachePrefix();
        this.batchSize = batchSize();
        this.shallProcessUpdates = shallProcessUpdates();
    }
}
