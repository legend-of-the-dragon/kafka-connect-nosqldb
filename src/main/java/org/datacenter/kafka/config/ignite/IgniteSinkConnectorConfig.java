package org.datacenter.kafka.config.ignite;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.datacenter.kafka.config.AbstractConnectorConfig;

import java.time.ZoneId;
import java.util.Map;
import java.util.TimeZone;

/**
 * IgniteSinkConnectorConfig
 *
 * @author sky
 * @date 2022-05-10
 */
public class IgniteSinkConnectorConfig extends AbstractConnectorConfig {

    public static final String IGNITE_CFG_KEY = "ignite.cfg";
    public static final String SHALL_PROCESS_UPDATES_KEY = "shall.process.updates";
    public static final Boolean SHALL_PROCESS_UPDATES_DEFAULT = true;
    private static final String IGNITE_CFG_DOC =
            "Path to the Ignite configuration file. $IGNITE_HOME/config/default-config.xml is used if no Ignite config is configured";
    private static final String SHALL_PROCESS_UPDATES_DOC =
            "Indicates if overwriting or removing existing values in the sink cache is enabled. Sink connector performs better if this flag is disabled.";
    private static final String DB_TIMEZONE_KEY = "db.timezone";

    public static ConfigDef configDef() {

        return AbstractConnectorConfig.configDef()
                .define(IGNITE_CFG_KEY, Type.STRING, "", Importance.HIGH, IGNITE_CFG_DOC)
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

    public Boolean shallProcessUpdates() {
        return this.getBoolean(SHALL_PROCESS_UPDATES_KEY);
    }

    public final boolean shallProcessUpdates;

    public IgniteSinkConnectorConfig(Map<String, String> props) {

        super(configDef(), props);

        this.shallProcessUpdates = shallProcessUpdates();
    }
}
