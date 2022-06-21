package org.datacenter.kafka.config.ignite;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.datacenter.kafka.config.AbstractConnectorConfig;

import java.util.Map;

/**
 * IgniteSinkConnectorConfig
 *
 * @author sky
 * @date 2022-05-10
 */
public class IgniteSinkConnectorConfig extends AbstractConnectorConfig {

    public static final String IGNITE_CFG_KEY = "ignite.cfg";
    private static final String IGNITE_CFG_DOC =
            "Path to the Ignite configuration file. $IGNITE_HOME/config/default-config.xml is used if no Ignite config is configured";

    public static final String PARALLEL_OPS_KEY = "parallel.ops";
    public static final Integer PARALLEL_OPS_DEFAULT = 5;
    private static final String PARALLEL_OPS_DOC = "控制写入的并发度.默认5";

    public static final String SHALL_PROCESS_UPDATES_KEY = "shall.process.updates";
    public static final Boolean SHALL_PROCESS_UPDATES_DEFAULT = true;
    private static final String SHALL_PROCESS_UPDATES_DOC =
            "Indicates if overwriting or removing existing values in the sink cache is enabled. Sink connector performs better if this flag is disabled.";

    public static ConfigDef configDef() {

        return AbstractConnectorConfig.configDef()
                .define(IGNITE_CFG_KEY, Type.STRING, null, Importance.HIGH, IGNITE_CFG_DOC)
                .define(
                        PARALLEL_OPS_KEY,
                        Type.INT,
                        PARALLEL_OPS_DEFAULT,
                        Importance.HIGH,
                        PARALLEL_OPS_DOC)
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

    public int parallelOps() {
        return this.getInt(PARALLEL_OPS_KEY);
    }

    public final boolean shallProcessUpdates;

    public final int parallelOps;

    public IgniteSinkConnectorConfig(Map<String, String> props) {

        super(configDef(), props);

        this.parallelOps = parallelOps();
        this.shallProcessUpdates = shallProcessUpdates();
    }
}
