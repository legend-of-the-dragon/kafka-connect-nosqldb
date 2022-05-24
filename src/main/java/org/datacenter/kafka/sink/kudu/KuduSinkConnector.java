package org.datacenter.kafka.sink.kudu;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.datacenter.kafka.config.Version;
import org.datacenter.kafka.config.kudu.KuduSinkConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * KuduSinkConnector
 *
 * @author sky
 * @date 2022-05-10
 */
public class KuduSinkConnector extends SinkConnector {

    private static final Logger log = LoggerFactory.getLogger(KuduSinkConnector.class);

    private KuduSinkConnectorConfig sinkConfig;

    @Override
    public void start(Map<String, String> map) {

        this.sinkConfig = new KuduSinkConnectorConfig(map);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return KuduSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (maxTasks <= 0) {
            throw new IllegalArgumentException("Number of tasks must be a positive number.");
        } else {
            List<Map<String, String>> configs = new ArrayList<>(maxTasks);

            for (int i = 0; i < maxTasks; ++i) {
                configs.add(this.sinkConfig.originalsStrings());
            }

            return configs;
        }
    }

    @Override
    public void stop() {}

    @Override
    public ConfigDef config() {
        return KuduSinkConnectorConfig.configDef();
    }

    @Override
    public String version() {
        return Version.getVersion();
    }
}
