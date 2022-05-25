package org.datacenter.kafka.sink.ignite;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.datacenter.kafka.config.Version;
import org.datacenter.kafka.config.ignite.IgniteSinkConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * IgniteSinkConnector
 *
 * @author sky
 * @date 2022-05-10
 */
public class GridgainSinkConnector extends SinkConnector {

    private static final Logger log = LoggerFactory.getLogger(GridgainSinkConnector.class);

    private IgniteSinkConnectorConfig sinkConfig;

    @Override
    public void start(Map<String, String> map) {

        this.sinkConfig = new IgniteSinkConnectorConfig(map);

        log.info("start ignite sink connector.");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return IgniteSinkTask.class;
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
    public void stop() {

        log.info("stop ignite sink connector.");
    }

    @Override
    public ConfigDef config() {
        return IgniteSinkConnectorConfig.configDef();
    }

    @Override
    public String version() {
        return Version.getVersion();
    }
}
