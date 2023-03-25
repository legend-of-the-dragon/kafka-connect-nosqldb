package org.datacenter.kafka.sink.iceberg.connect;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.datacenter.kafka.sink.AbstractConnectorConfig;
import org.datacenter.kafka.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * IcebergSinkConnector
 *
 * @author sky
 * @date 2022-05-10
 */
public class IcebergSinkConnector extends SinkConnector {

    private static final Logger log = LoggerFactory.getLogger(IcebergSinkConnector.class);

    private AbstractConnectorConfig sinkConfig;

    @Override
    public void start(Map<String, String> map) {

        this.sinkConfig = new IcebergSinkConnectorConfig(map);
        log.info("start iceberg sink connector.");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return IcebergSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (maxTasks <= 0) {
            throw new IllegalArgumentException("Number of tasks must be a positive number.");
        } else {
            List<Map<String, String>> configs = new ArrayList<>(maxTasks);

            for (int i = 0; i < maxTasks; ++i) {
                Map<String, String> stringStringMap = this.sinkConfig.originalsStrings();
                stringStringMap.put("taskId", String.valueOf(i));
                configs.add(stringStringMap);
            }

            return configs;
        }
    }

    @Override
    public void stop() {

        log.info("stop iceberg sink connector.");
    }

    @Override
    public ConfigDef config() {
        return IcebergSinkConnectorConfig.configDef();
    }

    @Override
    public String version() {
        log.info("get iceberg sink connector version.");
        return Version.getVersion();
    }
}
