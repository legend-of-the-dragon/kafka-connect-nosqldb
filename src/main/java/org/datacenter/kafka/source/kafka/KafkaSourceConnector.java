package org.datacenter.kafka.source.kafka;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.datacenter.kafka.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * KafkaSourceConnector
 *
 * @author sky
 * @date 2023-10-10
 */
public class KafkaSourceConnector extends SourceConnector {

    private static final Logger log = LoggerFactory.getLogger(KafkaSourceConnector.class);

    private KafkaSourceConnectorConfig sourceConfig;

    @Override
    public void start(Map<String, String> map) {

        this.sourceConfig = new KafkaSourceConnectorConfig(map);
        log.info("start kafka source connector.");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return KafkaSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (maxTasks <= 0) {
            throw new IllegalArgumentException("Number of tasks must be a positive number.");
        } else {
            List<Map<String, String>> configs = new ArrayList<>(maxTasks);

            for (int i = 0; i < maxTasks; ++i) {
                configs.add(this.sourceConfig.originalsStrings());
            }

            return configs;
        }
    }

    @Override
    public void stop() {

        log.info("stop kafka source connector.");
    }

    @Override
    public ConfigDef config() {
        return KafkaSourceConnectorConfig.configDef();
    }

    @Override
    public String version() {
        log.info("get kafka source connector version.");
        return Version.getVersion();
    }
}
