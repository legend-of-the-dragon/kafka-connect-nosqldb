package org.datacenter.kafka.sink.ignite;

import org.datacenter.kafka.config.TopicNaming;
import org.datacenter.kafka.config.Version;
import org.datacenter.kafka.config.ignite.IgniteSinkConnectorConfig;
import org.datacenter.kafka.sink.AbstractSinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * IgniteSinkTask
 *
 * @author sky
 * @date 2022-05-10
 */
public final class IgniteSinkTask extends AbstractSinkTask {
    private static final Logger log = LoggerFactory.getLogger(IgniteSinkTask.class);

    private static final String dialectName = "ignite";

    @Override
    public String getDialectName() {
        return dialectName;
    }

    public String version() {
        return Version.getVersion();
    }

    public void start(Map<String, String> map) {

        this.sinkConfig = new IgniteSinkConnectorConfig(map);
        dialect = new IgniteDialect((IgniteSinkConnectorConfig) sinkConfig);
        log.info("ignite Sink task started");
    }
}
