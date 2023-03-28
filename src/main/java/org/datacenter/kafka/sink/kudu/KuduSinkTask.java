package org.datacenter.kafka.sink.kudu;

import org.datacenter.kafka.Version;
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
public final class KuduSinkTask extends AbstractSinkTask {

    private static final Logger log = LoggerFactory.getLogger(KuduSinkTask.class);
    private static final String dialectName = "kudu";

    @Override
    public String getDialectName() {
        return dialectName;
    }

    public String version() {
        return Version.getVersion();
    }

    public void start(Map<String, String> map) {

        this.sinkConfig = new KuduSinkConnectorConfig(map);
        this.dialect = new KuduDialect((KuduSinkConnectorConfig) sinkConfig);

        log.info("Kudu Sink task started");
    }
}
