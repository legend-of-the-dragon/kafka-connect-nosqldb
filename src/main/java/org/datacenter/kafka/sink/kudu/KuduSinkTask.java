package org.datacenter.kafka.sink.kudu;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.datacenter.kafka.config.TopicNaming;
import org.datacenter.kafka.config.Version;
import org.datacenter.kafka.config.kudu.KuduSinkConnectorConfig;
import org.datacenter.kafka.sink.AbstractSinkTask;
import org.datacenter.kafka.sink.DbDmlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * IgniteSinkTask
 *
 * @author sky
 * @date 2022-05-10
 */
public final class KuduSinkTask extends AbstractSinkTask {

    private static final Logger log = LoggerFactory.getLogger(KuduSinkTask.class);

    public String version() {
        return Version.getVersion();
    }

    public void start(Map<String, String> map) {

        this.sinkConfig = new KuduSinkConnectorConfig(map);
        this.abstractDialect = new KuduDialect((KuduSinkConnectorConfig)sinkConfig);

        log.info("Kudu Sink task started");
    }

}
