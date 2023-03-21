package org.datacenter.kafka.sink.iceberg.connect;

import org.datacenter.kafka.config.Version;
import org.datacenter.kafka.sink.AbstractSinkTask;
import org.datacenter.kafka.sink.iceberg.IcebergDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * IcebergSinkTask
 *
 * @author sky
 * @date 2022-05-10
 */
public final class IcebergSinkTask extends AbstractSinkTask {

    private static final Logger log = LoggerFactory.getLogger(IcebergSinkTask.class);
    private static final String dialectName = "iceberg";

    @Override
    public String getDialectName() {
        return dialectName;
    }

    public String version() {
        return Version.getVersion();
    }

    public void start(Map<String, String> map) {

        this.sinkConfig = new IcebergSinkConnectorConfig(map);

        String taskIdString = map.get("taskId");
        int taskId = Integer.parseInt(taskIdString);
        this.dialect = new IcebergDialect((IcebergSinkConnectorConfig) sinkConfig, taskId);

        log.info("iceberg sink task started");
    }
}
