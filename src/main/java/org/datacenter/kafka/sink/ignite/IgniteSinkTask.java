package org.datacenter.kafka.sink.ignite;

import org.apache.ignite.IgniteDataStreamer;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.datacenter.kafka.CacheEntry;
import org.datacenter.kafka.DataGrid;
import org.datacenter.kafka.config.TopicNaming;
import org.datacenter.kafka.config.Version;
import org.datacenter.kafka.config.ignite.IgniteSinkConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

/**
 * IgniteSinkTask
 *
 * @author sky
 * @date 2022-05-10
 */
public final class IgniteSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(IgniteSinkTask.class);

    private final Map<String, IgniteDataStreamer<Object, Object>> dataStreamers = new HashMap<>();
    private volatile boolean isStarted;
    private IgniteSinkConnectorConfig cfg;
    private SinkRecordParser recordParser;
    private Predicate<CacheEntry> filter;

    public IgniteSinkTask() {}

    public String version() {
        return Version.getVersion();
    }

    public void start(Map<String, String> map) {

        if (!this.isStarted) {
            this.cfg = new IgniteSinkConnectorConfig(map);
            this.recordParser = new SinkRecordParser(this.cfg.timeZone());
            DataGrid.SINK.init(this.cfg.igniteCfg());
            this.isStarted = true;
            log.info("ignite Sink task started");
        }
    }

    public void put(Collection<SinkRecord> records) {

        final int recordsCount = records.size();
        int writeCount = 0;
        log.debug("Received {} records.  Writing them to the database...", recordsCount);

        for (SinkRecord sinkRecord : records) {

            String cacheName =
                    new TopicNaming(this.cfg.topicPrefix, this.cfg.cachePrefix)
                            .tableName(sinkRecord.topic());

            try {
                Object key = this.recordParser.parseKey(sinkRecord);
                Object value =
                        sinkRecord.value() == null
                                ? null
                                : this.recordParser.parseValue(sinkRecord);

                IgniteDataStreamer<Object, Object> dataStreamer = this.getDataStreamer(cacheName);
                if (value == null) {
                    dataStreamer.removeData(key);
                } else {
                    dataStreamer.addData(key, value);
                }
                writeCount++;
                if (writeCount >= cfg.batchSize) {
                    flushAll();
                    writeCount = 0;
                }
            } catch (Exception e) {
                log.warn("ignite sink apply exception.", e);
                throw new ConnectException("ignite sink apply exception.", e);
            }
        }

        flushAll();

        log.debug("write {} records. ", recordsCount);
    }

    private void flushAll() {
        try {
            for (IgniteDataStreamer<Object, Object> igniteDataStreamer : dataStreamers.values()) {
                igniteDataStreamer.flush();
            }
        } catch (Exception e) {
            log.error("ignite sink flush exception.", e);
            throw new ConnectException("ignite sink flush exception.", e);
        }
    }

    public void stop() {
        if (this.isStarted) {

            for (IgniteDataStreamer<Object, Object> igniteDataStreamer :
                    this.dataStreamers.values()) {
                try {
                    igniteDataStreamer.close();
                } catch (Exception e) {
                    log.error("igniteStreamer close exception.", e);
                }
            }

            DataGrid.SINK.close();
            this.isStarted = false;
            log.info("ignite Sink task stopped.");
        }
    }

    private IgniteDataStreamer<Object, Object> getDataStreamer(String cacheName) {
        IgniteDataStreamer<Object, Object> igniteDataStreamer = this.dataStreamers.get(cacheName);
        if (igniteDataStreamer == null) {
            DataGrid.SINK.ensureCache(cacheName);
            igniteDataStreamer = DataGrid.SINK.dataStreamer(cacheName);
            if (this.cfg.shallProcessUpdates) {
                igniteDataStreamer.allowOverwrite(true);
            }

            this.dataStreamers.put(cacheName, igniteDataStreamer);
        }

        return igniteDataStreamer;
    }
}
