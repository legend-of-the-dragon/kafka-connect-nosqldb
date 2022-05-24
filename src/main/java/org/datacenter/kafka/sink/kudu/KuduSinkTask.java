package org.datacenter.kafka.sink.kudu;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.datacenter.kafka.config.TopicNaming;
import org.datacenter.kafka.config.Version;
import org.datacenter.kafka.config.kudu.KuduSinkConnectorConfig;
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
public final class KuduSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(KuduSinkTask.class);

    private KuduSinkConnectorConfig sinkConfig;

    private KuduDialect kuduDialect;

    private Map<String, Pair<Schema, Schema>> oldTableSchamas = new HashMap<>();

    public String version() {
        return Version.getVersion();
    }

    public void start(Map<String, String> map) {

        this.sinkConfig = new KuduSinkConnectorConfig(map);
        kuduDialect = new KuduDialect(sinkConfig);

        log.info("Kudu Sink task started");
    }

    public void put(Collection<SinkRecord> records) {

        final int recordsCount = records.size();
        if (recordsCount > 0) {

            log.info(
                    "kudu sink Received {} records.  Writing them to the database...",
                    recordsCount);

            int writeCount = 0;

            for (SinkRecord sinkRecord : records) {

                String tableName = this.getTableName(sinkRecord.topic());

                boolean apply;
                Schema oldKeySchema = null;
                Schema oldValueSchema = null;

                Pair<Schema, Schema> schemaPair = oldTableSchamas.get(tableName);
                if (schemaPair != null) {
                    oldKeySchema = schemaPair.getKey();
                    oldValueSchema = schemaPair.getValue();
                }

                try {
                    if (sinkRecord.value() == null) {
                        if (!Objects.equals(oldKeySchema, sinkRecord.keySchema())) {
                            schemaChanged(tableName, oldValueSchema, sinkRecord);
                        }
                        apply = kuduDialect.applyDeleteRecord(tableName, sinkRecord);
                    } else {
                        if (!Objects.equals(oldValueSchema, sinkRecord.valueSchema())) {
                            schemaChanged(tableName, sinkRecord.valueSchema(), sinkRecord);
                        }
                        apply = kuduDialect.applyUpsertRecord(tableName, sinkRecord);
                    }
                } catch (Exception e) {
                    throw new DbDmlException("kuduDialect apply exception.", e);
                }

                if (apply) {
                    writeCount++;
                    if (writeCount >= sinkConfig.batchSize) {
                        flushAll();
                        writeCount = 0;
                    }
                } else {
                    throw new DbDmlException("apply sinkRecod exception.");
                }
            }
            flushAll();
            log.info("kudu sink write {} records. ", recordsCount);
        }
    }

    /**
     * 表结构变动相关的逻辑，三种情况可能导致schemaChangerd<br>
     * 1、创建表 2、修改表结构 3、表结构未变动(初始化)，该情况只要把缓存的表结构赋值即可。
     *
     * @param tableName
     * @param oldValueSchema
     * @param sinkRecord
     */
    private void schemaChanged(String tableName, Schema oldValueSchema, SinkRecord sinkRecord) {

        Schema newKeySchema = sinkRecord.keySchema();

        Schema newValueSchema = null;
        // 如果oldValueSchema，意味着还没有初始化或者上一次是delete dml语句，
        // alter和create table的时候只能先使用sinkRecord.valueSchema()
        // 如果sinkRecord.valueSchema()，意味着是delete dml语句，alter和create table的时候只能先使用oldValueSchema
        if (sinkRecord.valueSchema() == null) {
            if (oldValueSchema != null) {
                newValueSchema = oldValueSchema;
            } else {
                // TODO 这种情况属于初始化的时候，但是第一条记录是删除，怎么办？
                log.warn("");
            }
        } else {
            newValueSchema = sinkRecord.valueSchema();
        }

        boolean createOrAmendIfNecessary;

        boolean tableExists = kuduDialect.tableExists(tableName);

        if (tableExists) {

            // 对比schemaRegister中的schema和DB中的schema的差异，如果没有差异，加载即可，如果有差异，变更表结构
            boolean compare = kuduDialect.compare(tableName, newKeySchema, newValueSchema);
            if (!compare) {

                flushAll();

                kuduDialect.alterTable(tableName, newKeySchema, newValueSchema);
            }
        } else {

            flushAll();

            kuduDialect.createTable(tableName, newKeySchema, newValueSchema);
        }

        Pair<Schema, Schema> schemaPair = Pair.of(newKeySchema, newValueSchema);
        oldTableSchamas.put(tableName, schemaPair);
    }

    private void flushAll() {
        try {
            kuduDialect.flush();
        } catch (Exception e) {
            log.error("kuduDialect flush error." + e.getMessage());
            throw new DbDmlException("kuduDialect flush error.", e);
        }
    }

    public void stop() {
        kuduDialect.stop();
    }

    private String getTableName(String topic) {

        return (new TopicNaming(this.sinkConfig.topicPrefix, this.sinkConfig.tableNamePrefix))
                .tableName(topic);
    }
}
