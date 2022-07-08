package org.datacenter.kafka.sink;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.datacenter.kafka.config.AbstractConnectorConfig;
import org.datacenter.kafka.config.TopicNaming;
import org.datacenter.kafka.util.SinkRecordUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.datacenter.kafka.util.SinkRecordUtil.schema2String;

/**
 * @author sky
 * @date 2022-05-25
 * @discription
 */
public abstract class AbstractSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(AbstractSinkTask.class);

    protected AbstractConnectorConfig sinkConfig;

    protected AbstractDialect<?, ?> dialect;

    private final Map<String, Pair<Schema, Schema>> oldTableSchamas = new HashMap<>();

    public abstract String getDialectName();

    public void put(Collection<SinkRecord> records) {

        final int recordsCount = records.size();
        if (recordsCount > 0) {

            log.info(
                    "{} sink Received {} records.  Writing them to the database...",
                    getDialectName(),
                    recordsCount);

            Pair<Boolean, Long> elasticLimit = dialect.elasticLimit(sinkConfig.connectorName);
            Boolean isLimit = elasticLimit.getKey();
            if (isLimit) {
                Long limitValue = elasticLimit.getValue();
                log.info("{}触发限速，限速时长:{}ms", sinkConfig.connectorName, limitValue);
                try {
                    Thread.sleep(limitValue);
                } catch (InterruptedException e) {
                    log.error("线程暂停异常.", e);
                }
                log.info("{}限速结束，限速时长:{}ms", sinkConfig.connectorName, limitValue);
            }

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

                        apply = dialect.applyDeleteRecord(tableName, sinkRecord);
                    } else {
                        if (!Objects.equals(oldKeySchema, sinkRecord.keySchema())
                                || !Objects.equals(oldValueSchema, sinkRecord.valueSchema())) {
                            schemaChanged(tableName, sinkRecord.valueSchema(), sinkRecord);
                        }

                        apply = dialect.applyUpsertRecord(tableName, sinkRecord);
                    }
                } catch (Exception e) {
                    log.error(
                            "{} sinkTask apply exception.sinkRecordKeySchema:{},sinkRecord.key:{}.sinkRecordValueSchema:{}.sinkRecord.value:{}",
                            getDialectName(),
                            ConnectSchemaToString(sinkRecord.keySchema()),
                            sinkRecord.key(),
                            ConnectSchemaToString(sinkRecord.valueSchema()),
                            sinkRecord.value());
                    throw new DbDmlException(getDialectName() + " sinkTask apply exception.", e);
                }

                if (apply) {
                    writeCount++;
                    if (writeCount >= sinkConfig.batchSize) {
                        flushAll();
                        log.info("{} sink flush {} records. ", getDialectName(), writeCount);
                        writeCount = 0;
                    }
                } else {
                    throw new DbDmlException(getDialectName() + " flushAll sinkRecod exception.");
                }
            }
            flushAll();
            log.info("{} sink write {} records. ", getDialectName(), recordsCount);
        }
    }

    private String ConnectSchemaToString(Schema connectSchema) {

        StringBuilder result = new StringBuilder("Schema{");
        result.append("type:").append(connectSchema.type());
        if (connectSchema.name() != null) {
            result.append(",name:").append(connectSchema.name());
        }
        List<Field> fields = connectSchema.fields();
        result.append(",fields:[");
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            result.append("{").append(field).append("}");
            if (i < fields.size() - 1) {
                result.append(",");
            }
        }
        result.append("]}");
        return result.toString();
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
                log.error(
                        "{} sink,这种情况属于初始化的时候，但是第一条记录是删除.{},keySchema:{},valueSchema:{}",
                        getDialectName(),
                        sinkRecord.toString(),
                        SinkRecordUtil.schema2String(sinkRecord.keySchema()),
                        SinkRecordUtil.schema2String(sinkRecord.valueSchema()));
            }
        } else {
            newValueSchema = sinkRecord.valueSchema();
        }

        boolean tableExists = dialect.tableExists(tableName);

        if (tableExists) {

            // 对比schemaRegister中的schema和DB中的schema的差异，如果没有差异，加载即可，如果有差异，变更表结构
            boolean compare = dialect.compare(tableName, newKeySchema, newValueSchema);
            if (!compare) {

                flushAll();

                try {
                    dialect.alterTable(tableName, newKeySchema, newValueSchema);
                } catch (Throwable e) {
                    log.error(
                            "{} sink alterTable error:{}",
                            getDialectName(),
                            sinkRecord.toString(),
                            e);
                    throw new DbDdlException(e);
                }
            }
        } else {

            flushAll();
            try {
                dialect.createTable(tableName, newKeySchema, newValueSchema);
            } catch (Throwable e) {
                log.error(
                        "{} sink createTable error.keySchema:{},valueSchema:{}",
                        getDialectName(),
                        schema2String(sinkRecord.keySchema()),
                        schema2String(sinkRecord.valueSchema()),
                        e);
                throw new DbDdlException(e);
            }
        }

        Pair<Schema, Schema> schemaPair = Pair.of(newKeySchema, newValueSchema);
        oldTableSchamas.put(tableName, schemaPair);
    }

    private void flushAll() {
        try {
            dialect.flush();
        } catch (Exception e) {
            log.error("{} sinkTask flush error.", getDialectName(), e);
            throw new DbDmlException(getDialectName() + " sinkTask flush error.", e);
        }
    }

    public void stop() {
        log.info("task will stop.");
        dialect.stop();
    }

    public String getTableName(String topic) {

        return (new TopicNaming(
                        this.sinkConfig.topicReplacePrefix,
                        this.sinkConfig.tableNamePrefix,
                        sinkConfig.table_name_format))
                .tableName(topic);
    }
}
