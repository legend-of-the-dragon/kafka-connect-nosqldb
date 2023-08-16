package org.datacenter.kafka.sink;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.datacenter.kafka.TopicNaming;
import org.datacenter.kafka.sink.exception.DbDdlException;
import org.datacenter.kafka.sink.exception.DbDmlException;
import org.datacenter.kafka.sink.ignite.IgniteDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

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

    private final Map<String, Pair<Schema, Schema>> oldTableSchamasCache = new HashMap<>();

    public abstract String getDialectName();

    public void put(Collection<SinkRecord> records) {

        final int recordsCount = records.size();
        if (recordsCount > 0) {

            log.info(
                    "{} sink Received {} records.  Writing them to the database...",
                    getDialectName(),
                    recordsCount);

            if (sinkConfig.rateLimitEnabled) {
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
            }

            int writeCount = 0;
            for (SinkRecord sinkRecord : records) {

                String tableName = this.getTableName(sinkRecord.topic());

                if (sinkRecord.key() == null) {
                    throw new DbDmlException("table:" + tableName + ",sinkRecord.key() == null");
                }

                boolean apply = true;
                Schema oldKeySchema = null;
                Schema oldValueSchema = null;

                Pair<Schema, Schema> schemaPair = oldTableSchamasCache.get(tableName);
                if (schemaPair != null) {
                    oldKeySchema = schemaPair.getKey();
                    oldValueSchema = schemaPair.getValue();
                }

                try {
                    if (sinkRecord.value() == null) {

                        // 判断缓存中是否存在之前的表结构
                        if (oldValueSchema == null) {
                            // 如果不存在表结构缓存，检查一下是否存在表
                            boolean tableExists = dialect.tableExists(tableName);

                            // 如果表也不存在，说明是表不存在的情况下，第一条数据就是delete，这种暂时不支持处理，则跳过这一条数据的处理
                            if (!tableExists) {
                                log.error(
                                        "这种情况属于初始化的时候，但是第一条记录是删除,跳过当前record.{},key:{}",
                                        tableName,
                                        sinkRecord.key());
                                writeCount++;
                                continue;
                            }
                        }

                        if (!Objects.equals(oldKeySchema, sinkRecord.keySchema())) {
                            schemaChanged(tableName, sinkRecord);
                        }

                        if (sinkConfig.deleteEnabled) {
                            apply = dialect.applyDeleteRecord(tableName, sinkRecord);
                        }
                    } else {
                        if (!Objects.equals(oldKeySchema, sinkRecord.keySchema())
                                || !Objects.equals(oldValueSchema, sinkRecord.valueSchema())) {
                            schemaChanged(tableName, sinkRecord);
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
            context.requestCommit();
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
     * @param sinkRecord
     */
    private void schemaChanged(String tableName, SinkRecord sinkRecord) {

        if (dialect instanceof IgniteDialect) {
            return;
        }

        Schema newKeySchema = sinkRecord.keySchema();
        Schema newValueSchema = sinkRecord.valueSchema();

        // 如果oldValueSchema，意味着还没有初始化或者上一次是delete dml语句，
        // alter和create table的时候只能先使用sinkRecord.valueSchema()
        // 如果sinkRecord.valueSchema()，意味着是delete dml语句，alter和create table的时候只能先使用oldValueSchema
        boolean tableExists = dialect.tableExists(tableName);
        if (newValueSchema == null) {
            if (tableExists) {
                // 对比sinkRecord中的key与目标库中的key，如果一致，则不用修改表，继续执行delete操作，如果发生变化则直接报异常.
                List<String> newKeyNames =
                        sinkRecord.keySchema().fields().stream()
                                .map(Field::name)
                                .collect(Collectors.toList());
                List<String> oldKeyNames = dialect.getKeyNames(tableName);
                boolean equals = true;
                if (newKeyNames.size() == oldKeyNames.size()) {
                    for (String newKeyName : newKeyNames) {
                        if (!oldKeyNames.contains(newKeyName)) {
                            equals = false;
                            break;
                        }
                    }
                } else {
                    equals = false;
                }

                if (equals) {
                    return;
                } else {
                    throw new DbDmlException("表:" + tableName + "在delete的时候修改了主键字段，这是不允许的动作.");
                }
            } else {
                throw new DbDmlException("表:" + tableName + "不存在且表的第一条数据是delete.");
            }
        } else {

            if (tableExists) {

                // 对比schemaRegister中的schema和DB中的schema的差异，如果没有差异，加载即可，如果有差异，变更表结构
                boolean needChangeTableStructure =
                        dialect.needChangeTableStructure(tableName, newKeySchema, newValueSchema);
                if (needChangeTableStructure) {

                    flushAll();

                    try {
                        dialect.alterTable(tableName, newKeySchema, newValueSchema);
                    } catch (Throwable e) {
                        log.error("{} sink alterTable error:{}", getDialectName(), sinkRecord, e);
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
        }

        Pair<Schema, Schema> schemaPair = Pair.of(newKeySchema, newValueSchema);
        oldTableSchamasCache.put(tableName, schemaPair);
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

    Map<String, String> tableNameCache = new HashMap<>();

    public String getTableName(String topic) {
        String tableName = tableNameCache.get(topic);
        if (tableName == null) {
            tableName =
                    (new TopicNaming(
                            this.sinkConfig.topicReplacePrefix,
                            this.sinkConfig.tableNamePrefix,
                            sinkConfig.table_name_format))
                            .tableName(topic);
            tableNameCache.put(topic, tableName);
        }
        return tableName;
    }
}
