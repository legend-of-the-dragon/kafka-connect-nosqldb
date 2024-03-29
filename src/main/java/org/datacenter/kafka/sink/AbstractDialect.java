package org.datacenter.kafka.sink;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.datacenter.kafka.sink.exception.DbDdlException;
import org.datacenter.kafka.sink.exception.DbDmlException;

import java.util.List;

/**
 * @author sky
 * @date 2022-05-24
 * @discription
 */
public abstract class AbstractDialect<SinkTable, SinkDataType> {

    public abstract boolean tableExists(String tableName) throws DbDdlException;

    public abstract SinkTable getTable(String tableName) throws DbDdlException;

    public abstract List<String> getKeyNames(String tableName);

    public abstract boolean needChangeTableStructure(String tableName, Schema keySchema, Schema valueSchema)
            throws DbDdlException;

    public abstract void alterTable(String tableName, Schema keySchema, Schema valueSchema)
            throws DbDdlException;

    public abstract void createTable(String tableName, Schema keySchema, Schema valueSchema)
            throws DbDdlException;

    public abstract boolean applyUpsertRecord(String tableName, SinkRecord sinkRecord);

    public abstract boolean applyDeleteRecord(String tableName, SinkRecord sinkRecord);

    public abstract Pair<Boolean, Long> elasticLimit(String connectorName);

    public abstract void flush() throws DbDmlException;

    public abstract void stop() throws ConnectException;

    public abstract SinkDataType getDialectSchemaType(Schema.Type columnType, String columnSchemaName);
}
