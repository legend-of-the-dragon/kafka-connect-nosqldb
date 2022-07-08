package org.datacenter.kafka.sink;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * @author sky
 * @date 2022-05-24
 * @discription
 */
public abstract class AbstractDialect<Table, Type> {

    public abstract boolean tableExists(String tableName) throws DbDdlException;

    public abstract Table getTable(String tableName) throws DbDdlException;

    public abstract boolean compare(String tableName, Schema keySchema, Schema valueSchema)
            throws DbDdlException;

    public abstract void alterTable(String tableName, Schema keySchema, Schema valueSchema)
            throws DbDdlException;

    public abstract void createTable(String tableName, Schema keySchema, Schema valueSchema)
            throws DbDdlException;

    public abstract boolean applyUpsertRecord(String tableName, SinkRecord sinkRecord);

    public abstract boolean applyDeleteRecord(String tableName, SinkRecord sinkRecord);

    public abstract Pair<Boolean,Long> elasticLimit(String connectorName);

    public abstract void flush() throws DbDmlException;

    public abstract void stop() throws ConnectException;

    public abstract Type getDialectSchemaType(Schema.Type columnType, String columnSchemaName);
}
