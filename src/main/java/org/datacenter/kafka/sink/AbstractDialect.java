package org.datacenter.kafka.sink;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kudu.Type;

/**
 * @author sky
 * @date 2022-05-24
 * @discription
 */
public abstract class AbstractDialect <T>{

    public abstract boolean tableExists(String tableName) throws DbDdlException;

    public abstract boolean compare(String tableName, Schema keySchema, Schema valueSchema)
            throws DbDdlException;

    public abstract void alterTable(String tableName, Schema keySchema, Schema valueSchema)
            throws DbDdlException;

    public abstract void createTable(String tableName, Schema keySchema, Schema valueSchema)
            throws DbDdlException;

    public abstract boolean applyUpsertRecord(String tableName, SinkRecord sinkRecord);

    public abstract boolean applyDeleteRecord(String tableName, SinkRecord sinkRecord);

    public abstract void flush() throws DbDmlException;

    public abstract void stop() throws ConnectException;

    public abstract T getDialectSchemaType(Schema.Type columnType, String columnSchemaName);
}
