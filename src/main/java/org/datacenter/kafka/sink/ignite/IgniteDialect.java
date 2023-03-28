package org.datacenter.kafka.sink.ignite;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteState;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.stream.StreamReceiver;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.datacenter.kafka.sink.*;
import org.datacenter.kafka.sink.exception.DbDdlException;
import org.datacenter.kafka.sink.exception.DbDmlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.datacenter.kafka.util.SinkRecordUtil.getStructOfConfigMessageExtract;

/**
 * @author sky
 * @date 2022-05-25
 * @discription
 */
public class IgniteDialect
        extends AbstractDialect<IgniteDataStreamer<Object, Object>, SchemaTypeEnum> {

    private static final Logger log = LoggerFactory.getLogger(AbstractSinkTask.class);

    private final IgniteSinkConnectorConfig sinkConfig;
    private final Map<String, IgniteDataStreamer<Object, Object>> dataStreamers = new HashMap<>();

    public final String KEY_SUFFIX = ".Key";

    public final String VALUE_SUFFIX = ".Value";

    public IgniteDialect(IgniteSinkConnectorConfig sinkConfig) {
        this.sinkConfig = sinkConfig;
        DataGrid.SINK.init(this.sinkConfig.igniteCfg());
    }

    @Override
    public IgniteDataStreamer<Object, Object> getTable(String tableName) throws DbDdlException {
        IgniteDataStreamer<Object, Object> igniteDataStreamer = this.dataStreamers.get(tableName);
        if (igniteDataStreamer == null) {
            igniteDataStreamer = getDataStreamer(tableName);
            this.dataStreamers.put(tableName, igniteDataStreamer);
        }
        return igniteDataStreamer;
    }

    private IgniteDataStreamer<Object, Object> getDataStreamer(String cacheName) {

        IgniteState state = Ignition.state(DataGrid.SINK.getIgniteName());
        log.info("ignite sink state:{}", state);
        if (state != IgniteState.STARTED) {
            DataGrid.SINK.init(this.sinkConfig.igniteCfg());
        }

        DataGrid.SINK.ensureCache(cacheName);
        IgniteDataStreamer<Object, Object> igniteDataStreamer =
                DataGrid.SINK.dataStreamer(cacheName);

        igniteDataStreamer.allowOverwrite(true);
        igniteDataStreamer.keepBinary(true);

        if (this.sinkConfig.shallProcessUpdates) {
            igniteDataStreamer.perNodeParallelOperations(sinkConfig.parallelOps);
        }

        // 定义服务器端数据流处理逻辑.(这里由于需要支持update，默认ignite
        // DataStreamer是不支持update的，通过服务器端再处理的方式来对数据重新处理达到update的目的.)
        if (sinkConfig.allowRecordFieldsLessThanTableFields) {

            igniteDataStreamer.receiver(
                    (StreamReceiver<Object, Object>)
                            (cache, entries) ->
                                    entries.forEach(
                                            entry -> {
                                                // get ignite value
                                                Object key = entry.getKey();
                                                BinaryObjectImpl kafkaValue =
                                                        (BinaryObjectImpl) entry.getValue();
                                                if (kafkaValue == null) {
                                                    cache.remove(key);
                                                } else {
                                                    BinaryObjectImpl igniteValue =
                                                            (BinaryObjectImpl) cache.get(key);

                                                    if (igniteValue != null) {

                                                        // 用kafka中的value替换ignite中的value
                                                        Collection<String> igniteValueFieldNames =
                                                                igniteValue.type().fieldNames();

                                                        BinaryObjectBuilder
                                                                igniteValueBinaryObjectBuilder =
                                                                        igniteValue.toBuilder();
                                                        for (String fieldName :
                                                                igniteValueFieldNames) {
                                                            boolean hasField =
                                                                    kafkaValue.hasField(fieldName);
                                                            if (hasField) {
                                                                Object fieldValue =
                                                                        kafkaValue.field(fieldName);
                                                                igniteValueBinaryObjectBuilder
                                                                        .setField(
                                                                                fieldName,
                                                                                fieldValue);
                                                                //
                                                                //              System.out.println(
                                                                //
                                                                //
                                                                // "StreamReceiver:replace field:"
                                                                //
                                                                //                              +
                                                                // fieldName);
                                                            }
                                                        }

                                                        // 回写结果.
                                                        cache.put(
                                                                key,
                                                                igniteValueBinaryObjectBuilder
                                                                        .build());
                                                    } else {
                                                        cache.put(key, kafkaValue);
                                                    }
                                                }
                                            }));
        }
        return igniteDataStreamer;
    }

    @Override
    public boolean tableExists(String tableName) throws DbDdlException {

        // select cache_name from sys.tables where table_name='';
        return true;
    }

    @Override
    public boolean needChangeTableStructure(String tableName, Schema keySchema, Schema valueSchema)
            throws DbDdlException {

        // select column_name from sys.table_columns where table_name='';

        // select columns from sys.indexs where table_name='' and index_name='_key_PK'

        return false;
    }

    @Override
    public void alterTable(String tableName, Schema keySchema, Schema valueSchema)
            throws DbDdlException {}

    @Override
    public void createTable(String tableName, Schema keySchema, Schema valueSchema)
            throws DbDdlException {}

    @Override
    public boolean applyUpsertRecord(String tableName, SinkRecord sinkRecord) {

        Struct keyStruct =
                getStructOfConfigMessageExtract(
                        (Struct) sinkRecord.key(), sinkConfig.messageExtract);

        Struct valueStruct =
                getStructOfConfigMessageExtract(
                        (Struct) sinkRecord.value(), sinkConfig.messageExtract);

        IgniteDataStreamer<Object, Object> dataStreamer = getTable(tableName);

        BinaryObject keyBinaryObject = createIgniteBinaryObject(keyStruct, tableName + KEY_SUFFIX);

        BinaryObject valueBinaryObject =
                createIgniteBinaryObject(valueStruct, tableName + VALUE_SUFFIX);

        dataStreamer.addData(keyBinaryObject, valueBinaryObject);

        return true;
    }

    @Override
    public boolean applyDeleteRecord(String tableName, SinkRecord sinkRecord) {

        IgniteDataStreamer<Object, Object> dataStreamer = getTable(tableName);

        Struct keyStruct =
                getStructOfConfigMessageExtract(
                        (Struct) sinkRecord.key(), sinkConfig.messageExtract);

        BinaryObject keyBinaryObject = createIgniteBinaryObject(keyStruct, tableName + KEY_SUFFIX);

        dataStreamer.removeData(keyBinaryObject);

        return true;
    }

    @Override
    public Pair<Boolean, Long> elasticLimit(String connectorName) {
        return ElasticLimit.getElasticLimit(connectorName);
        //        return Pair.of(false, 0L);
    }

    private BinaryObject createIgniteBinaryObject(Struct struct, String typeName) {

        Schema schema = struct.schema();
        IgniteBinary binary = DataGrid.SINK.binary();
        BinaryObjectBuilder binaryObjectBuilder = binary.builder(typeName);
        schema.fields()
                .forEach(
                        (field) -> {
                            Object fieldValue = this.getFieldValue(typeName, field, struct);
                            binaryObjectBuilder.setField(field.name(), fieldValue);
                        });

        try {
            return binaryObjectBuilder.build();
        } catch (BinaryObjectException e) {
            throw new ConnectException("ignite sink createIgniteBinaryObject exception", e);
        }
    }

    private Object getFieldValue(
            final String typeName, final Field field, final Struct valueStruct) {

        String columnName = field.name();
        String columnSchemaName = field.schema().name();
        Schema.Type columnType = field.schema().type();

        SchemaTypeEnum columnSchemaTypeEnum =
                SinkRecordTypeTransform.getSchemaType(columnType, columnSchemaName);

        switch (columnSchemaTypeEnum) {
            case BOOLEAN:
                return SinkRecordTypeTransform.getBoolean(
                        columnName, columnSchemaName, columnType, valueStruct);
            case TINYINT:
                return SinkRecordTypeTransform.getTinyint(
                        columnName, columnSchemaName, columnType, valueStruct);
            case SHORT:
                return SinkRecordTypeTransform.getShort(
                        columnName, columnSchemaName, columnType, valueStruct);
            case INT:
                return SinkRecordTypeTransform.getInt(
                        columnName, columnSchemaName, columnType, valueStruct);
            case LONG:
                return SinkRecordTypeTransform.getLong(
                        columnName, columnSchemaName, columnType, valueStruct);
            case FLOAT:
                return SinkRecordTypeTransform.getFloat(
                        columnName, columnSchemaName, columnType, valueStruct);
            case DOUBLE:
                return SinkRecordTypeTransform.getDouble(
                        columnName, columnSchemaName, columnType, valueStruct);
            case STRING:
                return SinkRecordTypeTransform.getString(
                        columnName, columnSchemaName, columnType, valueStruct);
            case BYTES:
                return SinkRecordTypeTransform.getBytes(
                        columnName, columnSchemaName, columnType, valueStruct);
            case TIME:
                return SinkRecordTypeTransform.getTime(
                        columnName, columnSchemaName, columnType, valueStruct);
            case DATE:
                return SinkRecordTypeTransform.getDate(
                        columnName, columnSchemaName, columnType, valueStruct);
            case TIMESTAMP:
                return SinkRecordTypeTransform.getTimestamp(
                        columnName, columnSchemaName, columnType, valueStruct);
            case DECIMAL:
                return SinkRecordTypeTransform.getDecimal(
                        columnName, columnSchemaName, columnType, valueStruct);
            default:
                throw new DbDmlException(
                        "not match column type.tableName:"
                                + typeName
                                + ",columnName:"
                                + columnName
                                + ",columnSchemaName:"
                                + columnSchemaName
                                + ",columnType"
                                + columnType
                                + ",valueStruct:"
                                + valueStruct.toString());
        }
    }

    @Override
    public void stop() throws ConnectException {

        log.info("ignite Sink task ready to stop.");
        for (IgniteDataStreamer<Object, Object> igniteDataStreamer : this.dataStreamers.values()) {
            try {
                igniteDataStreamer.flush();
                igniteDataStreamer.close();
            } catch (Throwable e) {
                log.error("igniteStreamer stop exception.", e);
            }
        }
        this.dataStreamers.clear();
        log.info("ignite Sink task stopped.");
    }

    @Override
    public void flush() throws DbDmlException {

        try {
            for (IgniteDataStreamer<Object, Object> igniteDataStreamer : dataStreamers.values()) {
                igniteDataStreamer.flush();
            }
        } catch (Exception e) {
            log.error("ignite sink flush exception.", e);
            throw new ConnectException("ignite sink flush exception.", e);
        }
    }

    @Override
    public SchemaTypeEnum getDialectSchemaType(Schema.Type columnType, String columnSchemaName) {

        return SinkRecordTypeTransform.getSchemaType(columnType, columnSchemaName);
    }
}
