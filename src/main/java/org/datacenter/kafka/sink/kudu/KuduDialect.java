package org.datacenter.kafka.sink.kudu;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.apache.kudu.util.DecimalUtil;
import org.datacenter.kafka.config.kudu.KuduSinkConnectorConfig;
import org.datacenter.kafka.sink.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;

import static org.datacenter.kafka.util.SinkRecordUtil.getStructOfConfigMessageExtract;
import static org.datacenter.kafka.util.SinkRecordUtil.schema2String;

/**
 * KuduDialect
 *
 * @author sky
 */
public class KuduDialect extends AbstractDialect<KuduTable, Type> {

    private static final Logger log = LoggerFactory.getLogger(KuduDialect.class);

    private final KuduSinkConnectorConfig sinkConfig;
    private final KuduClient kuduClient;
    private KuduSession kuduSession;
    private final Map<String, KuduTable> kuduTableCache = new HashMap<>();

    public KuduDialect(KuduSinkConnectorConfig sinkConfig) {

        this.sinkConfig = sinkConfig;
        this.kuduClient = getKuduClient();
        this.kuduSession = getKuduSession();
    }

    private KuduSession getKuduSession() {

        if (kuduSession == null || kuduSession.isClosed()) {
            kuduSession = getKuduClient().newSession();
            kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        }
        return kuduSession;
    }

    private KuduClient getKuduClient() {

        if (kuduClient == null) {
            String[] kuduMasters = sinkConfig.kudu_masters.split(",");
            java.util.ArrayList<String> kuduMasterList = new ArrayList<>(kuduMasters.length);
            Collections.addAll(kuduMasterList, kuduMasters);
            return new KuduClient.KuduClientBuilder(kuduMasterList).build();
        } else {
            return kuduClient;
        }
    }

    Map<String, Boolean> tableExistsCache = new HashMap<>();

    @Override
    public boolean tableExists(String tableName) throws DbDdlException {

        Boolean tableExists = tableExistsCache.get(tableName);
        if (tableExists != null) {
            return tableExists;
        } else {
            try {
                tableExists = getKuduClient().tableExists(tableName);
                tableExistsCache.put(tableName, tableExists);
                return tableExists;
            } catch (KuduException e) {
                throw new DbDdlException("kudu tableExists Exception.", e);
            }
        }
    }

    public KuduTable getTable(String tableName) throws DbDdlException {

        KuduTable kuduTable = kuduTableCache.get(tableName);
        if (kuduTable == null) {

            kuduTable = getKuduTable(tableName);
            kuduTableCache.put(tableName, kuduTable);
        }
        return kuduTable;
    }

    /**
     * ??????schemaRegister??????schema???DB??????schema?????????<br>
     * ????????????????????????????????????????????????dialect?????????????????????????????????????????????????????????????????????????????????????????????
     *
     * @param tableName tableName
     * @param keySchema keySchama
     * @param valueSchema valueSchema
     * @return compare
     * @throws DbDdlException DbDdlException
     */
    @Override
    public boolean compare(String tableName, Schema keySchema, Schema valueSchema)
            throws DbDdlException {

        // 1?????????kafka connect??????schema???????????????kudu table schema
        HashMap<String, Type> keyColumnSchemaTypes = new HashMap<>();
        keySchema
                .fields()
                .forEach(
                        field ->
                                keyColumnSchemaTypes.put(
                                        field.name(),
                                        getDialectSchemaType(
                                                field.schema().type(), field.schema().name())));

        // 2?????????????????? kudu table schema
        KuduTable kuduTable = getTable(tableName);
        List<ColumnSchema> primaryKeyColumnSchemas = kuduTable.getSchema().getPrimaryKeyColumns();

        // 3?????????kudu?????????table Column schema?????????Map<columnName, Type> ??????????????????
        HashMap<String, Type> kuduTableKeyColumnTypes = new HashMap<>();
        primaryKeyColumnSchemas.forEach(
                columnSchema ->
                        kuduTableKeyColumnTypes.put(
                                columnSchema.getName(), columnSchema.getType()));
        // 4??????????????????kudu table schema???????????? kudu table schema
        boolean keyEquals = keyColumnSchemaTypes.equals(kuduTableKeyColumnTypes);

        // ??????????????????????????????delete record???valueSchema==null
        if (valueSchema != null) {
            // 1?????????kafka connect??????schema???????????????kudu table schema
            HashMap<String, Type> valueColumnSchemaTypes = new HashMap<>();
            valueSchema
                    .fields()
                    .forEach(
                            field ->
                                    valueColumnSchemaTypes.put(
                                            field.name(),
                                            getDialectSchemaType(
                                                    field.schema().type(), field.schema().name())));

            // 2?????????????????? kudu table schema
            List<ColumnSchema> columnSchemas = kuduTable.getSchema().getColumns();

            // 3?????????kudu?????????table Column schema?????????Map<columnName, Type> ??????????????????
            HashMap<String, Type> kuduTableColumnTypes = new HashMap<>();
            columnSchemas.forEach(
                    columnSchema ->
                            kuduTableColumnTypes.put(
                                    columnSchema.getName(), columnSchema.getType()));

            // 4??????????????????kudu table schema???????????? kudu table schema
            boolean valueEquals = valueColumnSchemaTypes.equals(kuduTableColumnTypes);

            // 5?????????????????????
            return keyEquals && valueEquals;
        } else {
            // 5?????????????????????
            return keyEquals;
        }
    }

    private KuduTable getKuduTable(String tableName) {
        KuduTable kuduTable;
        try {
            kuduTable = getKuduClient().openTable(tableName);
        } catch (KuduException e) {
            throw new DbDdlException("??????kudu?????????.", e);
        }
        return kuduTable;
    }

    @Override
    public void alterTable(String tableName, Schema keySchema, Schema valueSchema)
            throws DbDdlException {

        KuduTable kuduTable = getTable(tableName);
        AlterTableOptions alterTableOptions = new AlterTableOptions();
        Map<String, Type> changeKeyColumnSchemaTypes = new HashMap<>();
        Map<String, Type> changeValueColumnSchemaTypes = new HashMap<>();

        // 1?????????kafka connect??????schema???????????????kudu table schema
        HashMap<String, Type> keyColumnSchemaTypes = new HashMap<>();
        keySchema
                .fields()
                .forEach(
                        field ->
                                keyColumnSchemaTypes.put(
                                        field.name(),
                                        getDialectSchemaType(
                                                field.schema().type(), field.schema().name())));

        // 2?????????????????? kudu table schema
        List<ColumnSchema> primaryKeyColumnSchemas = kuduTable.getSchema().getPrimaryKeyColumns();

        // 3???????????????kudu table Column schema?????????Map<columnName, Type> ?????????????????????
        HashMap<String, Type> kuduTableKeyColumnTypes = new HashMap<>();
        primaryKeyColumnSchemas.forEach(
                columnSchema ->
                        kuduTableKeyColumnTypes.put(
                                columnSchema.getName(), columnSchema.getType()));

        // 4??????????????????kudu table schema???????????? kudu table schema
        HashSet<String> tempKeyColumnName = new HashSet<>();
        tempKeyColumnName.addAll(keyColumnSchemaTypes.keySet());
        tempKeyColumnName.addAll(kuduTableKeyColumnTypes.keySet());

        // 5????????????????????????field???Type??????sinkRecord???schema????????????Type
        // ?????????sinkRecord??????field??????????????????Type==null???alter????????????drop??????
        for (String columnName : tempKeyColumnName) {
            Type recordType = keyColumnSchemaTypes.get(columnName);
            if (recordType == null) {
                changeKeyColumnSchemaTypes.put(columnName, null);
            } else if (!recordType.equals(kuduTableKeyColumnTypes.get(columnName))) {
                changeKeyColumnSchemaTypes.put(columnName, keyColumnSchemaTypes.get(columnName));
            }
        }

        // 6?????????AlterTableOptions
        changeKeyColumnSchemaTypes.forEach(
                (columnName, type) -> {
                    if (type == null) {
                        alterTableOptions.dropColumn(columnName);
                    } else {
                        Field field = keySchema.field(columnName);
                        Map<String, String> schemaParameters = field.schema().parameters();
                        ColumnSchema columnSchema =
                                getColumnSchema(
                                        field.name(),
                                        field.schema().name(),
                                        field.schema().type(),
                                        schemaParameters,
                                        true);
                        alterTableOptions.addColumn(columnSchema);
                    }
                });

        if (valueSchema != null) {

            // 1?????????kafka connect??????schema???????????????kudu table schema
            HashMap<String, Type> valueColumnSchemaTypes = new HashMap<>();
            valueSchema
                    .fields()
                    .forEach(
                            field ->
                                    valueColumnSchemaTypes.put(
                                            field.name(),
                                            getDialectSchemaType(
                                                    field.schema().type(), field.schema().name())));

            // 2?????????????????? kudu table schema
            List<ColumnSchema> columnSchemas = kuduTable.getSchema().getColumns();

            // 3???????????????kudu table Column schema?????????Map<columnName, Type> ?????????????????????
            HashMap<String, Type> kuduTableColumnTypes = new HashMap<>();
            columnSchemas.forEach(
                    columnSchema ->
                            kuduTableColumnTypes.put(
                                    columnSchema.getName(), columnSchema.getType()));

            // 4??????????????????kudu table schema???????????? kudu table schema
            HashSet<String> tempValueColumnName = new HashSet<>();
            tempValueColumnName.addAll(valueColumnSchemaTypes.keySet());
            tempValueColumnName.addAll(kuduTableColumnTypes.keySet());

            // 5????????????????????????field???Type??????sinkRecord???schema????????????Type
            // ?????????sinkRecord??????field??????????????????Type==null???alter????????????drop??????
            for (String columnName : tempValueColumnName) {
                Type recordType = valueColumnSchemaTypes.get(columnName);
                if (recordType == null) {
                    changeValueColumnSchemaTypes.put(columnName, null);
                } else if (!recordType.equals(kuduTableColumnTypes.get(columnName))) {
                    changeValueColumnSchemaTypes.put(
                            columnName, valueColumnSchemaTypes.get(columnName));
                }
            }

            // 6?????????AlterTableOptions
            changeValueColumnSchemaTypes.forEach(
                    (columnName, type) -> {
                        if (type == null) {
                            alterTableOptions.dropColumn(columnName);
                        } else {
                            Field field = valueSchema.field(columnName);
                            Map<String, String> schemaParameters = field.schema().parameters();
                            ColumnSchema columnSchema =
                                    getColumnSchema(
                                            field.name(),
                                            field.schema().name(),
                                            field.schema().type(),
                                            schemaParameters,
                                            false);
                            alterTableOptions.addColumn(columnSchema);
                        }
                    });
        }

        // 7??????kudu??????alterTable
        try {
            getKuduClient().alterTable(tableName, alterTableOptions);
            boolean alterTableDone = getKuduClient().isAlterTableDone(tableName);
            if (alterTableDone) {
                kuduTable = getKuduTable(tableName);
                kuduTableCache.put(tableName, kuduTable);
                tableExistsCache.put(tableName, true);
                log.info("alter table done:{}", tableName);
            }
        } catch (KuduException e) {
            throw new DbDdlException(
                    "alter table exception,tableName:"
                            + tableName
                            + ";alterKeyColumn:"
                            + changeKeyColumnSchemaTypes.toString()
                            + ";alterColumn:"
                            + changeValueColumnSchemaTypes.toString()
                            + ";keySchema:"
                            + schema2String(keySchema)
                            + ";valueSchema:"
                            + schema2String(valueSchema),
                    e);
        }
    }

    @Override
    public void createTable(String tableName, Schema keySchema, Schema valueSchema)
            throws DbDdlException {

        if (tableExists(tableName)) {
            return;
        }

        // 1???build key field names.
        List<String> keyNames = new ArrayList<>();
        for (Field field : keySchema.fields()) {
            keyNames.add(field.name());
        }

        // 2???build  CreateTableOptions
        CreateTableOptions createTableOptions = new CreateTableOptions();
        createTableOptions.addHashPartitions(keyNames, sinkConfig.defaultPartitionBuckets);
        createTableOptions.setRangePartitionColumns(keyNames);

        // 3???build columnSchemas
        // ????????????????????????????????????????????????????????????????????????????????????????????????columnSchemas.add()???????????????
        // ???????????????????????????????????????????????????????????????action_time???????????????app_name???????????????????????????
        // ```
        // org.apache.kudu.client.NonRecoverableException: Got out-of-order key column: name:
        // ???action_time??? type: INT64 is_key: true is_nullable: false cfile_block_size: 0
        // ```
        List<ColumnSchema> columnSchemas = new ArrayList<>();
        if (valueSchema == null) {
            for (Field field : keySchema.fields()) {
                String fieldName = field.name();
                Map<String, String> schemaParameters = field.schema().parameters();

                ColumnSchema columnSchema =
                        getColumnSchema(
                                fieldName,
                                field.schema().name(),
                                field.schema().type(),
                                schemaParameters,
                                true);

                columnSchemas.add(columnSchema);
            }
        } else {

            for (Field field : keySchema.fields()) {
                String fieldName = field.name();
                Map<String, String> schemaParameters = field.schema().parameters();

                ColumnSchema columnSchema =
                        getColumnSchema(
                                fieldName,
                                field.schema().name(),
                                field.schema().type(),
                                schemaParameters,
                                true);

                columnSchemas.add(columnSchema);
            }

            for (Field field : valueSchema.fields()) {

                String fieldName = field.name();
                Map<String, String> schemaParameters = field.schema().parameters();
                boolean isKey = keyNames.contains(fieldName);
                if (!isKey) {
                    ColumnSchema columnSchema =
                            getColumnSchema(
                                    fieldName,
                                    field.schema().name(),
                                    field.schema().type(),
                                    schemaParameters,
                                    false);

                    columnSchemas.add(columnSchema);
                }
            }
        }

        // 4???build table Schema
        org.apache.kudu.Schema tableSchema = new org.apache.kudu.Schema(columnSchemas);

        // 5???create table
        try {
            getKuduClient().createTable(tableName, tableSchema, createTableOptions);
            boolean createTableDone = getKuduClient().isCreateTableDone(tableName);
            if (createTableDone) {

                KuduTable kuduTable = getKuduTable(tableName);
                kuduTableCache.put(tableName, kuduTable);
                tableExistsCache.put(tableName, true);
                log.info("created table:{}", tableName);
            }
        } catch (KuduException e) {
            throw new DbDdlException(
                    "create table exception,tableName:"
                            + tableName
                            + ";keySchema:"
                            + schema2String(keySchema)
                            + ";valueSchema:"
                            + schema2String(valueSchema)
                            + ";kuduColumnSchemas:"
                            + columnSchemas.toString(),
                    e);
        }
    }

    private ColumnSchema getColumnSchema(
            String fieldName,
            String fieldSchemaName,
            Schema.Type fieldSchemaType,
            Map<String, String> schemaParameters,
            boolean isKey) {

        ColumnSchema.ColumnSchemaBuilder columnSchemaBuilder = null;

        try {

            Type kuduSchemaType = getDialectSchemaType(fieldSchemaType, fieldSchemaName);
            if (kuduSchemaType != null) {
                columnSchemaBuilder =
                        new ColumnSchema.ColumnSchemaBuilder(fieldName, kuduSchemaType);
                if (isKey) {
                    columnSchemaBuilder.key(true);
                } else {
                    columnSchemaBuilder.nullable(true);
                }

                // default value ????????????????????????????????????bug?????????????????????.
                //            if (defaultValue != null) {
                //                columnSchemaBuilder.defaultValue(defaultValue);
                //            }

                // DECIMAL ???????????????typeAttributes???????????????????????????????????????
                if (kuduSchemaType.equals(Type.DECIMAL)) {

                    int precision = 20;
                    int scale = 4;

                    if (schemaParameters != null) {
                        String precisionString = schemaParameters.get("connect.decimal.precision");
                        String scaleString = schemaParameters.get("scale");

                        if (precisionString != null) {
                            try {
                                precision = Integer.parseInt(precisionString);
                            } catch (Exception e) {
                                log.error(
                                        "fieldName:{},??????decimal????????????????????????precision???????????????precisionString:{}",
                                        fieldName,
                                        precisionString);
                            }
                        }
                        if (scaleString != null) {
                            try {
                                scale = Integer.parseInt(scaleString);
                            } catch (Exception e) {
                                log.error(
                                        "fieldName:{},??????decimal????????????????????????scale???????????????scaleString:{}",
                                        fieldName,
                                        scaleString);
                            }
                        }
                    }
                    ColumnTypeAttributes columnTypeAttributes =
                            DecimalUtil.typeAttributes(precision, scale);
                    columnSchemaBuilder.typeAttributes(columnTypeAttributes);
                }
            } else {
                throw new DbDdlException("schema type not match.");
            }
        } catch (Throwable e) {
            log.error(
                    "kudu sink getColumnSchema exception.fieldName:{},fieldSchemaName:{},fieldSchemaType:{},schemaParameters:{}",
                    fieldName,
                    fieldSchemaName,
                    fieldSchemaType,
                    schemaParameters);
            throw new DbDdlException("schema type not match.", e);
        }
        return columnSchemaBuilder.build();
    }

    @Override
    public Type getDialectSchemaType(Schema.Type columnType, String columnSchemaName) {
        Type kuduSchemaType = null;

        SchemaTypeEnum columnSchemaTypeEnum =
                SinkRecordTypeTransform.getSchemaType(columnType, columnSchemaName);

        switch (columnSchemaTypeEnum) {
            case DECIMAL:
                kuduSchemaType = Type.DECIMAL;
                break;
            case DATE:
            case TIME:
            case STRING:
                kuduSchemaType = Type.STRING;
                break;
            case TIMESTAMP:
                kuduSchemaType = Type.UNIXTIME_MICROS;
                break;
            case TINYINT:
                kuduSchemaType = Type.INT8;
                break;
            case SHORT:
                kuduSchemaType = Type.INT16;
                break;
            case INT:
                kuduSchemaType = Type.INT32;
                break;
            case LONG:
                kuduSchemaType = Type.INT64;
                break;
            case BOOLEAN:
                kuduSchemaType = Type.BOOL;
                break;
            case FLOAT:
                kuduSchemaType = Type.FLOAT;
                break;
            case DOUBLE:
                kuduSchemaType = Type.DOUBLE;
                break;
            case BYTES:
                kuduSchemaType = Type.BINARY;
                break;
        }
        return kuduSchemaType;
    }

    @Override
    public boolean applyUpsertRecord(String tableName, SinkRecord sinkRecord) {

        Struct valueStruct =
                getStructOfConfigMessageExtract(
                        (Struct) sinkRecord.value(), sinkConfig.messageExtract);

        KuduTable kuduTable = getTable(tableName);
        Upsert upsert = kuduTable.newUpsert();
        PartialRow row = upsert.getRow();

        for (Field field : valueStruct.schema().fields()) {
            try {
                addRowValues(tableName, row, field, valueStruct);
            } catch (Exception e) {
                throw new DbDmlException(
                        "addRowValues error:{tableName:"
                                + tableName
                                + ",fieldName:"
                                + field.name()
                                + ",fieldSchemaName:"
                                + field.schema().name()
                                + ",fieldSchemaType:"
                                + field.schema().type()
                                + ",sinkRecord:"
                                + sinkRecord.toString()
                                + "}",
                        e);
            }
        }

        apply(upsert);

        return true;
    }

    @Override
    public boolean applyDeleteRecord(String tableName, SinkRecord sinkRecord) {

        Struct keyStruct =
                getStructOfConfigMessageExtract(
                        (Struct) sinkRecord.key(), sinkConfig.messageExtract);

        KuduTable kuduTable = getTable(tableName);
        Delete delete = kuduTable.newDelete();
        PartialRow row = delete.getRow();

        for (Field field : keyStruct.schema().fields()) {
            try {
                addRowValues(tableName, row, field, keyStruct);
            } catch (Exception e) {
                throw new DbDmlException(
                        "addRowValues error:{tableName:"
                                + tableName
                                + ",fieldName:"
                                + field.name()
                                + ",fieldSchemaName:"
                                + field.schema().name()
                                + ",fieldSchemaType:"
                                + field.schema().type()
                                + ",sinkRecord:"
                                + sinkRecord.toString()
                                + "}",
                        e);
            }
        }

        apply(delete);

        return true;
    }

    @Override
    public Pair<Boolean, Long> elasticLimit(String connectorName) {
        return Pair.of(false, 0L);
    }

    private void apply(Object operationRecord) throws DbDmlException {

        KuduSession kuduSession = getKuduSession();

        try {
            kuduSession.apply((Operation) operationRecord);
        } catch (KuduException e) {
            log.error("kudu apply exception:", e);
            throw new DbDmlException(e);
        }
    }

    @Override
    public void flush() throws DbDmlException {

        KuduSession kuduSession = getKuduSession();

        try {
            kuduSession.flush();
        } catch (KuduException e) {
            log.error("kudu flush exception:", e);
            throw new DbDmlException(e);
        }
    }

    @Override
    public void stop() throws ConnectException {

        if (kuduSession != null && !kuduSession.isClosed()) {
            try {
                kuduSession.flush();
                kuduSession.close();
            } catch (KuduException e) {
                log.error("kudu flush exception:", e);
            }
        }
        if (kuduClient != null) {
            try {
                kuduClient.close();
            } catch (KuduException ke) {
                throw new ConnectException(ke);
            }
        }
    }

    private void addRowValues(
            String tableName, final PartialRow row, final Field field, final Struct valueStruct) {

        String columnName = field.name();
        String columnSchemaName = field.schema().name();
        Schema.Type columnType = field.schema().type();

        SchemaTypeEnum columnSchemaTypeEnum =
                SinkRecordTypeTransform.getSchemaType(columnType, columnSchemaName);

        switch (columnSchemaTypeEnum) {
            case BOOLEAN:
                Boolean booleanValue =
                        SinkRecordTypeTransform.getBoolean(
                                columnName, columnSchemaName, columnType, valueStruct);
                if (booleanValue != null) {
                    row.addBoolean(columnName, booleanValue);
                }
                break;
            case TINYINT:
                Byte tinyintValue =
                        SinkRecordTypeTransform.getTinyint(
                                columnName, columnSchemaName, columnType, valueStruct);
                if (tinyintValue != null) {
                    row.addByte(columnName, tinyintValue);
                }
                break;
            case SHORT:
                Short shortValue =
                        SinkRecordTypeTransform.getShort(
                                columnName, columnSchemaName, columnType, valueStruct);
                if (shortValue != null) {
                    row.addShort(columnName, shortValue);
                }
                break;
            case INT:
                Integer intValue =
                        SinkRecordTypeTransform.getInt(
                                columnName, columnSchemaName, columnType, valueStruct);
                if (intValue != null) {
                    row.addInt(columnName, intValue);
                }
                break;
            case LONG:
                Long longValue =
                        SinkRecordTypeTransform.getLong(
                                columnName, columnSchemaName, columnType, valueStruct);
                if (longValue != null) {
                    row.addLong(columnName, longValue);
                }
                break;
            case FLOAT:
                Float floatValue =
                        SinkRecordTypeTransform.getFloat(
                                columnName, columnSchemaName, columnType, valueStruct);
                if (floatValue != null) {
                    row.addFloat(columnName, floatValue);
                }
                break;
            case DOUBLE:
                Double doubleValue =
                        SinkRecordTypeTransform.getDouble(
                                columnName, columnSchemaName, columnType, valueStruct);
                if (doubleValue != null) {
                    row.addDouble(columnName, doubleValue);
                }
                break;
            case STRING:
                String stringValue =
                        SinkRecordTypeTransform.getString(
                                columnName, columnSchemaName, columnType, valueStruct);
                if (stringValue != null) {
                    row.addString(columnName, stringValue);
                }
                break;
            case BYTES:
                byte[] bytesValue =
                        SinkRecordTypeTransform.getBytes(
                                columnName, columnSchemaName, columnType, valueStruct);
                if (bytesValue != null) {
                    row.addBinary(columnName, bytesValue);
                }
                break;
            case TIME:
                Time timeValue =
                        SinkRecordTypeTransform.getTime(
                                columnName, columnSchemaName, columnType, valueStruct);
                if (timeValue != null) {
                    row.addString(columnName, timeValue.toString());
                }
                break;
            case DATE:
                Date dateValue =
                        SinkRecordTypeTransform.getDate(
                                columnName, columnSchemaName, columnType, valueStruct);
                if (dateValue != null) {
                    row.addString(columnName, dateValue.toString());
                }
                break;
            case TIMESTAMP:
                Timestamp timestampValue =
                        SinkRecordTypeTransform.getTimestamp(
                                columnName, columnSchemaName, columnType, valueStruct);
                if (timestampValue != null) {
                    row.addTimestamp(columnName, timestampValue);
                }
                break;
            case DECIMAL:
                BigDecimal decimalValue =
                        SinkRecordTypeTransform.getDecimal(
                                columnName, columnSchemaName, columnType, valueStruct);
                if (decimalValue != null) {
                    row.addDecimal(columnName, decimalValue);
                }
                break;
            default:
                throw new DbDmlException(
                        "not match column type.tableName:"
                                + tableName
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
}
