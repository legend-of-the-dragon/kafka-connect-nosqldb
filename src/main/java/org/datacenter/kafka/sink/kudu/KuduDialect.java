package org.datacenter.kafka.sink.kudu;

import com.alibaba.fastjson.JSON;
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
     * 对比schemaRegister中的schema和DB中的schema的差异<br>
     * 对比的时候需要把字段的类型转换成dialect的类型来对比，不能用公共类型对比，由方言来处理类型的转换问题。
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

        // 1、通过kafka connect中的schema构建预期的kudu table schema
        HashMap<String, Type> keyColumnSchemaTypes = new HashMap<>();
        keySchema
                .fields()
                .forEach(
                        field ->
                                keyColumnSchemaTypes.put(
                                        field.name(),
                                        getDialectSchemaType(
                                                field.schema().type(), field.schema().name())));

        // 2、获取真实的 kudu table schema
        KuduTable kuduTable = getTable(tableName);
        List<ColumnSchema> primaryKeyColumnSchemas = kuduTable.getSchema().getPrimaryKeyColumns();

        // 3、把从kudu读取的table Column schema组装成Map<columnName, Type> 形式方便对比
        HashMap<String, Type> kuduTableKeyColumnTypes = new HashMap<>();
        primaryKeyColumnSchemas.forEach(
                columnSchema ->
                        kuduTableKeyColumnTypes.put(
                                columnSchema.getName(), columnSchema.getType()));
        // 4、对比预期的kudu table schema和真实的 kudu table schema
        boolean keyEquals = keyColumnSchemaTypes.equals(kuduTableKeyColumnTypes);

        // 如果启动的时候刚好是delete record，valueSchema==null
        if (valueSchema != null) {
            // 1、通过kafka connect中的schema构建预期的kudu table schema
            HashMap<String, Type> valueColumnSchemaTypes = new HashMap<>();
            valueSchema
                    .fields()
                    .forEach(
                            field ->
                                    valueColumnSchemaTypes.put(
                                            field.name(),
                                            getDialectSchemaType(
                                                    field.schema().type(), field.schema().name())));

            // 2、获取真实的 kudu table schema
            List<ColumnSchema> columnSchemas = kuduTable.getSchema().getColumns();

            // 3、把从kudu读取的table Column schema组装成Map<columnName, Type> 形式方便对比
            HashMap<String, Type> kuduTableColumnTypes = new HashMap<>();
            columnSchemas.forEach(
                    columnSchema ->
                            kuduTableColumnTypes.put(
                                    columnSchema.getName(), columnSchema.getType()));

            // 4、对比预期的kudu table schema和真实的 kudu table schema
            boolean valueEquals = valueColumnSchemaTypes.equals(kuduTableColumnTypes);

            // 5、返回对比结果
            return keyEquals && valueEquals;
        } else {
            // 5、返回对比结果
            return keyEquals;
        }
    }

    private KuduTable getKuduTable(String tableName) {
        KuduTable kuduTable;
        try {
            kuduTable = getKuduClient().openTable(tableName);
        } catch (KuduException e) {
            throw new DbDdlException("获取kudu表异常.", e);
        }
        return kuduTable;
    }

    @Override
    public void alterTable(String tableName, Schema keySchema, Schema valueSchema)
            throws DbDdlException {

        // 通过kafka connect中的schema构建预期的kudu table schema
        HashMap<String, Type> keyColumnSchemaTypes = new HashMap<>();
        HashMap<String, Type> valueColumnSchemaTypes = new HashMap<>();
        keySchema
                .fields()
                .forEach(
                        field ->
                                keyColumnSchemaTypes.put(
                                        field.name(),
                                        getDialectSchemaType(
                                                field.schema().type(), field.schema().name())));
        valueSchema
                .fields()
                .forEach(
                        field ->
                                valueColumnSchemaTypes.put(
                                        field.name(),
                                        getDialectSchemaType(
                                                field.schema().type(), field.schema().name())));

        // 获取真实的 kudu table schema
        KuduTable kuduTable = getTable(tableName);
        List<ColumnSchema> primaryKeyColumnSchemas = kuduTable.getSchema().getPrimaryKeyColumns();
        List<ColumnSchema> columnSchemas = kuduTable.getSchema().getColumns();

        // 把真实的kudu table Column schema组装成Map<columnName, Type> 形式，方便对比
        HashMap<String, Type> kuduTableKeyColumnTypes = new HashMap<>();
        primaryKeyColumnSchemas.forEach(
                columnSchema ->
                        kuduTableKeyColumnTypes.put(
                                columnSchema.getName(), columnSchema.getType()));

        HashMap<String, Type> kuduTableColumnTypes = new HashMap<>();
        columnSchemas.forEach(
                columnSchema ->
                        kuduTableColumnTypes.put(columnSchema.getName(), columnSchema.getType()));

        // 对比预期的kudu table schema和真实的 kudu table schema
        HashSet<String> tempKeyColumnName = new HashSet<>();
        tempKeyColumnName.addAll(keyColumnSchemaTypes.keySet());
        tempKeyColumnName.addAll(kuduTableKeyColumnTypes.keySet());

        HashSet<String> tempValueColumnName = new HashSet<>();
        tempValueColumnName.addAll(valueColumnSchemaTypes.keySet());
        tempValueColumnName.addAll(kuduTableColumnTypes.keySet());

        // 找出发生变更的field，Type使用sinkRecord的schema转换来的Type
        // 如果是sinkRecord中的field被删除了，则Type==null，alter的时候做drop处理
        Map<String, Type> changeKeyColumnSchemaTypes = new HashMap<>();
        Map<String, Type> changeValueColumnSchemaTypes = new HashMap<>();
        for (String columnName : tempKeyColumnName) {
            Type recordType = keyColumnSchemaTypes.get(columnName);
            if (recordType == null) {
                changeKeyColumnSchemaTypes.put(columnName, null);
            } else if (!recordType.equals(kuduTableKeyColumnTypes.get(columnName))) {
                changeKeyColumnSchemaTypes.put(columnName, keyColumnSchemaTypes.get(columnName));
            }
        }

        for (String columnName : tempValueColumnName) {
            Type recordType = valueColumnSchemaTypes.get(columnName);
            if (recordType == null) {
                changeValueColumnSchemaTypes.put(columnName, null);
            } else if (!recordType.equals(kuduTableColumnTypes.get(columnName))) {
                changeValueColumnSchemaTypes.put(
                        columnName, valueColumnSchemaTypes.get(columnName));
            }
        }

        // 构建变更实体
        AlterTableOptions alterTableOptions = new AlterTableOptions();

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

        // 向kudu发起alterTable
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
                            + JSON.toJSONString(changeKeyColumnSchemaTypes)
                            + ";alterColumn:"
                            + JSON.toJSONString(changeValueColumnSchemaTypes)
                            + ";keySchema:"
                            + JSON.toJSONString(keySchema)
                            + ";valueSchema:"
                            + JSON.toJSONString(valueSchema),
                    e);
        }
    }

    @Override
    public void createTable(String tableName, Schema keySchema, Schema valueSchema)
            throws DbDdlException {

        if (tableExists(tableName)) {
            return;
        }

        // 1、build key field names.
        List<String> keyNames = new ArrayList<>();
        for (Field field : keySchema.fields()) {
            keyNames.add(field.name());
        }

        // 2、build  CreateTableOptions
        CreateTableOptions createTableOptions = new CreateTableOptions();
        createTableOptions.addHashPartitions(keyNames, sinkConfig.defaultPartitionBuckets);
        createTableOptions.setRangePartitionColumns(keyNames);

        // 3、build columnSchemas
        List<ColumnSchema> columnSchemas = new ArrayList<>();
        for (Field field : valueSchema.fields()) {

            String fieldName = field.name();
            Map<String, String> schemaParameters = field.schema().parameters();
            boolean isKey = keyNames.contains(fieldName);
            try {
                ColumnSchema columnSchema =
                        getColumnSchema(
                                fieldName,
                                field.schema().name(),
                                field.schema().type(),
                                schemaParameters,
                                isKey);

                columnSchemas.add(columnSchema);
            } catch (Exception e) {
                throw new DbDdlException(
                        "alter table exception,tableName:"
                                + tableName
                                + ";fieldName:"
                                + JSON.toJSONString(fieldName)
                                + ";fieldSchemaName:"
                                + JSON.toJSONString(field.schema().name())
                                + ";fieldSchemaType:"
                                + JSON.toJSONString(field.schema().type())
                                + ";keySchema:"
                                + JSON.toJSONString(keySchema)
                                + ";valueSchema:"
                                + JSON.toJSONString(valueSchema));
            }
        }

        // 4、build table Schema
        org.apache.kudu.Schema tableSchema = new org.apache.kudu.Schema(columnSchemas);

        // 5、create table
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
                    "alter table exception,tableName:"
                            + tableName
                            + ";keySchema:"
                            + JSON.toJSONString(keySchema)
                            + ";valueSchema:"
                            + JSON.toJSONString(valueSchema),
                    e);
        }
    }

    private ColumnSchema getColumnSchema(
            String fieldName,
            String fieldSchemaName,
            Schema.Type fieldSchemaType,
            Map<String, String> schemaParameters,
            boolean isKey) {

        ColumnSchema.ColumnSchemaBuilder columnSchemaBuilder;

        Type kuduSchemaType = getDialectSchemaType(fieldSchemaType, fieldSchemaName);
        if (kuduSchemaType != null) {
            columnSchemaBuilder = new ColumnSchema.ColumnSchemaBuilder(fieldName, kuduSchemaType);
            if (isKey) {
                columnSchemaBuilder.key(true);
            } else {
                columnSchemaBuilder.nullable(true);
            }

            // default value 由于默认值类型匹配导致的bug过多，暂时放弃.
            //            if (defaultValue != null) {
            //                columnSchemaBuilder.defaultValue(defaultValue);
            //            }

            // DECIMAL 必须要通过typeAttributes指定位数，不然会空指针报错
            if (kuduSchemaType.equals(Type.DECIMAL)) {
                String precisionString = schemaParameters.get("connect.decimal.precision");
                String scaleString = schemaParameters.get("scale");
                int precision = 20;
                int scale = 4;
                if (precisionString != null) {
                    try {
                        precision = Integer.parseInt(precisionString);
                    } catch (Exception e) {
                        log.error(
                                "fieldName:{},处理decimal字段类型的时候，precision获取错误。precisionString:{}",
                                fieldName,
                                precisionString);
                    }
                }
                if (scaleString != null) {
                    try {
                        scale = Integer.parseInt(scaleString);
                    } catch (Exception e) {
                        log.error(
                                "fieldName:{},处理decimal字段类型的时候，scale获取错误。scaleString:{}",
                                fieldName,
                                scaleString);
                    }
                }
                ColumnTypeAttributes columnTypeAttributes =
                        DecimalUtil.typeAttributes(precision, scale);
                columnSchemaBuilder.typeAttributes(columnTypeAttributes);
            }
        } else {
            throw new DbDdlException("schema type not match.");
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
                String sinkRecordJson = JSON.toJSONString(valueStruct);
                throw new DbDmlException(
                        "addRowValues error:{tableName:"
                                + tableName
                                + ",fieldName:"
                                + field.name()
                                + ",fieldSchemaName:"
                                + field.schema().name()
                                + ",fieldSchemaType:"
                                + field.schema().type()
                                + ",sinkRecordJson:"
                                + sinkRecordJson
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
                                + ",sinkRecordJson:"
                                + JSON.toJSONString(keyStruct)
                                + "}",
                        e);
            }
        }

        apply(delete);

        return true;
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
                                + ",valueStructJson:"
                                + JSON.toJSONString(valueStruct));
        }
    }
}
