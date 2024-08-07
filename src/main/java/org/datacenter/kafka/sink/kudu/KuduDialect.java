package org.datacenter.kafka.sink.kudu;

import static org.datacenter.kafka.util.SinkRecordUtil.getStructOfConfigMessageExtract;
import static org.datacenter.kafka.util.SinkRecordUtil.schema2String;

import org.apache.commons.lang3.tuple.MutablePair;
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
import org.datacenter.kafka.sink.AbstractDialect;
import org.datacenter.kafka.sink.SchemaTypeEnum;
import org.datacenter.kafka.sink.SinkRecordTypeTransform;
import org.datacenter.kafka.sink.exception.DbDdlException;
import org.datacenter.kafka.sink.exception.DbDmlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

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
    Map<String, MutablePair<HashMap<String, Type>, HashMap<String, Type>>> columnSchemaTypesCache =
            new HashMap<>();

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

        tableName = tableName.toLowerCase(Locale.ROOT);
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

        tableName = tableName.toLowerCase(Locale.ROOT);
        KuduTable kuduTable = kuduTableCache.get(tableName);
        if (kuduTable == null) {

            kuduTable = getKuduTable(tableName);
            kuduTableCache.put(tableName, kuduTable);
        }
        return kuduTable;
    }

    private KuduTable getKuduTable(String tableName) {

        tableName = tableName.toLowerCase(Locale.ROOT);
        KuduTable kuduTable;
        try {
            kuduTable = getKuduClient().openTable(tableName);
        } catch (KuduException e) {
            throw new DbDdlException("获取kudu表异常.", e);
        }
        return kuduTable;
    }

    @Override
    public List<String> getKeyNames(String tableName) {

        tableName = tableName.toLowerCase(Locale.ROOT);
        KuduTable table = getTable(tableName);

        return table.getSchema().getPrimaryKeyColumns().stream()
                .map(ColumnSchema::getName)
                .collect(Collectors.toList());
    }

    @Override
    public boolean needChangeTableStructure(String tableName, Schema keySchema, Schema valueSchema)
            throws DbDdlException {

        tableName = tableName.toLowerCase(Locale.ROOT);
        boolean hasNewFields = false;
        boolean allowRecordFieldLessThanTableField =
                sinkConfig.allowRecordFieldsLessThanTableFields;

        KuduTable kuduTable = getTable(tableName);

        // 计算key的Schema是否发生变更
        // 1、把record的所有字段的数据类型转换成kudu的type
        HashMap<String, Type> schemaKeyColumnTypes = new HashMap<>();
        List<Field> keyFields = keySchema.fields();
        keyFields.forEach(
                field ->
                        schemaKeyColumnTypes.put(
                                field.name(),
                                getDialectSchemaType(
                                        field.schema().type(), field.schema().name())));
        // 2、获取对应的kuduTable的表结构
        List<ColumnSchema> primaryKeyColumnSchemas = kuduTable.getSchema().getPrimaryKeyColumns();

        // 3、把从kudu读取的table Column schema组装成Map<columnName, Type> 形式方便对比
        HashMap<String, Type> kuduTableKeyColumnTypes = new HashMap<>();
        primaryKeyColumnSchemas.forEach(
                columnSchema ->
                        kuduTableKeyColumnTypes.put(
                                columnSchema.getName(), columnSchema.getType()));
        // 4、把record的字段和table中的字段进行混合
        HashMap<String, Type> keyColumnTypes = new HashMap<>();
        keyColumnTypes.putAll(schemaKeyColumnTypes);
        keyColumnTypes.putAll(kuduTableKeyColumnTypes);

        // 5、找出record中新增和缺少的字段
        HashMap<String, Type> newKeyColumns = new HashMap<>();
        HashMap<String, Type> dropKeyColumns = new HashMap<>();
        for (String fieldName : keyColumnTypes.keySet()) {
            Type type = schemaKeyColumnTypes.get(fieldName);
            Type type2 = kuduTableKeyColumnTypes.get(fieldName);
            if (type == null) {
                // record比table少的字段
                dropKeyColumns.put(fieldName, type);
            } else if (type2 == null) {
                // record比table多的字段
                newKeyColumns.put(fieldName, type);
            } else {
                // 两个都有，但是数据类型不一致.
                if (!type.equals(type2)) {
                    throw new DbDdlException("record和table中" + fieldName + "字段的数据类型不一致.");
                }
            }
        }

        if (newKeyColumns.size() > 0 || dropKeyColumns.size() > 0) {
            if (dropKeyColumns.size() > 0) {
                if (!allowRecordFieldLessThanTableField) {
                    hasNewFields = true;
                }
            }

            if (newKeyColumns.size() > 0) {
                hasNewFields = true;
            }
        }

        // 计算value的Schema是否发生变更
        if (valueSchema != null) {
            // 1、把record的所有字段的数据类型转换成kudu的type
            HashMap<String, Type> schemaValueColumnTypes = new HashMap<>();
            List<Field> valueFields = valueSchema.fields();
            valueFields.forEach(
                    field ->
                            schemaValueColumnTypes.put(
                                    field.name(),
                                    getDialectSchemaType(
                                            field.schema().type(), field.schema().name())));
            // 2、获取对应的kuduTable的表结构
            List<ColumnSchema> columnSchemas = kuduTable.getSchema().getColumns();

            // 3、把从kudu读取的table Column schema组装成Map<columnName, Type> 形式方便对比
            HashMap<String, Type> kuduTableValueColumnTypes = new HashMap<>();
            columnSchemas.forEach(
                    columnSchema ->
                            kuduTableValueColumnTypes.put(
                                    columnSchema.getName(), columnSchema.getType()));
            // 4、把record的字段和table中的字段进行混合
            HashMap<String, Type> valueColumnTypes = new HashMap<>();
            valueColumnTypes.putAll(schemaValueColumnTypes);
            valueColumnTypes.putAll(kuduTableValueColumnTypes);

            // 5、找出record中新增和缺少的字段
            HashMap<String, Type> newValueColumns = new HashMap<>();
            HashMap<String, Type> dropValueColumns = new HashMap<>();
            for (String fieldName : valueColumnTypes.keySet()) {
                Type type = schemaValueColumnTypes.get(fieldName);
                Type type2 = kuduTableValueColumnTypes.get(fieldName);
                if (type == null) {
                    // record比table少的字段
                    dropValueColumns.put(fieldName, type);
                } else if (type2 == null) {
                    // record比table多的字段
                    newValueColumns.put(fieldName, type);
                } else {
                    // 两个都有，但是数据类型不一致.
                    if (!type.equals(type2)) {
                        throw new DbDdlException("record和table中" + fieldName + "字段的数据类型不一致.");
                    }
                }
            }

            int newValueColumnsSize = newValueColumns.size();
            int dropValueColumnsSize = dropValueColumns.size();
            if (newValueColumnsSize > 0 || dropValueColumnsSize > 0) {
                if (dropValueColumnsSize > 0) {
                    if (!allowRecordFieldLessThanTableField) {
                        hasNewFields = true;
                    }
                }

                if (newValueColumnsSize > 0) {
                    hasNewFields = true;
                }
            }
        }
        return hasNewFields;
    }

    @Override
    public void alterTable(String tableName, Schema keySchema, Schema valueSchema)
            throws DbDdlException {

        tableName = tableName.toLowerCase(Locale.ROOT);
        KuduTable kuduTable = getTable(tableName);
        AlterTableOptions alterTableOptions = new AlterTableOptions();
        Map<String, Type> changeKeyColumnSchemaTypes = new HashMap<>();
        Map<String, Type> changeValueColumnSchemaTypes = new HashMap<>();

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
        List<ColumnSchema> primaryKeyColumnSchemas = kuduTable.getSchema().getPrimaryKeyColumns();

        // 3、把真实的kudu table Column schema组装成Map<columnName, Type> 形式，方便对比
        HashMap<String, Type> kuduTableKeyColumnTypes = new HashMap<>();
        primaryKeyColumnSchemas.forEach(
                columnSchema ->
                        kuduTableKeyColumnTypes.put(
                                columnSchema.getName(), columnSchema.getType()));

        // 4、对比预期的kudu table schema和真实的 kudu table schema
        HashSet<String> tempKeyColumnName = new HashSet<>();
        tempKeyColumnName.addAll(keyColumnSchemaTypes.keySet());
        tempKeyColumnName.addAll(kuduTableKeyColumnTypes.keySet());

        // 5、找出发生变更的field，Type使用sinkRecord的schema转换来的Type
        // 如果是sinkRecord中的field被删除了，则Type==null，alter的时候做drop处理
        for (String columnName : tempKeyColumnName) {
            Type recordType = keyColumnSchemaTypes.get(columnName);
            if (recordType == null) {
                changeKeyColumnSchemaTypes.put(columnName, null);
            } else if (!recordType.equals(kuduTableKeyColumnTypes.get(columnName))) {
                changeKeyColumnSchemaTypes.put(columnName, keyColumnSchemaTypes.get(columnName));
            }
        }

        // 6、构建AlterTableOptions
        changeKeyColumnSchemaTypes.forEach(
                (columnName, type) -> {
                    if (type == null) {
                        if (!sinkConfig.allowRecordFieldsLessThanTableFields()) {
                            alterTableOptions.dropColumn(columnName);
                        }
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

            // 3、把真实的kudu table Column schema组装成Map<columnName, Type> 形式，方便对比
            HashMap<String, Type> kuduTableColumnTypes = new HashMap<>();
            columnSchemas.forEach(
                    columnSchema ->
                            kuduTableColumnTypes.put(
                                    columnSchema.getName(), columnSchema.getType()));

            // 4、对比预期的kudu table schema和真实的 kudu table schema
            HashSet<String> tempValueColumnName = new HashSet<>();
            tempValueColumnName.addAll(valueColumnSchemaTypes.keySet());
            tempValueColumnName.addAll(kuduTableColumnTypes.keySet());

            // 5、找出发生变更的field，Type使用sinkRecord的schema转换来的Type
            // 如果是sinkRecord中的field被删除了，则Type==null，alter的时候做drop处理
            for (String columnName : tempValueColumnName) {
                Type recordType = valueColumnSchemaTypes.get(columnName);
                if (recordType == null) {
                    changeValueColumnSchemaTypes.put(columnName, null);
                } else if (!recordType.equals(kuduTableColumnTypes.get(columnName))) {
                    changeValueColumnSchemaTypes.put(
                            columnName, valueColumnSchemaTypes.get(columnName));
                }
            }

            // 6、构建AlterTableOptions
            changeValueColumnSchemaTypes.forEach(
                    (columnName, type) -> {
                        if (type == null) {
                            if (!sinkConfig.allowRecordFieldsLessThanTableFields) {
                                alterTableOptions.dropColumn(columnName);
                            }
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

        // 7、向kudu发起alterTable
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

        tableName = tableName.toLowerCase(Locale.ROOT);
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
        // 联合主键是有序的，这就意味着，你的主键在进行代码添加（也就是通过columnSchemas.add()方法）时，
        // 必须要严格按照你的业务来进行，比如你不能把action_time放在非主键app_name之后，否则会报异常
        // ```
        // org.apache.kudu.client.NonRecoverableException: Got out-of-order key column: name:
        // “action_time” type: INT64 is_key: true is_nullable: false cfile_block_size: 0
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
                        new ColumnSchema.ColumnSchemaBuilder(
                                fieldName, kuduSchemaType);
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

        tableName = tableName.toLowerCase(Locale.ROOT);
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

        tableName = tableName.toLowerCase(Locale.ROOT);
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
                } else if (sinkConfig.ignoreNullValues) {
                    row.setNull(columnName);
                }
                break;
            case TINYINT:
                Byte tinyintValue =
                        SinkRecordTypeTransform.getTinyint(
                                columnName, columnSchemaName, columnType, valueStruct);
                if (tinyintValue != null) {
                    row.addByte(columnName, tinyintValue);
                } else if (sinkConfig.ignoreNullValues) {
                    row.setNull(columnName);
                }
                break;
            case SHORT:
                Short shortValue =
                        SinkRecordTypeTransform.getShort(
                                columnName, columnSchemaName, columnType, valueStruct);
                if (shortValue != null) {
                    row.addShort(columnName, shortValue);
                } else if (sinkConfig.ignoreNullValues) {
                    row.setNull(columnName);
                }
                break;
            case INT:
                Integer intValue =
                        SinkRecordTypeTransform.getInt(
                                columnName, columnSchemaName, columnType, valueStruct);
                if (intValue != null) {
                    row.addInt(columnName, intValue);
                } else if (sinkConfig.ignoreNullValues) {
                    row.setNull(columnName);
                }
                break;
            case LONG:
                Long longValue =
                        SinkRecordTypeTransform.getLong(
                                columnName, columnSchemaName, columnType, valueStruct);
                if (longValue != null) {
                    row.addLong(columnName, longValue);
                } else if (sinkConfig.ignoreNullValues) {
                    row.setNull(columnName);
                }
                break;
            case FLOAT:
                Float floatValue =
                        SinkRecordTypeTransform.getFloat(
                                columnName, columnSchemaName, columnType, valueStruct);
                if (floatValue != null) {
                    row.addFloat(columnName, floatValue);
                } else if (sinkConfig.ignoreNullValues) {
                    row.setNull(columnName);
                }
                break;
            case DOUBLE:
                Double doubleValue =
                        SinkRecordTypeTransform.getDouble(
                                columnName, columnSchemaName, columnType, valueStruct);
                if (doubleValue != null) {
                    row.addDouble(columnName, doubleValue);
                } else if (sinkConfig.ignoreNullValues) {
                    row.setNull(columnName);
                }
                break;
            case STRING:
                String stringValue =
                        SinkRecordTypeTransform.getString(
                                columnName, columnSchemaName, columnType, valueStruct);
                if (stringValue != null) {
                    row.addString(columnName, stringValue);
                } else if (sinkConfig.ignoreNullValues) {
                    row.setNull(columnName);
                }
                break;
            case BYTES:
                byte[] bytesValue =
                        SinkRecordTypeTransform.getBytes(
                                columnName, columnSchemaName, columnType, valueStruct);
                if (bytesValue != null) {
                    row.addBinary(columnName, bytesValue);
                } else if (sinkConfig.ignoreNullValues) {
                    row.setNull(columnName);
                }
                break;
            case TIME:
                Time timeValue =
                        SinkRecordTypeTransform.getTime(
                                columnName, columnSchemaName, columnType, valueStruct);
                if (timeValue != null) {
                    row.addString(columnName, timeValue.toString());
                } else if (sinkConfig.ignoreNullValues) {
                    row.setNull(columnName);
                }
                break;
            case DATE:
                Date dateValue =
                        SinkRecordTypeTransform.getDate(
                                columnName, columnSchemaName, columnType, valueStruct);
                if (dateValue != null) {
                    row.addString(columnName, dateValue.toString());
                } else if (sinkConfig.ignoreNullValues) {
                    row.setNull(columnName);
                }
                break;
            case TIMESTAMP:
                Timestamp timestampValue =
                        SinkRecordTypeTransform.getTimestamp(
                                columnName, columnSchemaName, columnType, valueStruct);
                if (timestampValue != null) {
                    row.addTimestamp(columnName, timestampValue);
                } else if (sinkConfig.ignoreNullValues) {
                    row.setNull(columnName);
                }
                break;
            case DECIMAL:
                BigDecimal decimalValue =
                        SinkRecordTypeTransform.getDecimal(
                                columnName, columnSchemaName, columnType, valueStruct);
                if (decimalValue != null) {
                    row.addDecimal(columnName, decimalValue);
                } else if (sinkConfig.ignoreNullValues) {
                    row.setNull(columnName);
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
