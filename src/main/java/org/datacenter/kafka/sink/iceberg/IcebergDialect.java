package org.datacenter.kafka.sink.iceberg;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.datacenter.kafka.sink.*;
import org.datacenter.kafka.sink.iceberg.connect.IcebergSinkConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;

import static org.apache.iceberg.CatalogUtil.ICEBERG_CATALOG_HADOOP;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;

/**
 * KuduDialect
 *
 * @author sky
 */
public class IcebergDialect extends AbstractDialect<Table, Type> {

    private static final Logger log = LoggerFactory.getLogger(IcebergDialect.class);

    private final IcebergSinkConnectorConfig sinkConfig;
    private final Catalog catalog;
    private final IcebergTableWriterFactory writerFactory;
    private final int taskId;

    public IcebergDialect(IcebergSinkConnectorConfig sinkConfig, int taskId) {

        this.sinkConfig = sinkConfig;
        this.catalog = create(sinkConfig);
        this.writerFactory = new IcebergTableWriterFactory(sinkConfig);
        this.taskId = taskId;
    }

    public static Catalog create(IcebergSinkConnectorConfig sinkConfig) {
        Configuration hadoopConfig = new Configuration();
        if (sinkConfig.catalogImpl.equals(ICEBERG_CATALOG_HADOOP)) {
            String hdfsConfigFile = sinkConfig.hdfsConfigFile;
            String[] paths = hdfsConfigFile.split(",");
            for (String path : paths) {
                hadoopConfig.addResource(path);
            }
        }
        Map<String, String> catalogConfiguration = sinkConfig.getIcebergCatalogConfiguration();
        catalogConfiguration.forEach(hadoopConfig::set);
        return CatalogUtil.buildIcebergCatalog(
                sinkConfig.catalogName, catalogConfiguration, hadoopConfig);
    }

    Map<String, Boolean> tableExistsCache = new HashMap<>();
    private final Map<String, Table> tableCache = new HashMap<>();

    @Override
    public boolean tableExists(String tableName) throws DbDdlException {

        Boolean tableExists = tableExistsCache.get(tableName);
        if (tableExists == null) {
            TableIdentifier tableIdentifier =
                    TableIdentifier.of(Namespace.of(sinkConfig.tableNamespace), tableName);
            tableExists = catalog.tableExists(tableIdentifier);
            if (tableExists) {
                tableExistsCache.put(tableName, tableExists);
            }
        }
        return tableExists;
    }

    public Table getTable(String tableName) throws DbDdlException {

        TableIdentifier tableId =
                TableIdentifier.of(Namespace.of(sinkConfig.tableNamespace), tableName);
        Table table = tableCache.get(tableName);
        if (table == null) {
            table = IcebergUtil.loadIcebergTable(catalog, tableId);
            tableCache.put(tableName, table);
        }
        return table;
    }

    @Override
    public boolean needChangeTableStructure(String tableName, Schema keySchema, Schema valueSchema)
            throws DbDdlException {

        return false;
    }

    @Override
    public void alterTable(String tableName, Schema keySchema, Schema valueSchema)
            throws DbDdlException {}

    @Override
    public void createTable(String tableName, Schema keySchema, Schema valueSchema)
            throws DbDdlException {

        Map<String, Types.NestedField> schemaColumns = new HashMap<>();

        TableIdentifier tableIdentifier =
                TableIdentifier.of(Namespace.of(sinkConfig.tableNamespace), tableName);
        Map<String, String> icebergTableConfiguration = sinkConfig.getIcebergTableConfiguration();
        org.apache.iceberg.Schema schema;

        int columnId = 0;
        Map<String, Integer> filedIds = new HashMap<>();
        List<Field> fields = valueSchema.fields();
        for (Field field : fields) {
            columnId++;
            String fieldName = field.name();
            String columnSchemaName = field.schema().name();
            Schema.Type fieldType = field.schema().type();
            String fieldDoc = field.schema().doc();
            Type.PrimitiveType dialectSchemaType =
                    getDialectSchemaType(fieldType, columnSchemaName);
            schemaColumns.put(
                    fieldName,
                    Types.NestedField.optional(columnId, fieldName, dialectSchemaType, fieldDoc));
            filedIds.put(fieldName, columnId);
        }

        // 构建主键
        Set<Integer> identifierFieldIds = new HashSet<>();
        for (Field field : keySchema.fields()) {
            String filedName = field.name();
            Integer id = filedIds.get(filedName);
            if (id == null) {
                throw new DbDdlException("主键识别错误.");
            }
            identifierFieldIds.add(id);

            // 把字段属性改成必选
            Types.NestedField icebergField = schemaColumns.get(filedName);
            icebergField =
                    Types.NestedField.of(
                            icebergField.fieldId(),
                            false,
                            icebergField.name(),
                            icebergField.type(),
                            icebergField.doc());
            schemaColumns.put(filedName, icebergField);
        }

        schema =
                new org.apache.iceberg.Schema(
                        new ArrayList<>(schemaColumns.values()), identifierFieldIds);

        // 分区字段 = bucket_10(实例id + 库名 + 所有主键字段)
        // <实例id + 库名由配置决定是否需要加入主键字段>
        // <sink默认不进行分区，需要更加复杂的分区需要手工建表>

        final PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

        catalog.buildTable(tableIdentifier, schema)
                .withProperties(icebergTableConfiguration)
                .withProperty(FORMAT_VERSION, "2")
                .withSortOrder(IcebergUtil.getIdentifierFieldsAsSortOrder(schema))
                .withPartitionSpec(partitionSpec)
                .create();

        boolean tableExists = tableExists(tableName);
        if (!tableExists) {
            throw new DbDdlException("创建表之后依旧找不到表.");
        }
    }

    @Override
    public Type.PrimitiveType getDialectSchemaType(
            Schema.Type columnType, String columnSchemaName) {
        Type.PrimitiveType icebergSchemaType = null;

        SchemaTypeEnum columnSchemaTypeEnum =
                SinkRecordTypeTransform.getSchemaType(columnType, columnSchemaName);

        switch (columnSchemaTypeEnum) {
            case DECIMAL:
                icebergSchemaType = Types.StringType.get();
                break;
            case DATE:
            case TIME:
            case STRING:
                icebergSchemaType = Types.StringType.get();
                break;
            case TIMESTAMP:
                icebergSchemaType = Types.LongType.get();
                break;
            case TINYINT:
            case SHORT:
            case INT:
                icebergSchemaType = Types.IntegerType.get();
                break;
            case LONG:
                icebergSchemaType = Types.LongType.get();
                break;
            case BOOLEAN:
                icebergSchemaType = Types.BooleanType.get();
                break;
            case FLOAT:
                icebergSchemaType = Types.FloatType.get();
                break;
            case DOUBLE:
                icebergSchemaType = Types.DoubleType.get();
                break;
            case BYTES:
                icebergSchemaType = Types.BinaryType.get();
                break;
        }
        return icebergSchemaType;
    }

    @Override
    public boolean applyUpsertRecord(String tableName, SinkRecord sinkRecord) {

        Table table = getTable(tableName);

        org.apache.iceberg.Schema schema = table.schema();
        org.datacenter.kafka.sink.iceberg.SinkRecord icebergRecord =
                org.datacenter.kafka.sink.iceberg.SinkRecord.create(schema);
        Struct value = (Struct) sinkRecord.value();
        for (Field field : sinkRecord.valueSchema().fields()) {
            addRowValues(tableName, icebergRecord, field, value);
        }
        icebergRecord.setOp(2);
        apply(tableName, icebergRecord);

        return true;
    }

    @Override
    public boolean applyDeleteRecord(String tableName, SinkRecord sinkRecord) {

        Table table = getTable(tableName);
        org.apache.iceberg.Schema schema = table.schema();
        org.datacenter.kafka.sink.iceberg.SinkRecord icebergRecord =
                org.datacenter.kafka.sink.iceberg.SinkRecord.create(schema);
        Struct key = (Struct) sinkRecord.key();
        for (Field field : sinkRecord.keySchema().fields()) {
            addRowValues(tableName, icebergRecord, field, key);
        }
        icebergRecord.setOp(0);
        apply(tableName, icebergRecord);

        return true;
    }

    private void apply(String tableName, org.datacenter.kafka.sink.iceberg.SinkRecord icebergRecord)
            throws DbDmlException {

        TaskWriter<Record> taskWriter = getTaskWriter(tableName);

        try {
            taskWriter.write(icebergRecord);
        } catch (IOException e) {
            throw new DbDmlException("写入数据错误.", e);
        }
    }

    Map<String, TaskWriter<Record>> taskWriterCache = new HashMap<>();

    public TaskWriter<Record> getTaskWriter(String tableName) {
        TaskWriter<Record> taskWriter;
        Table icebergTable = getTable(tableName);
        taskWriter = writerFactory.create(icebergTable, taskId);
        return taskWriter;
    }

    @Override
    public Pair<Boolean, Long> elasticLimit(String connectorName) {
        return Pair.of(false, 0L);
    }

    @Override
    public void flush() throws DbDmlException {

        Set<String> tableNames = taskWriterCache.keySet();
        for (String tableName : tableNames) {

            TaskWriter<Record> taskWriter = getTaskWriter(tableName);
            WriteResult result;
            try {
                //  taskWriter.close();
                result = taskWriter.complete();
            } catch (IOException e) {
                throw new DbDmlException("iceberg flush 失败.", e);
            }

            Table icebergTable = getTable(tableName);
            RowDelta rowDelta = icebergTable.newRowDelta();
            Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);
            Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);
            rowDelta.commit();
        }
    }

    @Override
    public void stop() throws ConnectException {

        Set<String> tableNames = taskWriterCache.keySet();
        for (String tableName : tableNames) {

            TaskWriter<Record> taskWriter = getTaskWriter(tableName);
            WriteResult result;
            try {
                taskWriter.close();
                result = taskWriter.complete();
            } catch (IOException e) {
                throw new DbDmlException("flush 失败.", e);
            }

            Table icebergTable = getTable(tableName);
            RowDelta rowDelta = icebergTable.newRowDelta();
            Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);
            Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);
            rowDelta.commit();
        }
    }

    private void addRowValues(
            String tableName,
            final org.datacenter.kafka.sink.iceberg.SinkRecord icebergRecord,
            final Field field,
            final Struct valueStruct) {

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
                    icebergRecord.setField(columnName, booleanValue);
                } else if (sinkConfig.ignoreNullValues) {
                    icebergRecord.setField(columnName, null);
                }
                break;
            case TINYINT:
                Byte tinyintValue =
                        SinkRecordTypeTransform.getTinyint(
                                columnName, columnSchemaName, columnType, valueStruct);
                if (tinyintValue != null) {
                    icebergRecord.setField(columnName, tinyintValue);
                } else if (sinkConfig.ignoreNullValues) {
                    icebergRecord.setField(columnName, null);
                }
                break;
            case SHORT:
                Short shortValue =
                        SinkRecordTypeTransform.getShort(
                                columnName, columnSchemaName, columnType, valueStruct);
                if (shortValue != null) {
                    icebergRecord.setField(columnName, shortValue);
                } else if (sinkConfig.ignoreNullValues) {
                    icebergRecord.setField(columnName, null);
                }
                break;
            case INT:
                Integer intValue =
                        SinkRecordTypeTransform.getInt(
                                columnName, columnSchemaName, columnType, valueStruct);
                if (intValue != null) {
                    icebergRecord.setField(columnName, intValue);
                } else if (sinkConfig.ignoreNullValues) {
                    icebergRecord.setField(columnName, null);
                }
                break;
            case LONG:
                Long longValue =
                        SinkRecordTypeTransform.getLong(
                                columnName, columnSchemaName, columnType, valueStruct);
                if (longValue != null) {
                    icebergRecord.setField(columnName, longValue);
                } else if (sinkConfig.ignoreNullValues) {
                    icebergRecord.setField(columnName, null);
                }
                break;
            case FLOAT:
                Float floatValue =
                        SinkRecordTypeTransform.getFloat(
                                columnName, columnSchemaName, columnType, valueStruct);
                if (floatValue != null) {
                    icebergRecord.setField(columnName, floatValue);
                } else if (sinkConfig.ignoreNullValues) {
                    icebergRecord.setField(columnName, null);
                }
                break;
            case DOUBLE:
                Double doubleValue =
                        SinkRecordTypeTransform.getDouble(
                                columnName, columnSchemaName, columnType, valueStruct);
                if (doubleValue != null) {
                    icebergRecord.setField(columnName, doubleValue);
                } else if (sinkConfig.ignoreNullValues) {
                    icebergRecord.setField(columnName, null);
                }
                break;
            case STRING:
                String stringValue =
                        SinkRecordTypeTransform.getString(
                                columnName, columnSchemaName, columnType, valueStruct);
                if (stringValue != null) {
                    icebergRecord.setField(columnName, stringValue);
                } else if (sinkConfig.ignoreNullValues) {
                    icebergRecord.setField(columnName, null);
                }
                break;
            case BYTES:
                byte[] bytesValue =
                        SinkRecordTypeTransform.getBytes(
                                columnName, columnSchemaName, columnType, valueStruct);
                if (bytesValue != null) {
                    icebergRecord.setField(columnName, bytesValue);
                } else if (sinkConfig.ignoreNullValues) {
                    icebergRecord.setField(columnName, null);
                }
                break;
            case TIME:
                Time timeValue =
                        SinkRecordTypeTransform.getTime(
                                columnName, columnSchemaName, columnType, valueStruct);
                if (timeValue != null) {
                    icebergRecord.setField(columnName, timeValue.toString());
                } else if (sinkConfig.ignoreNullValues) {
                    icebergRecord.setField(columnName, null);
                }
                break;
            case DATE:
                Date dateValue =
                        SinkRecordTypeTransform.getDate(
                                columnName, columnSchemaName, columnType, valueStruct);
                if (dateValue != null) {
                    icebergRecord.setField(columnName, dateValue.toString());
                } else if (sinkConfig.ignoreNullValues) {
                    icebergRecord.setField(columnName, null);
                }
                break;
            case TIMESTAMP:
                Timestamp timestampValue =
                        SinkRecordTypeTransform.getTimestamp(
                                columnName, columnSchemaName, columnType, valueStruct);
                if (timestampValue != null) {
                    icebergRecord.setField(columnName, timestampValue);
                } else if (sinkConfig.ignoreNullValues) {
                    icebergRecord.setField(columnName, null);
                }
                break;
            case DECIMAL:
                BigDecimal decimalValue =
                        SinkRecordTypeTransform.getDecimal(
                                columnName, columnSchemaName, columnType, valueStruct);
                if (decimalValue != null) {
                    icebergRecord.setField(columnName, decimalValue);
                } else if (sinkConfig.ignoreNullValues) {
                    icebergRecord.setField(columnName, null);
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
