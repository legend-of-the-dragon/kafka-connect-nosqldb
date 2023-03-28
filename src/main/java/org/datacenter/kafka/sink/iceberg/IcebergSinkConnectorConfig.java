package org.datacenter.kafka.sink.iceberg;

import org.apache.iceberg.TableProperties;
import org.apache.kafka.common.config.ConfigDef;
import org.datacenter.kafka.sink.AbstractConnectorConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * KuduSinkConnectorConfig
 *
 * @author sky
 * @date 2022-05-10
 */
public class IcebergSinkConnectorConfig extends AbstractConnectorConfig {

    public static final String HIVE_CATALOG = "hiveCatalog";
    public static final String HADOOP_CATALOG = "hadoopCatalog";
    public static final String DEFAULT_CATALOG_NAME = "icebergSinkConnector";

    public static final String CATALOGNAME_KEY = "iceberg.catalog.name";
    public static final String CATALOGNAME_DEFAULT = DEFAULT_CATALOG_NAME;
    private static final String CATALOGNAME_DOC = "catalog名称.";

    public static final String TABLE_NAMESPACE_KEY = "table.namespace";
    public static final String TABLE_NAMESPACE_DEFAULT = "default";
    private static final String TABLE_NAMESPACE_DOC = "iceberg的表空间名称.";

    public static final String TABLE_WRITE_FORMAT_KEY = "iceberg.table-default.write.format";
    public static final String TABLE_WRITE_FORMAT_DEFAULT = "avro";
    private static final String TABLE_WRITE_FORMAT_DOC = "数据写入格式.";

    public static final String CATALOGIMPL_KEY = "iceberg.catalog.catalog-impl";
    public static final String CATALOGIMPL_DEFAULT = "org.apache.iceberg.hive.HiveCatalog";
    private static final String CATALOGIMPL_DOC =
            "Indicates if overwriting or removing existing values in the sink cache is enabled. Sink connector performs better if this flag is disabled.";

    public static final String HDFS_CONFIG_FILE_KEY = "iceberg.catalog.hdfs-config-file";
    public static final String HDFS_CONFIG_FILE_DEFAULT = null;
    private static final String HDFS_CONFIG_FILE_DOC = "core-site.xml,hdfs-site.xml的文件路径";

    public static final String WAREHOUSE_KEY = "iceberg.catalog.warehouse";
    public static final String WAREHOUSE_DEFAULT = null;
    private static final String WAREHOUSE_DOC = ".";

    public static final String TABLE_PREFIX_KEY = "iceberg.table-default.prefix";
    public static final String TABLE_PREFIX_DEFAULT = "iceberg_";
    private static final String TABLE_PREFIX_DOC = "自动创建的iceberg表的表名前缀.";

    public static final String TABLE_AUTO_CREATE_KEY = "iceberg.table-default.auto-create";
    public static final boolean TABLE_AUTO_CREATE_DEFAULT = true;
    private static final String TABLE_AUTO_CREATE_DOC = "是否自动创建表.";

    public static final String TABLE_PROPERTIES_PARQUET_BATCHSIZE_BYTES_KEY =
            "iceberg.table-default.parquetBatchSize.Bytes";
    public static final int TABLE_PROPERTIES_PARQUET_BATCHSIZE_BYTES_DEFAULT =
            TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT;
    private static final String TABLE_PROPERTIES_PARQUET_BATCHSIZE_BYTES_DOC =
            "如果iceberg表使用parquet格式，一次读取的字节数.";

    public static ConfigDef configDef() {
        return AbstractConnectorConfig.configDef()
                .define(
                        CATALOGNAME_KEY,
                        ConfigDef.Type.STRING,
                        CATALOGNAME_DEFAULT,
                        ConfigDef.Importance.LOW,
                        CATALOGNAME_DOC)
                .define(
                        TABLE_NAMESPACE_KEY,
                        ConfigDef.Type.STRING,
                        TABLE_NAMESPACE_DEFAULT,
                        ConfigDef.Importance.LOW,
                        TABLE_NAMESPACE_DOC)
                .define(
                        TABLE_WRITE_FORMAT_KEY,
                        ConfigDef.Type.STRING,
                        TABLE_WRITE_FORMAT_DEFAULT,
                        ConfigDef.Importance.LOW,
                        TABLE_WRITE_FORMAT_DOC)
                .define(
                        CATALOGIMPL_KEY,
                        ConfigDef.Type.STRING,
                        CATALOGIMPL_DEFAULT,
                        ConfigDef.Importance.LOW,
                        CATALOGIMPL_DOC)
                .define(
                        HDFS_CONFIG_FILE_KEY,
                        ConfigDef.Type.STRING,
                        HDFS_CONFIG_FILE_DEFAULT,
                        ConfigDef.Importance.LOW,
                        HDFS_CONFIG_FILE_DOC)
                .define(
                        WAREHOUSE_KEY,
                        ConfigDef.Type.STRING,
                        WAREHOUSE_DEFAULT,
                        ConfigDef.Importance.LOW,
                        WAREHOUSE_DOC)
                .define(
                        TABLE_AUTO_CREATE_KEY,
                        ConfigDef.Type.BOOLEAN,
                        TABLE_AUTO_CREATE_DEFAULT,
                        ConfigDef.Importance.LOW,
                        TABLE_AUTO_CREATE_DOC)
                .define(
                        TABLE_PREFIX_KEY,
                        ConfigDef.Type.STRING,
                        TABLE_PREFIX_DEFAULT,
                        ConfigDef.Importance.LOW,
                        TABLE_PREFIX_DOC)
                .define(
                        TABLE_PROPERTIES_PARQUET_BATCHSIZE_BYTES_KEY,
                        ConfigDef.Type.INT,
                        TABLE_PROPERTIES_PARQUET_BATCHSIZE_BYTES_DEFAULT,
                        ConfigDef.Importance.LOW,
                        TABLE_PROPERTIES_PARQUET_BATCHSIZE_BYTES_DOC);
    }

    public IcebergSinkConnectorConfig(Map<String, String> properties) {
        super(configDef(), properties);
        this.properties = properties;

        this.catalogName = catalogName();
        this.tableNamespace = tableNamespace();
        this.tableWriteFormat = tableWriteFormat();
        this.catalogImpl = catalogImpl();
        this.hdfsConfigFile = hdfsConfigFile();
        this.warehouse = warehouse();
        this.isTableAutoCreate = isTableAutoCreate();
        this.tableNamePrefix = tableNamePrefix();
        this.parquetBatchSizeInBytes = parquetBatchSizeInBytes();
    }

    public final String catalogName;
    public final String tableNamespace;
    public final String tableWriteFormat;
    public final String catalogImpl;
    public final String hdfsConfigFile;
    public final String warehouse;
    public final boolean isTableAutoCreate;
    public final String tableNamePrefix;
    public final int parquetBatchSizeInBytes;

    public String catalogName() {
        return this.getString(CATALOGNAME_KEY);
    }

    public String tableNamespace() {
        return this.getString(TABLE_NAMESPACE_KEY);
    }

    public String tableWriteFormat() {
        return this.getString(TABLE_WRITE_FORMAT_KEY);
    }

    public String catalogImpl() {
        return this.getString(CATALOGIMPL_KEY);
    }

    public String hdfsConfigFile() {
        return this.getString(HDFS_CONFIG_FILE_KEY);
    }

    public String warehouse() {
        return this.getString(WAREHOUSE_KEY);
    }

    public boolean isTableAutoCreate() {
        return this.getBoolean(TABLE_AUTO_CREATE_KEY);
    }

    public String getTablePrefix() {
        return this.getString(TABLE_PREFIX_KEY);
    }

    public int parquetBatchSizeInBytes() {
        return this.getInt(TABLE_PROPERTIES_PARQUET_BATCHSIZE_BYTES_KEY);
    }

    public static final String ICEBERG_CATALOG_PREFIX = "iceberg.catalog.";
    public static final String ICEBERG_TABLE_PREFIX = "iceberg.table-default.";
    public static final String ICEBERG_CATALOG_NAME = ICEBERG_CATALOG_PREFIX + "name";
    public static final String ICEBERG_CATALOG_IMPL = ICEBERG_CATALOG_PREFIX + "catalog-impl";
    public static final String ICEBERG_CATALOG_TYPE = ICEBERG_CATALOG_PREFIX + "type";

    private final Map<String, String> properties;

    public Map<String, String> getIcebergCatalogConfiguration() {
        return getConfiguration(ICEBERG_CATALOG_PREFIX);
    }

    public Map<String, String> getIcebergTableConfiguration() {
        return getConfiguration(ICEBERG_TABLE_PREFIX);
    }

    private Map<String, String> getConfiguration(String prefix) {
        Map<String, String> config = new HashMap<>();
        properties.keySet().stream()
                .filter(key -> key.startsWith(prefix))
                .forEach(
                        key -> {
                            config.put(key.substring(prefix.length()), properties.get(key));
                        });
        return config;
    }
}
