package org.datacenter.kafka.sink;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;

import java.util.Locale;

/**
 * 中转数据类型
 *
 * @author sky
 * @date 2022-05-17
 * @discription
 */
public enum SchemaTypeEnum {
    TINYINT(Schema.Type.INT8, null, null),
    /**
     * 16-bit signed integer
     *
     * <p>Note that if you have an unsigned 16-bit data source, {@link Schema.Type#INT32} will be
     * required to safely capture all valid values
     */
    SHORT(Schema.Type.INT16, null, null),
    /**
     * 32-bit signed integer
     *
     * <p>Note that if you have an unsigned 32-bit data source, {@link Schema.Type#INT64} will be
     * required to safely capture all valid values
     */
    INT(Schema.Type.INT32, null, null),
    /**
     * 64-bit signed integer
     *
     * <p>Note that if you have an unsigned 64-bit data source, the {@link Decimal} logical type
     * (encoded as {@link Schema.Type#BYTES}) will be required to safely capture all valid values
     */
    LONG(Schema.Type.INT64, null, null),
    /** 32-bit IEEE 754 floating point number */
    FLOAT(Schema.Type.FLOAT32, null, null),
    /** 64-bit IEEE 754 floating point number */
    DOUBLE(Schema.Type.FLOAT64, null, null),
    /** Boolean value (true or false) */
    BOOLEAN(Schema.Type.BOOLEAN, null, null),
    /**
     * Character string that supports all Unicode characters.
     *
     * <p>Note that this does not imply any specific encoding (e.g. UTF-8) as this is an in-memory
     * representation.
     */
    STRING(Schema.Type.STRING, null, null),
    /** Sequence of unsigned 8-bit bytes */
    BYTES(Schema.Type.BYTES, null, null),
    DECIMAL(Schema.Type.BYTES, SchemaTypeEnum.CONNECT_DECIMAL, null),
    TIME(Schema.Type.INT32, SchemaTypeEnum.CONNECT_TIME, null),

    DATE(Schema.Type.INT32, SchemaTypeEnum.CONNECT_DATE, SchemaTypeEnum.DEBEZIUM_DATE),
    TIMESTAMP(Schema.Type.INT64, null, SchemaTypeEnum.DEBEZIUM_MICRO_TIMESTAMP),
    OTHER(null, null, null);

    private final String name;

    private final Schema.Type schemaType;
    private final String connectName;
    private final String debeziumName;

    SchemaTypeEnum(Schema.Type schemaType, String connectName, String debeziumName) {
        this.name = this.name().toLowerCase(Locale.ROOT);
        this.schemaType = schemaType;
        this.connectName = connectName;
        this.debeziumName = debeziumName;
    }

    public String getName() {
        return this.name;
    }

    public String getConnectName() {
        return this.connectName;
    }

    public String getDebeziumName() {
        return this.debeziumName;
    }

    public Schema.Type getSchemaType() {
        return this.schemaType;
    }

    public static final String DEBEZIUM_BITS = "io.debezium.data.Bits";
    public static final String CONNECT_DECIMAL = "org.apache.kafka.connect.data.Decimal";
    public static final String CONNECT_DATE = "org.apache.kafka.connect.data.Date";
    public static final String DEBEZIUM_DATE = "io.debezium.time.Date";
    public static final String CONNECT_TIMESTAMP = "org.apache.kafka.connect.data.Timestamp";
    public static final String DEBEZIUM_TIMESTAMP = "io.debezium.time.Timestamp";
    public static final String DEBEZIUM_ZONED_TIMESTAMP = "io.debezium.time.ZonedTimestamp";
    public static final String DEBEZIUM_MICRO_TIMESTAMP = "io.debezium.time.MicroTimestamp";
    public static final String CONNECT_TIME = "org.apache.kafka.connect.data.Time";
    public static final String DEBEZIUM_MICRO_TIME = "io.debezium.time.MicroTime";
    public static final String DEBEZIUM_YEAR = "io.debezium.time.Year";
    public static final String DEBEZIUM_ENUM = "io.debezium.data.Enum";
    public static final String DEBEZIUM_ENUM_SET = "io.debezium.data.EnumSet";
    public static final String DEBEZIUM_JSON = "io.debezium.data.Json";
}
