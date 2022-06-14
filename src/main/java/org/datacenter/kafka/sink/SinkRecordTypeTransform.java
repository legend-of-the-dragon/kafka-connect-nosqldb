package org.datacenter.kafka.sink;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;

/**
 * sink record 转换器，把kafka中的record按照配置转换成大数据标准类型.
 *
 * @author sky
 * @date 2022-05-18
 */
public class SinkRecordTypeTransform {

    public static final ZoneId ZONE_ID = ZoneId.of("Asia/Shanghai");

    /**
     * 根据record的schema中的schemaType和schemaName来匹配中转数据类型
     *
     * @param schemaType
     * @param schemaName
     * @return
     */
    public static SchemaTypeEnum getSchemaType(Schema.Type schemaType, String schemaName) {

        if (schemaName == null) {

            switch (schemaType) {
                case INT8:
                    return SchemaTypeEnum.TINYINT;
                case INT16:
                    return SchemaTypeEnum.SHORT;
                case INT32:
                    return SchemaTypeEnum.INT;
                case INT64:
                    return SchemaTypeEnum.LONG;
                case FLOAT32:
                    return SchemaTypeEnum.FLOAT;
                case FLOAT64:
                    return SchemaTypeEnum.DOUBLE;
                case BOOLEAN:
                    return SchemaTypeEnum.BOOLEAN;
                case STRING:
                    return SchemaTypeEnum.STRING;
                case BYTES:
                    return SchemaTypeEnum.BYTES;
                default:
                    break;
            }
        } else {

            if (schemaType.equals(Schema.Type.BYTES)
                    && schemaName.equals(SchemaTypeEnum.CONNECT_DECIMAL)) {
                return SchemaTypeEnum.DECIMAL;
            } else if (schemaType.equals(Schema.Type.INT32)
                    && (schemaName.equals(SchemaTypeEnum.CONNECT_DATE)
                            || schemaName.equals(SchemaTypeEnum.DEBEZIUM_DATE))) {
                return SchemaTypeEnum.DATE;
            } else if (schemaType.equals(Schema.Type.INT64)
                    && (schemaName.equals(SchemaTypeEnum.CONNECT_TIMESTAMP)
                            || schemaName.equals(SchemaTypeEnum.DEBEZIUM_TIMESTAMP))) {
                return SchemaTypeEnum.TIMESTAMP;
            } else if (schemaType.equals(Schema.Type.STRING)
                    && schemaName.equals(SchemaTypeEnum.DEBEZIUM_ZONED_TIMESTAMP)) {
                return SchemaTypeEnum.TIMESTAMP;
            } else if (schemaType.equals(Schema.Type.INT64)
                    && schemaName.equals(SchemaTypeEnum.DEBEZIUM_MICRO_TIMESTAMP)) {
                return SchemaTypeEnum.TIMESTAMP;
            } else if (schemaType.equals(Schema.Type.INT32)
                    && schemaName.equals(SchemaTypeEnum.CONNECT_TIME)) {
                return SchemaTypeEnum.TIME;
            } else if (schemaType.equals(Schema.Type.INT64)
                    && schemaName.equals(SchemaTypeEnum.DEBEZIUM_MICRO_TIME)) {
                return SchemaTypeEnum.TIME;
            } else if (schemaType.equals(Schema.Type.INT32)
                    && schemaName.equals(SchemaTypeEnum.DEBEZIUM_YEAR)) {
                return SchemaTypeEnum.INT;
            } else if (schemaType.equals(Schema.Type.BYTES)
                    && schemaName.equals(SchemaTypeEnum.DEBEZIUM_BITS)) {
                return SchemaTypeEnum.BYTES;
            } else if (schemaType.equals(Schema.Type.STRING)
                    && schemaName.equals(SchemaTypeEnum.DEBEZIUM_ENUM)) {
                return SchemaTypeEnum.STRING;
            } else if (schemaType.equals(Schema.Type.STRING)
                    && schemaName.equals(SchemaTypeEnum.DEBEZIUM_ENUM_SET)) {
                return SchemaTypeEnum.STRING;
            } else if (schemaType.equals(Schema.Type.STRING)
                    && schemaName.equals(SchemaTypeEnum.DEBEZIUM_JSON)) {
                return SchemaTypeEnum.STRING;
            }
        }

        return SchemaTypeEnum.OTHER;
    }

    public static Boolean getBoolean(
            String columnName,
            String columnSchemaName,
            Schema.Type columnType,
            final Struct valueStruct) {

        Object value = valueStruct.get(columnName);
        if (value == null) {
            return (Boolean) null;
        } else {
            return (Boolean) value;
        }
    }

    public static Byte getTinyint(
            String columnName,
            String columnSchemaName,
            Schema.Type columnType,
            final Struct valueStruct) {

        Object value = valueStruct.get(columnName);
        if (value == null) {
            return (Byte)null;
        } else {
            return (Byte) value;
        }
    }

    public static Short getShort(
            String columnName,
            String columnSchemaName,
            Schema.Type columnType,
            final Struct valueStruct) {

        Object value = valueStruct.get(columnName);
        if (value == null) {
            return (Short)null;
        } else {
            return (Short) value;
        }
    }

    public static Integer getInt(
            String columnName,
            String columnSchemaName,
            Schema.Type columnType,
            final Struct valueStruct) {

        Object val = valueStruct.get(columnName);
        if (val == null) {
            return (Integer)null;
        } else {
            return (Integer) val;
        }
    }

    public static Long getLong(
            String columnName,
            String columnSchemaName,
            Schema.Type columnType,
            final Struct valueStruct) {

        Object value = valueStruct.get(columnName);
        if (value == null) {
            return (Long)null;
        } else {
            return (Long) value;
        }
    }

    public static Float getFloat(
            String columnName,
            String columnSchemaName,
            Schema.Type columnType,
            final Struct valueStruct) {

        Object value = valueStruct.get(columnName);
        if (value == null) {
            return (Float)null;
        } else {
            return (Float) value;
        }
    }

    public static Double getDouble(
            String columnName,
            String columnSchemaName,
            Schema.Type columnType,
            final Struct valueStruct) {

        Object value = valueStruct.get(columnName);
        if (value == null) {
            return (Double)null;
        } else {
            return (Double) value;
        }
    }

    public static byte[] getBytes(
            String columnName,
            String columnSchemaName,
            Schema.Type columnType,
            final Struct valueStruct) {

        Object value = valueStruct.get(columnName);
        if (value == null) {
            return null;
        } else {
            if (value instanceof ByteBuffer) {

                ByteBuffer byteBuffer = (ByteBuffer) value;
                int remaining = byteBuffer.remaining();
                byte[] bytes = new byte[remaining];
                byteBuffer.get(bytes, 0, remaining);
                return bytes;
            } else if (value instanceof byte[]) {

                return (byte[]) value;
            } else {
                return (byte[])null;
            }
        }
    }

    public static String getString(
            String columnName,
            String columnSchemaName,
            Schema.Type columnType,
            final Struct valueStruct) {
        Object value = valueStruct.get(columnName);
        if (value == null) {
            return (String)null;
        } else {
            return (String) value;
        }
    }

    public static java.sql.Time getTime(
            String columnName,
            String columnSchemaName,
            Schema.Type columnType,
            final Struct valueStruct) {

        Object value = valueStruct.get(columnName);
        if (value == null) {
            return (java.sql.Time)null;
        } else {
            Instant timeInstant =
                    ((java.util.Date) value)
                            .toInstant()
                            .plus(-8, ChronoUnit.HOURS)
                            .atZone(ZONE_ID)
                            .toInstant();

            return new java.sql.Time(timeInstant.toEpochMilli());
        }
    }

    /**
     * 默认getDate的时候已经符合如下条件：<br>
     * if (columnType.equals(Schema.Type.INT32) &&
     * (columnSchemaName.equals(SchemaTypeEnum.CONNECT_DATE) ||
     * columnSchemaName.equals(SchemaTypeEnum.DEBEZIUM_DATE)))
     *
     * @param valueStruct
     * @return
     */
    public static Date getDate(
            String columnName,
            String columnSchemaName,
            Schema.Type columnType,
            final Struct valueStruct) {

        Object value = valueStruct.get(columnName);
        if (value == null) {
            return (java.sql.Date)null;
        } else {
            java.util.Date dateValue = (java.util.Date) value;
            Instant plus = dateValue.toInstant().atZone(ZONE_ID).toInstant();
            return new java.sql.Date(plus.toEpochMilli());
        }
    }

    public static Timestamp getTimestamp(
            String columnName,
            String columnSchemaName,
            Schema.Type columnType,
            final Struct valueStruct) {

        Object value = valueStruct.get(columnName);
        if (value == null) {
            return (Timestamp)null;
        } else {

            if (columnType.equals(Schema.Type.INT64)
                    && (columnSchemaName.equals(SchemaTypeEnum.CONNECT_TIMESTAMP)
                            || columnSchemaName.equals(SchemaTypeEnum.DEBEZIUM_TIMESTAMP))) {

                return new Timestamp(((java.util.Date) value).getTime());
            } else if (columnType.equals(Schema.Type.STRING)
                    && columnSchemaName.equals(SchemaTypeEnum.DEBEZIUM_ZONED_TIMESTAMP)) {
                return new Timestamp(
                        Instant.parse((String) value)
                                .plus(8, ChronoUnit.HOURS)
                                .atZone(ZONE_ID)
                                .toInstant()
                                .toEpochMilli());
            } else if (columnType.equals(Schema.Type.INT64)
                    && columnSchemaName.equals(SchemaTypeEnum.DEBEZIUM_MICRO_TIMESTAMP)) {

                return new Timestamp((Long) value);
            } else {
                return new Timestamp((Long) value);
            }
        }
    }

    public static BigDecimal getDecimal(
            String columnName,
            String columnSchemaName,
            Schema.Type columnType,
            final Struct valueStruct) {

        Object value = valueStruct.get(columnName);
        if (value == null) {
            return (BigDecimal)null;
        } else {
            return (BigDecimal) value;
        }
    }

    public static Object getOther(
            String columnName,
            String columnSchemaName,
            Schema.Type columnType,
            final Struct valueStruct) {

        return valueStruct.get(columnName);
    }
}
