package org.datacenter.kafka.sink.ignite;

import org.apache.ignite.IgniteBinary;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.datacenter.kafka.DataGrid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author sky
 * @date 2022-05-10
 */
final class SinkRecordParser {

    public static final String KEY_SUFFIX = ".Key";
    public static final String VALUE_SUFFIX = ".Value";
    private static final Logger log = LoggerFactory.getLogger(SinkRecordParser.class);
    public static final String DEBEZIUM_TIME_ZONED_TIMESTAMP = "io.debezium.time.ZonedTimestamp";
    private final TimeZone timeZone;

    SinkRecordParser(TimeZone timeZone) {
        this.timeZone = timeZone;
    }

    private static Object toIgniteBinary(Object obj, String typeName) {
        if (obj != null && !hasNativeIgniteType(obj.getClass())) {
            if (obj instanceof Map) {
                return toIgniteBinary((Map) obj, typeName);
            } else if (obj instanceof List) {
                return toIgniteBinary((List) obj, typeName);
            } else {
                throw new DataException(
                        "Failed to convert ["
                                + obj
                                + "]. Unsupported data type: "
                                + obj.getClass().getName());
            }
        } else {
            return obj;
        }
    }

    private static BinaryObject toIgniteBinary(Map<?, ?> map, String typeName) {
        BinaryObjectBuilder res = DataGrid.SINK.binary().builder(typeName);

        for (Map.Entry<?, ?> value : map.entrySet()) {
            String name = value.getKey().toString();
            res.setField(name, toIgniteBinary(value.getValue(), name));
        }

        return res.build();
    }

    private static List<?> toIgniteBinary(List<?> lst, String typeName) {
        List<Object> res = new ArrayList<>();

        for (Object o : lst) {
            res.add(toIgniteBinary(o, typeName));
        }

        return res;
    }

    private static boolean hasNativeIgniteType(Class<?> cls) {
        if (cls == null) {
            return false;
        } else {
            return cls.isPrimitive()
                    || cls == String.class
                    || Number.class.isAssignableFrom(cls)
                    || cls == Boolean.class
                    || cls == Character.class
                    || cls == UUID.class
                    || cls == byte[].class
                    || Date.class.isAssignableFrom(cls)
                    || IgniteBinary.class.isAssignableFrom(cls)
                    || hasNativeIgniteType(cls.getComponentType());
        }
    }

    Object parseKey(SinkRecord rec) {

        return this.parse(rec.key(), rec.keySchema(), rec.topic() + KEY_SUFFIX);
    }

    Object parseValue(SinkRecord rec) {
        return this.parse(rec.value(), rec.valueSchema(), rec.topic() + VALUE_SUFFIX);
    }

    private Object parse(Object obj, Schema schema, String typeName) {

        if (obj == null) {
            return null;
        }

        Schema finalSchema;
        if (schema == null && obj instanceof Struct) {
            finalSchema = ((Struct) obj).schema();
        } else {
            finalSchema = schema;
        }

        if (finalSchema != null) {
            if (!finalSchema.type().isPrimitive()) {

                if (finalSchema.type() == Type.ARRAY) {
                    if (!finalSchema.valueSchema().type().isPrimitive()) {
                        return this.parseArray((List<?>) obj, finalSchema);
                    }
                } else if (finalSchema.type() == Type.MAP) {
                    if (!finalSchema.keySchema().type().isPrimitive()
                            || !finalSchema.valueSchema().type().isPrimitive()) {

                        Set<Map.Entry<?, ?>> set = ((Map) obj).entrySet();

                        return set.stream()
                                .map(
                                        (e) ->
                                                new AbstractMap.SimpleEntry(
                                                        this.parse(
                                                                e.getKey(),
                                                                finalSchema.keySchema(),
                                                                "KafkaKey"),
                                                        this.parse(
                                                                e.getValue(),
                                                                finalSchema.valueSchema(),
                                                                "KafkaValue")))
                                .collect(
                                        Collectors.toMap(
                                                AbstractMap.SimpleEntry::getKey,
                                                AbstractMap.SimpleEntry::getValue));
                    }
                } else {
                    Object binaryObj =
                            this.createIgniteBinaryObject((Struct) obj, finalSchema, typeName);
                    if (binaryObj != null) {
                        return binaryObj;
                    }
                }
            } else {
                Object fieldValue;
                try {
                    String schemaName = schema.name();
                    if (schemaName != null) {
                        fieldValue = maybeBindLogical(schemaName, obj);
                    } else {
                        Type schemaType = schema.type();
                        fieldValue = maybeBindPrimitive(schemaType, obj);
                    }
                } catch (Exception e) {
                    throw new ConnectException("fieldValue bind exception.", e);
                }

                if (fieldValue == null) {
                    throw new ConnectException("fieldValue bind exception.");
                }

                return fieldValue;
            }
        } else if (obj instanceof Map) {
            return toIgniteBinary((Map<?, ?>) obj, typeName);
        }

        return obj;
    }

    private Object maybeBindLogical(String schemaName, Object value) throws Exception {
        switch (schemaName) {
            case Date.LOGICAL_NAME:
                return new java.sql.Date(((java.util.Date) value).getTime());
            case Decimal.LOGICAL_NAME:
                return value;
            case Time.LOGICAL_NAME:
                return new java.sql.Time(((java.util.Date) value).getTime());
            case Timestamp.LOGICAL_NAME:
                return new java.sql.Timestamp(((java.util.Date) value).getTime());
            case DEBEZIUM_TIME_ZONED_TIMESTAMP:
                return new java.sql.Timestamp(Instant.parse((String) value).toEpochMilli());
            default:
                return null;
        }
    }

    private Object maybeBindPrimitive(Type schemaType, Object value) throws Exception {

        switch (schemaType) {
            case INT8:
            case INT16:
            case INT32:
                return (int) value;
            case INT64:
                return (long) value;
            case FLOAT32:
            case FLOAT64:
                return (Double) value;
            case BOOLEAN:
                return (boolean) value;
            case STRING:
                return (String) value;
            case BYTES:
                final byte[] bytes;
                if (value instanceof ByteBuffer) {
                    final ByteBuffer buffer = ((ByteBuffer) value).slice();
                    bytes = new byte[buffer.remaining()];
                    buffer.get(bytes);
                } else {
                    bytes = (byte[]) value;
                }
                return bytes;
            default:
                return null;
        }
    }

    private Object createIgniteBinaryObject(Struct struct, Schema schema, String typeName) {

        IgniteBinary binary = DataGrid.SINK.binary();
        BinaryObjectBuilder binaryObjectBuilder = binary.builder(typeName);
        schema.fields()
                .forEach(
                        (field) ->
                                binaryObjectBuilder.setField(
                                        field.name(),
                                        this.parse(
                                                struct.get(field), field.schema(), field.name())));

        try {
            return binaryObjectBuilder.build();
        } catch (BinaryObjectException e) {
            log.error("ignite sink createIgniteBinaryObject exception", e);
            throw new ConnectException("ignite sink createIgniteBinaryObject exception", e);
        }
    }

    private Object parseArray(List<?> objs, Schema schema) {

        Schema valSchema = schema.valueSchema();
        List<Object> col = new ArrayList<>(objs.size());
        for (Object o : objs) {
            col.add(this.parse(o, valSchema, "KafkaValue"));
        }

        return col;
    }
}
