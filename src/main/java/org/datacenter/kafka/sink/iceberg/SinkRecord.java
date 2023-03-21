package org.datacenter.kafka.sink.iceberg;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SinkRecord implements Record, StructLike {

    private static final LoadingCache<Types.StructType, Map<String, Integer>> NAME_MAP_CACHE =
            Caffeine.newBuilder()
                    .weakKeys()
                    .build(
                            struct -> {
                                Map<String, Integer> idToPos = Maps.newHashMap();
                                List<Types.NestedField> fields = struct.fields();
                                for (int i = 0; i < fields.size(); i += 1) {
                                    idToPos.put(fields.get(i).name(), i);
                                }
                                return idToPos;
                            });

    public static SinkRecord create(Schema schema) {
        return new SinkRecord(schema.asStruct());
    }

    public static SinkRecord create(Types.StructType struct) {
        return new SinkRecord(struct);
    }

    private int op;
    private final Types.StructType struct;
    private final int size;
    private final Object[] values;
    private final Map<String, Integer> nameToPos;

    public SinkRecord(Types.StructType struct) {
        this.struct = struct;
        this.size = struct.fields().size();
        this.values = new Object[size];
        this.nameToPos = NAME_MAP_CACHE.get(struct);
    }

    public SinkRecord(SinkRecord toCopy) {
        this.struct = toCopy.struct;
        this.size = toCopy.size;
        this.values = Arrays.copyOf(toCopy.values, toCopy.values.length);
        this.nameToPos = toCopy.nameToPos;
    }

    public SinkRecord(SinkRecord toCopy, Map<String, Object> overwrite) {
        this.struct = toCopy.struct;
        this.size = toCopy.size;
        this.values = Arrays.copyOf(toCopy.values, toCopy.values.length);
        this.nameToPos = toCopy.nameToPos;
        for (Map.Entry<String, Object> entry : overwrite.entrySet()) {
            setField(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public Types.StructType struct() {
        return struct;
    }

    @Override
    public Object getField(String name) {
        Integer pos = nameToPos.get(name);
        if (pos != null) {
            return values[pos];
        }

        return null;
    }

    @Override
    public void setField(String name, Object value) {
        Integer pos = nameToPos.get(name);
        Preconditions.checkArgument(pos != null, "Cannot set unknown field named: %s", name);
        values[pos] = value;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public Object get(int pos) {
        return values[pos];
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
        Object value = get(pos);
        if (value == null || javaClass.isInstance(value)) {
            return javaClass.cast(value);
        } else {
            throw new IllegalStateException(
                    "Not an instance of " + javaClass.getName() + ": " + value);
        }
    }

    public int getOp() {
        return op;
    }

    public void setOp(int op) {
        this.op = op;
    }

    @Override
    public <T> void set(int pos, T value) {
        values[pos] = value;
    }

    @Override
    public SinkRecord copy() {
        return new SinkRecord(this);
    }

    @Override
    public SinkRecord copy(Map<String, Object> overwriteValues) {
        return new SinkRecord(this, overwriteValues);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Record(");
        for (int i = 0; i < values.length; i += 1) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(values[i]);
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (!(other instanceof SinkRecord)) {
            return false;
        }

        SinkRecord that = (SinkRecord) other;
        return Arrays.deepEquals(this.values, that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(values);
    }
}
