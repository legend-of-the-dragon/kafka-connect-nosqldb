package org.datacenter.kafka.sink.iceberg;

import com.google.common.collect.Sets;
import org.apache.iceberg.*;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.TypeUtil;
import org.datacenter.kafka.sink.exception.DbDmlException;

import java.io.IOException;
import java.util.List;

public abstract class BaseDeltaTaskWriter extends BaseTaskWriter<Record> {

    private final Schema schema;
    private final Schema deleteSchema;
    private final InternalRecordWrapper wrapper;

    private final InternalRecordWrapper keyWrapper;

    public BaseDeltaTaskWriter(
            PartitionSpec spec,
            FileFormat format,
            FileAppenderFactory<Record> appenderFactory,
            OutputFileFactory fileFactory,
            FileIO io,
            long targetFileSize,
            Schema schema,
            List<Integer> equalityFieldIds) {
        super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
        this.schema = schema;
        this.deleteSchema = TypeUtil.select(schema, Sets.newHashSet(equalityFieldIds));
        this.wrapper = new InternalRecordWrapper(schema.asStruct());
        this.keyWrapper = new InternalRecordWrapper(deleteSchema.asStruct());
    }

    abstract RowDataDeltaWriter route(Record row);

    InternalRecordWrapper wrapper() {
        return wrapper;
    }

    @Override
    public void write(Record record) throws IOException {
        RowDataDeltaWriter writer = route(record);
        SinkRecord sinkRecord = (SinkRecord) record;
        int op = sinkRecord.getOp();
        // upsert
        if (op == 2) {
            writer.delete(sinkRecord);
            writer.write(sinkRecord);
        }
        // delete
        else if (op == 0) {
            writer.deleteKey(sinkRecord);
        }
        // insert
        else if (op == 1) {
            writer.write(sinkRecord);
        } else {
            throw new DbDmlException("非法op:" + op);
        }
    }

    public class RowDataDeltaWriter extends BaseEqualityDeltaWriter {

        RowDataDeltaWriter(PartitionKey partition) {
            super(partition, schema, deleteSchema);
        }

        @Override
        protected StructLike asStructLike(Record data) {
            return wrapper.wrap(data);
        }

        @Override
        protected StructLike asStructLikeKey(Record data) {
            return keyWrapper.wrap(data);
        }
    }
}
