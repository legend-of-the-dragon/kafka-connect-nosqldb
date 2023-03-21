package org.datacenter.kafka.sink.iceberg;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;

import java.io.IOException;
import java.util.List;

public class UnpartitionedDeltaWriter extends BaseDeltaTaskWriter {
    private final RowDataDeltaWriter writer;

    public UnpartitionedDeltaWriter(PartitionSpec spec,
                             FileFormat format,
                             FileAppenderFactory<Record> appenderFactory,
                             OutputFileFactory fileFactory,
                             FileIO io,
                             long targetFileSize,
                             Schema schema,
                             List<Integer> equalityFieldIds) {
        super(spec, format, appenderFactory, fileFactory, io, targetFileSize, schema, equalityFieldIds);
        this.writer = new RowDataDeltaWriter(null);
    }

    @Override
    RowDataDeltaWriter route(Record row) {
        return writer;
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
