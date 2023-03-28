package org.datacenter.kafka.sink.iceberg;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class IcebergTableWriterFactory {

    private static final Logger log = LoggerFactory.getLogger(IcebergTableWriterFactory.class);

    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);

    private final IcebergSinkConnectorConfig configuration;

    public IcebergTableWriterFactory(IcebergSinkConnectorConfig configuration) {
        this.configuration = configuration;
    }

    public BaseTaskWriter<Record> create(Table icebergTable, int taskId) {

        FileFormat format = IcebergUtil.getTableFileFormat(icebergTable);
        GenericAppenderFactory appenderFactory = IcebergUtil.getTableAppender(icebergTable);
        int partitionId = 1;
        OutputFileFactory fileFactory =
                OutputFileFactory.builderFor(icebergTable, partitionId, taskId)
                        .defaultSpec(icebergTable.spec())
                        .format(format)
                        .build();
        List<Integer> equalityFieldIds =
                new ArrayList<>(icebergTable.schema().identifierFieldIds());

        BaseTaskWriter<Record> writer = null;
        if (icebergTable.schema().identifierFieldIds().isEmpty()) {
            if (icebergTable.spec().isUnpartitioned()) {
                writer =
                        new UnpartitionedWriter<>(
                                icebergTable.spec(),
                                format,
                                appenderFactory,
                                fileFactory,
                                icebergTable.io(),
                                Long.MAX_VALUE);
            } else {
                //                writer = new PartitionedAppendWriter(
                //                        icebergTable.spec(), format, appenderFactory, fileFactory,
                // icebergTable.io(), Long.MAX_VALUE, icebergTable.schema());
            }
        } else if (icebergTable.spec().isUnpartitioned()) {
            writer =
                    new UnpartitionedDeltaWriter(
                            icebergTable.spec(),
                            format,
                            appenderFactory,
                            fileFactory,
                            icebergTable.io(),
                            Long.MAX_VALUE,
                            icebergTable.schema(),
                            equalityFieldIds);
        } else {
            writer =
                    new PartitionedDeltaWriter(
                            icebergTable.spec(),
                            format,
                            appenderFactory,
                            fileFactory,
                            icebergTable.io(),
                            Long.MAX_VALUE,
                            icebergTable.schema(),
                            equalityFieldIds);
        }

        return writer;
    }
}
