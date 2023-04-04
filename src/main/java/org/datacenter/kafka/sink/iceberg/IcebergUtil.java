/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package org.datacenter.kafka.sink.iceberg;

import com.google.common.primitives.Ints;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

import static org.apache.iceberg.TableProperties.*;

/** @author Ismail Simsek */
public class IcebergUtil {
    protected static final Logger LOGGER = LoggerFactory.getLogger(IcebergUtil.class);

    public static Table createIcebergTable(
            Catalog icebergCatalog,
            TableIdentifier tableIdentifier,
            Schema schema,
            IcebergSinkConnectorConfig icebergSinkConnectorConfig) {

        LOGGER.info(
                "Creating table:'{}'\nschema:{}\nrowIdentifier:{}",
                tableIdentifier,
                schema,
                schema.identifierFieldNames());

        final PartitionSpec ps;
        if (schema.findField("__source_ts") != null) {
            ps = PartitionSpec.builderFor(schema).day("__source_ts").build();
        } else {
            ps = PartitionSpec.builderFor(schema).build();
        }

        return icebergCatalog
                .buildTable(tableIdentifier, schema)
                .withProperties(icebergSinkConnectorConfig.getIcebergTableConfiguration())
                .withProperty(FORMAT_VERSION, "2")
                .withSortOrder(IcebergUtil.getIdentifierFieldsAsSortOrder(schema))
                .withPartitionSpec(ps)
                .create();
    }

    public static SortOrder getIdentifierFieldsAsSortOrder(Schema schema) {
        SortOrder.Builder sob = SortOrder.builderFor(schema);
        for (String fieldName : schema.identifierFieldNames()) {
            sob = sob.asc(fieldName);
        }

        return sob.build();
    }

    public static Table loadIcebergTable(Catalog icebergCatalog, TableIdentifier tableId) {
        try {
            return icebergCatalog.loadTable(tableId);
        } catch (NoSuchTableException e) {
            LOGGER.info("Table not found: {}", tableId.toString());
            return null;
        }
    }

    public static FileFormat getTableFileFormat(Table icebergTable) {
        String formatAsString =
                icebergTable
                        .properties()
                        .getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
        return FileFormat.valueOf(formatAsString.toUpperCase(Locale.ROOT));
    }

    public static GenericAppenderFactory getTableAppender(Table icebergTable) {
        return new GenericAppenderFactory(
                        icebergTable.schema(),
                        icebergTable.spec(),
                        Ints.toArray(icebergTable.schema().identifierFieldIds()),
                        icebergTable.schema(),
                        null)
                .setAll(icebergTable.properties());
    }
}
