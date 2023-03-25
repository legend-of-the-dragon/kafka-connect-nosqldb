package org.datacenter.kafka.util;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.datacenter.kafka.sink.AbstractConnectorConfig;

import java.util.List;
import java.util.Map;

/**
 * @author sky
 * @date 2022-05-31
 * @discription
 */
public class SinkRecordUtil {

    public static Struct getStructOfConfigMessageExtract(
            Struct struct, AbstractConnectorConfig.MessageExtract messageExtract) {

        if (messageExtract.equals(AbstractConnectorConfig.MessageExtract.SCHEMA_REGISTRY)) {
            return struct;
        } else {
            return struct.getStruct("after");
        }
    }

    public static String schema2String(Schema schema) {
        if (schema == null) {
            return "null";
        } else {

            StringBuilder stringBuilder = new StringBuilder("Schema:");

            parseSchemaObject(schema, stringBuilder);

            return stringBuilder.toString();
        }
    }

    private static void parseSchemaObject(Schema schema, StringBuilder stringBuilder) {

        Schema.Type type = schema.type();
        stringBuilder
                .append("{name:'")
                .append(schema.name())
                .append("',type:'")
                .append(type)
                .append("',version:'")
                .append(schema.version()).append("'");

        Map<String, String> schemaParameters = schema.parameters();
        if (schemaParameters != null) {
            stringBuilder.append(",parameters:{").append(schemaParameters.toString()).append("}");
        }
        if (type == Schema.Type.STRUCT) {
            List<Field> fields = schema.fields();
            if (fields != null && fields.size() > 0) {
                stringBuilder.append(",");
                parseSchemaFields(fields, stringBuilder);
            }
        }
        stringBuilder.append("}");
    }

    private static void parseSchemaFields(List<Field> fields, StringBuilder stringBuilder) {

        stringBuilder.append("fields:[");
        int fieldSize = fields.size();
        for (int i = 0; i < fieldSize; i++) {
            Field field = fields.get(i);
            stringBuilder.append("fieldName:'").append(field.name()).append("',Schema:");
            parseSchemaObject(field.schema(), stringBuilder);
            if (i < fieldSize - 1) {
                stringBuilder.append(",");
            }
        }
        stringBuilder.append("]");
    }
}
