package org.datacenter.kafka.util;

import org.apache.kafka.connect.data.Struct;
import org.datacenter.kafka.config.kudu.KuduSinkConnectorConfig;

/**
 * @author sky
 * @date 2022-05-
 * @discription
 */
public class SinkRecordUtil {

    public static Struct getStructOfConfigMessageExtract(
            Struct struct, KuduSinkConnectorConfig.MessageExtract messageExtract) {

        if (messageExtract.equals(KuduSinkConnectorConfig.MessageExtract.SCHEMA_REGISTRY)) {
            return struct;
        } else {
            return struct.getStruct("after");
        }
    }
}
