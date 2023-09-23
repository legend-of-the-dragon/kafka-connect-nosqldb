package org.datacenter.kafka.sink.iceberg;

import com.google.gson.Gson;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.datacenter.kafka.sink.iceberg.batch.BatchKafkaConsumer;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;


public class IcebergBatchKafkaConsumerTest {

    @Test
    public void testBatchConsumer() throws IOException {

                
        // 配置log4j2.xml，让slf4j可用
        System.setProperty("log4j.configurationFile", "log4j2.xml");
                
        // 启动log4j2
        org.apache.logging.log4j.core.LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) org.apache.logging.log4j.LogManager.getContext(false);
        context.start(context.getConfiguration());



        String[] args = new String[]{"d:/**.json", "2023-05-08 00:00:00", "30m"};
        // 配置文件路径
        String configPath = args[0];
        // 截止时间
        String endTimestamp = args[1];
        // 容错区间Duration
        String faultTolerantDuration = args[2];

        Map<String, String> configMap = getConfigMap(configPath);
        configMap.put("endTimestamp", endTimestamp);
        configMap.put("faultTolerantDuration", faultTolerantDuration);
        IcebergSinkConnectorConfig connectorConfig = new IcebergSinkConnectorConfig(configMap);

        BatchKafkaConsumer batchKafkaConsumer = new BatchKafkaConsumer();
        List<String> topicNameList = getTopicNameList(connectorConfig, batchKafkaConsumer);
        for (String topic : topicNameList) {
            batchKafkaConsumer.batchConsumerSinkIceberg(topic, connectorConfig);
        }
    }

    private static Map<String, String> getConfigMap(String configPath) throws IOException {
        String jsonContent = FileUtils.readFileToString(new File(configPath), StandardCharsets.UTF_8);
        return new Gson().fromJson(jsonContent, Map.class);
    }

    @NotNull
    private static List<String> getTopicNameList(IcebergSinkConnectorConfig connectorConfig, BatchKafkaConsumer batchKafkaConsumer) throws IOException {
        List<String> topicNameList;
        String topics = connectorConfig.getString("topics");
        String topicRegex = connectorConfig.getString("topics.regex");
        if (topics != null) {
            topicNameList = List.of(topics.split(","));
        } else if (topicRegex != null) {
            KafkaConsumer<byte[], byte[]> kafkaConsumer = batchKafkaConsumer.getKafkaConsumer(connectorConfig);
            Map<String, List<PartitionInfo>> listTopics = kafkaConsumer.listTopics(Duration.ofSeconds(180));
            Set<String> topicNameSet = listTopics.keySet();
            topicNameList = new ArrayList<>();
            Pattern pattern = Pattern.compile(topicRegex);
            for (String topicName : topicNameSet) {
                if (pattern.matcher(topicName).matches()) {
                    topicNameList.add(topicName);
                }
            }
        } else {
            throw new IOException("topics 或 topics.regex 需要填写其中之一.");
        }
        return topicNameList;
    }
}
