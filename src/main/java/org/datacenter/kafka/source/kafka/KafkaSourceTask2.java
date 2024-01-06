package org.datacenter.kafka.source.kafka;

import io.confluent.connect.replicator.util.ReplicatorAdminClient;
import io.confluent.connect.replicator.util.Utils;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class KafkaSourceTask2 extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(KafkaSourceTask2.class);

    // Kafka消费者，用于从源Kafka集群中拉取数据
    private Consumer<byte[], byte[]> consumer;

    // Kafka连接配置，包含了连接源Kafka集群和目的地Kafka集群的配置信息
    private KafkaSourceConnectorConfig sourceConfig;

    // Kafka管理客户端，用于管理目的地Kafka集群的主题和分区
    private ReplicatorAdminClient destAdminClient;

    // 任务ID，用于标识当前的Kafka Connect任务
    private String taskId;

    // 源Kafka集群中的主题和分区信息
    private List<TopicPartition> assignTopicPartitions;

    // 用于转换源Kafka集群中的数据的key和value的转换器
    private Converter sourceKeyConverter;
    private Converter sourceValueConverter;

    // 用于记录每个分区已消费的最后偏移量和提交的偏移量
    private Map<TopicPartition, Long> lastConsumedOffsets;
    private Map<TopicPartition, OffsetAndMetadata> commitOffsets;

    // 分区数量映射，记录源Kafka集群和目的地Kafka集群中每个主题的分区数量
    private Map<String, Integer> srcTopicPartitionNumMap;
    private Map<String, Integer> destTopicPartitionNumMap;

    // 死信处理器，用于处理无法转发的消息
    private DeadlineManager deadlineManager;

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public void start(Map<String, String> props) {
        try {
            this.sourceConfig = new KafkaSourceConnectorConfig(props);
            this.taskId = props.getOrDefault("taskId", "default_task_id");
            this.consumer = createConsumer(this.sourceConfig.getKafkaConsumerConfig());
            this.sourceKeyConverter = this.sourceConfig.getConverter("src.key.converter", true);
            this.sourceValueConverter =
                    this.sourceConfig.getConverter("src.value.converter", false);

            // 订阅主题
            this.assignTopicPartitions = getAssignedTopicPartitions();
            this.consumer.assign(this.assignTopicPartitions);

            this.lastConsumedOffsets = new HashMap<>();
            this.commitOffsets = new HashMap<>();
            this.srcTopicPartitionNumMap = new HashMap<>();
            this.destTopicPartitionNumMap = new HashMap<>();
            log.info("Kafka Source Task started with task ID: {}", this.taskId);
        } catch (ConfigException e) {
            throw new ConnectException(
                    "Couldn't start KafkaSourceTask due to configuration error", e);
        }
    }

    private List<TopicPartition> getAssignedTopicPartitions() {
        // 获取配置中指定的topic正则表达式
        Pattern topicPattern = Pattern.compile(sourceConfig.srcTopicPrefix);

        // 查询Kafka集群以获取所有topic的信息
        Map<String, List<PartitionInfo>> topicInfoMap = consumer.listTopics();

        // 过滤出匹配正则表达式的topics
        return topicInfoMap.entrySet().stream()
                .filter(entry -> topicPattern.matcher(entry.getKey()).matches())
                .flatMap(entry -> entry.getValue().stream())
                .map(info -> new TopicPartition(info.topic(), info.partition()))
                .collect(Collectors.toList());
    }

    private KafkaConsumer<byte[], byte[]> createConsumer(Properties consumerProps) {
        return new KafkaConsumer<>(consumerProps);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {

        // 定义返回的SourceRecord列表
        List<SourceRecord> records = new ArrayList<>();

        // 使用Kafka消费者拉取数据
        ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(Duration.ofMillis(200));

        // 遍历所有拉取到的记录
        for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
            // 转换为SourceRecord对象
            SourceRecord sourceRecord = convertRecord(record);
            if (sourceRecord != null) {
                records.add(sourceRecord);
            }
        }

        // 返回转换后的SourceRecord列表
        return records;
    }

    private SourceRecord convertRecord(ConsumerRecord<byte[], byte[]> record) {
        // 获取topic目标名称
        String sourceTopic = record.topic();
        String targeTopic = toDestTopic(sourceTopic, sourceConfig.topicRenameFormat);

        // 构建SourceRecord的key和value
        SchemaAndValue key;
        SchemaAndValue value;
        key = this.sourceKeyConverter.toConnectData(sourceTopic, record.key());
        value = this.sourceValueConverter.toConnectData(sourceTopic, record.value());

        int destPartition = toDestPartition(new TopicPartition(sourceTopic,record.partition()), this.sourceConfig);
        Map<String, Object> connectOffset = Utils.toConnectOffset(record.offset());

//        ConnectHeaders connectHeaders =
//                toConnectHeaders(sourceTopic, record, this.converter);

        // 构建SourceRecord对象并返回
//        return new SourceRecord(
//                sourcePartition(record),
//                connectOffset,
//                targeTopic,
//                null,
//                keySchema,
//                key,
//                valueSchema,
//                value);

        return null;
    }

    public static int toDestPartition(
            TopicPartition sourcePartition, KafkaSourceConnectorConfig config) {

        if (config.getTopicPreservePartitions()) {
            return sourcePartition.partition();
        } else {
            return 0;
        }
    }

    private static ConnectHeaders toConnectHeaders(
            String sourceTopic, ConsumerRecord<byte[], byte[]> record, HeaderConverter converter) {
        log.trace("Creating a Connect header for new Source Record...");
        Headers headers = record.headers();
        ConnectHeaders connectHeaders = new ConnectHeaders();
        if (headers != null)
            for (Header origHeader : headers) {
                connectHeaders.add(
                        origHeader.key(),
                        converter.toConnectHeader(
                                sourceTopic, origHeader.key(), origHeader.value()));
            }
        return connectHeaders;
    }

    private static String toDestTopic(String sourceTopic, String topicRenameFormat) {
        return Utils.renameTopic(topicRenameFormat, sourceTopic);
    }

    @Override
    public void stop() {
        log.info("Stopping Kafka source task.");
        closeConsumer();
    }

    private void closeConsumer() {
        if (this.consumer != null) {
            try {
                this.consumer.close(Duration.ofSeconds(10));
            } catch (Exception e) {
                log.warn("Exception while closing Kafka consumer: ", e);
            }
        }
    }
}
