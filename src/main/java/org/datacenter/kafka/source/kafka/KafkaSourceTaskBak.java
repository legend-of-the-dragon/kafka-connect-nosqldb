package org.datacenter.kafka.source.kafka;

import io.confluent.connect.replicator.util.NewReplicatorAdminClient;
import io.confluent.connect.replicator.util.ReplicatorAdminClient;
import io.confluent.connect.replicator.util.Utils;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.StringConverter;
import org.datacenter.kafka.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * KafkaSourceTask
 *
 * @author sky
 * @date 2023-10-10
 */
public final class KafkaSourceTaskBak extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(KafkaSourceTaskBak.class);

    public static final Pattern PROVENANCE_HEADER_PATTERN =
            Pattern.compile("([^,]+),([^,]+),([^,]+)");

    public static final Pattern FILTER_OVERRIDE_PATTERN =
            Pattern.compile("([^,]+),([^,]+),([^-]*)-([^;]*);?");

    private String taskId;

    private Converter sourceKeyConverter;

    private Converter sourceValueConverter;

    DeadlineManager deadlineManager = new DeadlineManager();

    private Consumer<byte[], byte[]> consumer;

    private HeaderConverter headerConverter;

    private KafkaSourceConnectorConfig sourceConfig;

    private ReplicatorAdminClient destAdminClient;

    private List<TopicPartition> assignTopicPartitions;

    public KafkaSourceTaskBak() {}

    public String version() {
        return Version.getVersion();
    }

    public void initialize(SourceTaskContext context) {
        log.debug("Initializing SourceTaskContext for ReplicatorSourceTask");
        super.initialize(context);
        this.context = context;
    }

    public void start(Map<String, String> map) {

        this.sourceConfig = new KafkaSourceConnectorConfig(map);

        this.taskId = map.get("taskId");
        this.consumer = new KafkaConsumer<>(this.sourceConfig.getKafkaConsumerConfig());

        this.sourceKeyConverter = this.sourceConfig.getConverter("src.key.converter", true);

        this.sourceValueConverter = this.sourceConfig.getConverter("src.value.converter", false);

        this.headerConverter = new StringConverter();

        log.debug("Creating destination admin client...");
        Map<String, Object> destAdminClientConfig = sourceConfig.destAdminClientConfig();
        destAdminClientConfig.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        destAdminClientConfig.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        this.destAdminClient = createAdminClient(destAdminClientConfig);

        String srcTopics = sourceConfig.srcTopics;
        String srcTopicPrefix = sourceConfig.srcTopicPrefix;
        try {
            assignTopicPartitions = getTopicNameList(srcTopics, srcTopicPrefix);
            consumer.assign(this.assignTopicPartitions);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Set<String> srcTopicList = new HashSet<>();
        for (TopicPartition topicPartition : assignTopicPartitions) {
            srcTopicList.add(topicPartition.topic());
        }

        int defaultPartitionNum = this.sourceConfig.getDestTopicReplicationParitionNum();
        int partitionNum;
        int defaultFactor = this.sourceConfig.destTopicReplicationFactor;
        KafkaConsumer<byte[], byte[]> destKafkaConsumer =
                new KafkaConsumer<>(destAdminClientConfig);

        Map<String, List<PartitionInfo>> destTopicListMap =
                destKafkaConsumer.listTopics(Duration.ofSeconds(180));
        Properties destDefaultProperties = new Properties();
        destDefaultProperties.put("cleanup.policy", sourceConfig.destTopicCleanupPolicy);

        for (String sourceTopic : srcTopicList) {
            String destTopic = toTargetTopic(sourceTopic, this.sourceConfig);
            boolean topicExists = destTopicListMap.containsKey(destTopic);
            if (!topicExists) {
                try {
                    if (this.sourceConfig.getTopicPreservePartitions()) {
                        Map<String, List<PartitionInfo>> listTopics =
                                consumer.listTopics(Duration.ofSeconds(30));
                        List<PartitionInfo> partitionInfos = listTopics.get(sourceTopic);
                        partitionNum = partitionInfos.size();
                    } else {
                        partitionNum = defaultPartitionNum;
                    }

                    destAdminClient.createTopic(
                            destTopic, partitionNum, (short) defaultFactor, destDefaultProperties);
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }

            Map<String, List<PartitionInfo>> listTopics =
                    consumer.listTopics(Duration.ofSeconds(30));
            List<PartitionInfo> partitionInfos = listTopics.get(sourceTopic);
            partitionNum = partitionInfos.size();
            srcTopicPartitionNumMap.put(sourceTopic, partitionNum);

            listTopics = destKafkaConsumer.listTopics(Duration.ofSeconds(30));
            partitionInfos = listTopics.get(sourceTopic);
            partitionNum = partitionInfos.size();
            destTopicPartitionNumMap.put(destTopic, partitionNum);
        }

        destKafkaConsumer.close();

        log.info("Successfully started up Replicator source task {}", this.taskId);

        log.info("Kafka Source task started");
    }

    Map<String, Integer> srcTopicPartitionNumMap = new HashMap<>();
    Map<String, Integer> destTopicPartitionNumMap = new HashMap<>();

    private List<TopicPartition> getTopicNameList(String topics, String topicRegex)
            throws IOException {

        List<String> topicNameList;
        List<TopicPartition> topicPartitions = new ArrayList<>();

        Map<String, List<PartitionInfo>> listTopics =
                this.consumer.listTopics(Duration.ofSeconds(180));

        if (topics != null) {
            topicNameList = List.of(topics.split(","));
        } else if (topicRegex != null) {
            topicNameList = new ArrayList<>();
            Set<String> topicNameSet = listTopics.keySet();
            Pattern pattern = Pattern.compile(topicRegex);
            for (String topicName : topicNameSet) {
                if (pattern.matcher(topicName).matches()) {
                    topicNameList.add(topicName);
                }
            }
        } else {
            throw new IOException("topics 或 topics.regex 需要填写其中之一.");
        }

        for (String topicName : topicNameList) {
            topicPartitions.addAll(
                    listTopics.get(topicName).stream()
                            .map(
                                    partitionInfo ->
                                            new TopicPartition(
                                                    topicName, partitionInfo.partition()))
                            .collect(Collectors.toList()));
        }

        return topicPartitions;
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {

        log.debug("Beginning poll for Replicator source task {}", this.taskId);
        try {
            ConsumerRecords<byte[], byte[]> records = ConsumerRecords.empty();
            boolean isEndAllPartition;
            int batchSize = sourceConfig.srcMaxPollRecords;
            List<SourceRecord> sourceRecords = new ArrayList<>(batchSize);
            long numPerPartitionRecordsToConnect;
            long count = 0;
            long lastConsumedOffset;
            Map<TopicPartition, Pair<Long, Long>> topicPartitionConsumedInfoMap = new HashMap<>();
            Map<TopicPartition, Long> topicPartitionSubmitInfoMap = new HashMap<>();

            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignTopicPartitions);
            int retry = 0;

            do {
                retry++;
                if (!this.consumer.assignment().isEmpty()) {
                    records = this.consumer.poll(Duration.ofSeconds(2));
                }
                if (log.isDebugEnabled()) {
                    Set<String> topics = topicNamesFor(records);
                    log.debug(
                            "Read {} records from {} topics: {}",
                            records.count(),
                            topics.size(),
                            topics);
                }

                for (TopicPartition topicPartition : records.partitions()) {
                    String sourceTopic = topicPartition.topic();

                    String targetTopic = toTargetTopic(sourceTopic, this.sourceConfig);
                    int partition = topicPartition.partition();
                    Map<String, ?> connectSourcePartition =
                            Utils.toConnectPartition(sourceTopic, partition);

                    numPerPartitionRecordsToConnect = 0L;
                    lastConsumedOffset = -1L;

                    for (ConsumerRecord<byte[], byte[]> record : records.records(topicPartition)) {
                        numPerPartitionRecordsToConnect++;
                        long recordOffset = record.offset();
                        if (recordOffset > lastConsumedOffset) {
                            lastConsumedOffset = recordOffset;
                        }
                        Pair<SchemaAndValue, SchemaAndValue> converted =
                                convertKeyValue(record, targetTopic);
                        Long timestamp = timestampFromRecord(record);
                        Map<String, Object> connectOffset = Utils.toConnectOffset(recordOffset);

                        int destPartition = toDestPartition(topicPartition, this.sourceConfig);

                        ConnectHeaders connectHeaders =
                                toConnectHeaders(sourceTopic, record, this.headerConverter);
                        sourceRecords.add(
                                new SourceRecord(
                                        connectSourcePartition,
                                        connectOffset,
                                        targetTopic,
                                        destPartition,
                                        converted.getKey().schema(),
                                        converted.getKey().value(),
                                        converted.getValue().schema(),
                                        converted.getValue().value(),
                                        timestamp,
                                        connectHeaders));
                    }

                    count += numPerPartitionRecordsToConnect;
                    if (lastConsumedOffset > -1) {
                        topicPartitionConsumedInfoMap.put(
                                topicPartition,
                                Pair.of(lastConsumedOffset, numPerPartitionRecordsToConnect));
                        topicPartitionSubmitInfoMap.put(topicPartition, lastConsumedOffset);
                    }
                }
                if (topicPartitionSubmitInfoMap.size() > 0) {
                    isEndAllPartition = isEndAllPartition(endOffsets, topicPartitionSubmitInfoMap);
                } else {
                    isEndAllPartition = true;
                }
            } while (!isEndAllPartition && count < batchSize && retry < 10);

            if (count == 0) {
                return Collections.emptyList();
            }

            // commit src kafka offset
            Map<TopicPartition, OffsetAndMetadata> commitOffset = new HashMap<>();
            for (TopicPartition topicPartition : topicPartitionSubmitInfoMap.keySet()) {
                commitOffset.put(
                        topicPartition,
                        new OffsetAndMetadata(topicPartitionSubmitInfoMap.get(topicPartition)));
            }
            consumer.commitSync(commitOffset, Duration.ofSeconds(180));

            log.info("poll完成,统计信息如下:{}", topicPartitionConsumedInfoMap);

            return sourceRecords;
        } catch (OffsetOutOfRangeException e) {
            Map<TopicPartition, Long> outOfRangePartitions = e.offsetOutOfRangePartitions();
            log.warn(
                    "Consumer from source cluster detected out of range partitions: {}",
                    outOfRangePartitions);
            return Collections.emptyList();
        } catch (WakeupException e) {
            log.error("Kafka replicator task {} woken up", this.taskId);
            return Collections.emptyList();
        }
    }

    private boolean isEndAllPartition(
            Map<TopicPartition, Long> lastOffsets, Map<TopicPartition, Long> consumerOffsets) {

        boolean isEndPartition = true;
        Long consumerOffset;
        Long lastOffset;
        for (TopicPartition topicPartition : lastOffsets.keySet()) {

            consumerOffset = consumerOffsets.get(topicPartition);
            lastOffset = lastOffsets.get(topicPartition);
            if (consumerOffset == null || lastOffset == null) {
                continue;
            }
            if (consumerOffset < lastOffset) {
                // 还未消费到最后一条数据.
                isEndPartition = false;
                break;
            }
        }
        return isEndPartition;
    }

    private Pair<SchemaAndValue, SchemaAndValue> convertKeyValue(
            ConsumerRecord<byte[], byte[]> record, String targetTopic) {

        String sourceTopic = record.topic();
        SchemaAndValue key = this.sourceKeyConverter.toConnectData(sourceTopic, record.key());
        Schema keySchema = key.schema();
        SchemaBuilder keySchemaBuilder = new SchemaBuilder(keySchema.type());
        keySchemaBuilder.name(targetTopic + "-Key");
        keySchemaBuilder.version(keySchema.version());
        keySchemaBuilder.doc(keySchema.doc());
        Schema newKeySchema = keySchemaBuilder.build();
        SchemaAndValue newKey = new SchemaAndValue(newKeySchema, key.value());

        SchemaAndValue value = this.sourceValueConverter.toConnectData(sourceTopic, record.value());
        Schema valueSchema = value.schema();
        SchemaBuilder valueSchemaBuilder = new SchemaBuilder(keySchema.type());
        valueSchemaBuilder.name(targetTopic + "-Value");
        valueSchemaBuilder.version(valueSchema.version());
        valueSchemaBuilder.doc(valueSchema.doc());
        Schema newValueSchema = valueSchemaBuilder.build();
        SchemaAndValue newValue = new SchemaAndValue(newValueSchema, value.value());

        return Pair.of(newKey, newValue);
        //        return Pair.of(key, value);
    }

    private ReplicatorAdminClient createAdminClient(Map<String, Object> adminClientConfig) {
        Time time = this.deadlineManager.getTime();
        return new NewReplicatorAdminClient(adminClientConfig, time, 30000L, this.taskId);
    }

    private static Set<String> topicNamesFor(ConsumerRecords<byte[], byte[]> records) {
        return records.isEmpty()
                ? Collections.emptySet()
                : records.partitions().stream()
                        .map(TopicPartition::topic)
                        .collect(Collectors.toSet());
    }

    private static Long timestampFromRecord(ConsumerRecord<byte[], byte[]> record) {
        Long timestamp;
        if (record.timestamp() >= 0L) {
            timestamp = record.timestamp();
        } else if (record.timestamp() == -1L) {
            timestamp = null;
        } else {
            throw new CorruptRecordException(
                    String.format("Invalid Record timestamp: %d", record.timestamp()));
        }
        return timestamp;
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

    public static int toDestPartition(
            TopicPartition sourcePartition, KafkaSourceConnectorConfig config) {

        if (config.getTopicPreservePartitions()) {
            return sourcePartition.partition();
        } else {
            return 0;
        }
    }

    private static String toTargetTopic(String sourceTopic, KafkaSourceConnectorConfig config) {
        return Utils.renameTopic(config.getTopicRenameFormat(), sourceTopic);
    }

    @Override
    public void stop() {
        log.info("Kafka source task will stop.");
        log.info("Closing kafka replicator task {}", this.taskId);
        if (this.consumer != null) {
            this.consumer.commitSync();
            this.consumer.close(Duration.ofSeconds(180));
        }
        log.info("Kafka source task stop.");
    }
}
