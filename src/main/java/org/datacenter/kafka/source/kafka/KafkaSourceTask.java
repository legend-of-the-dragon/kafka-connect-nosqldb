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
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.StringConverter;
import org.datacenter.kafka.Version;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public final class KafkaSourceTask extends SourceTask {

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

    private HeaderConverter converter = new StringConverter();

    private KafkaSourceConnectorConfig sourceConfig;

    private ReplicatorAdminClient destAdminClient;

    private List<TopicPartition> assignTopicPartitions;

    public KafkaSourceTask() {}

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

        log.debug("Creating destination admin client...");
        Map<String, Object> destAdminClientConfig = sourceConfig.destAdminClientConfig();
        destAdminClientConfig.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        destAdminClientConfig.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        this.destAdminClient = createAdminClient(destAdminClientConfig);

        String srcTopics = sourceConfig.srcTopics;
        String srcTopicPrefix = sourceConfig.srcTopicPrefix;
        assignTopicPartitions = getTopicNameList(srcTopics, srcTopicPrefix);
        consumer.assign(this.assignTopicPartitions);

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
            String destTopic = toDestTopic(sourceTopic, this.sourceConfig);
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

            // 获取源topic的分区数
            Map<String, List<PartitionInfo>> listTopics =
                    consumer.listTopics(Duration.ofSeconds(30));
            List<PartitionInfo> partitionInfos = listTopics.get(sourceTopic);
            partitionNum = partitionInfos.size();
            srcTopicPartitionNumMap.put(sourceTopic, partitionNum);

            // 获取目标topic的分区数
            listTopics = destKafkaConsumer.listTopics(Duration.ofSeconds(30));
            partitionInfos = listTopics.get(destTopic);
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
            throws ConnectException {

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
            throw new ConnectException("topics 或 topics.regex 需要填写其中之一.");
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
    public List<SourceRecord> poll() throws ConnectException {

        int countPoll = 0;
        try {
            ConsumerRecords<byte[], byte[]> records = ConsumerRecords.empty();
            boolean isEndAllPartition;
            int batchSize = sourceConfig.srcMaxPollRecords;
            List<SourceRecord> sourceRecords = new ArrayList<>(batchSize);
            int numPerPartitionRecordsToConnect;
            int count = 0;
            long lastConsumedOffset;
            Map<TopicPartition, Pair<Long, Integer>> topicPartitionConsumedInfoMap =
                    new HashMap<>();
            Map<TopicPartition, Long> topicPartitionSubmitInfoMap = new HashMap<>();

            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignTopicPartitions);
            int retry = 0;

            do {
                retry++;
                if (!this.consumer.assignment().isEmpty()) {
                    records = this.consumer.poll(Duration.ofSeconds(2));
                    countPoll += records.count();
                }

                for (TopicPartition topicPartition : records.partitions()) {
                    String sourceTopic = topicPartition.topic();

                    String destTopic = toDestTopic(sourceTopic, this.sourceConfig);
                    int partition = topicPartition.partition();
                    Map<String, ?> connectSourcePartition =
                            Utils.toConnectPartition(sourceTopic, partition);

                    numPerPartitionRecordsToConnect = 0;
                    lastConsumedOffset = -1L;

                    for (ConsumerRecord<byte[], byte[]> record : records.records(topicPartition)) {
                        numPerPartitionRecordsToConnect++;
                        long recordOffset = record.offset();
                        if (recordOffset > lastConsumedOffset) {
                            lastConsumedOffset = recordOffset;
                        }

                        SourceRecord targetSourceRecord =
                                getTargetSourceRecord(
                                        topicPartition,
                                        destTopic,
                                        connectSourcePartition,
                                        record,
                                        recordOffset);
                        sourceRecords.add(targetSourceRecord);
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

            log.info(
                    "poll完成,拉取数量:{},获得sourceRecords数量:{},统计信息如下:{}",
                    countPoll,
                    sourceRecords.size(),
                    topicPartitionConsumedInfoMap);

            return sourceRecords;
        } catch (OffsetOutOfRangeException e) {
            Map<TopicPartition, Long> outOfRangePartitions = e.offsetOutOfRangePartitions();
            log.error(
                    "Consumer from source cluster detected out of range partitions: {}",
                    outOfRangePartitions);
            throw new ConnectException("poll 异常.", e);
        } catch (WakeupException e) {
            log.error("Kafka replicator task {} woken up", this.taskId);
            throw new ConnectException("poll 异常.", e);
        }
    }

    private SourceRecord getTargetSourceRecord(
            TopicPartition topicPartition,
            String destTopic,
            Map<String, ?> connectSourcePartition,
            ConsumerRecord<byte[], byte[]> record,
            long recordOffset) {

        String sourceTopic = record.topic();

        // 转换key和keySchema
        SchemaAndValue key = this.sourceKeyConverter.toConnectData(sourceTopic, record.key());
        SchemaAndValue newKey = getNewSchemaAndValue(key, destTopic, ".Key");

        // 转换value和valueSchema
        Schema newValueSchema = null;
        Object newValueObject = null;
        if (record.value() != null) {
            SchemaAndValue value = this.sourceValueConverter.toConnectData(sourceTopic, record.value());
            SchemaAndValue newSchemaAndValue = getNewSchemaAndValue(value, destTopic, ".Value");
            newValueSchema = newSchemaAndValue.schema();
            newValueObject = newSchemaAndValue.value();
        } else {
            log.debug("record is delete");
        }

        // 转换timestamp
        Long timestamp = timestampFromRecord(record);
        Map<String, Object> connectOffset = Utils.toConnectOffset(recordOffset);

        //  转换partition
        int destPartition = toDestPartition(topicPartition, this.sourceConfig);

        // 转换header
        ConnectHeaders connectHeaders = toConnectHeaders(sourceTopic, record, this.converter);

        // 重新构造Record
        return new SourceRecord(
                connectSourcePartition,
                connectOffset,
                destTopic,
                destPartition,
                newKey.schema(),
                newKey.value(),
                newValueSchema,
                newValueObject,
                timestamp,
                connectHeaders);
    }

    @NotNull
    private static SchemaAndValue getNewSchemaAndValue(
            SchemaAndValue schemaAndValue, String destTopic, String schemaNameSuffix) {
        Schema schema = schemaAndValue.schema();
        Schema newSchema = getNewSchema(schema, destTopic, schemaNameSuffix);
        Struct value = (Struct) schemaAndValue.value();
        Struct newValue = getNewStruct(schema, newSchema, value);
        return new SchemaAndValue(newSchema, newValue);
    }

    @NotNull
    private static Struct getNewStruct(Schema keySchema, Schema newKeySchema, Struct keyStruct) {
        Struct newKeyStruct = new Struct(newKeySchema);
        for (Field field : keySchema.fields()) {
            newKeyStruct.put(field.name(), keyStruct.get(field.name()));
        }
        return newKeyStruct;
    }

    private static Schema getNewSchema(
            Schema valueSchema, String destTopic, String schemaNameSuffix) {
        Schema.Type schemaType = valueSchema.type();
        if (schemaType == null) {
            schemaType = Schema.Type.STRUCT;
        } else if (schemaType != Schema.Type.STRUCT) {
            schemaType = Schema.Type.STRUCT;
        }
        SchemaBuilder valueSchemaBuilder = new SchemaBuilder(schemaType);
        valueSchemaBuilder.name(valueSchema.name());
        for (Field field : valueSchema.fields()) {
            valueSchemaBuilder.field(field.name(), field.schema());
        }
        boolean optional2 = valueSchema.isOptional();
        if (optional2) {
            valueSchemaBuilder.optional();
        }
        valueSchemaBuilder.version(valueSchema.version());
        valueSchemaBuilder.doc(valueSchema.doc());
        valueSchemaBuilder.parameter("namespace", destTopic);
        valueSchemaBuilder.parameter("connect.name", destTopic + schemaNameSuffix);
        Schema newValueSchema = valueSchemaBuilder.build();
        return newValueSchema;
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

    private static String toDestTopic(String sourceTopic, KafkaSourceConnectorConfig config) {
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
