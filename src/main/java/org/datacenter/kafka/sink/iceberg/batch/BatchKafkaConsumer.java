package org.datacenter.kafka.sink.iceberg.batch;

/*
 * 此处为Kafka消费者类，用于反序列化Confluent Schema Registry中的Avro数据。
 */

import io.confluent.connect.avro.AvroConverter;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.runtime.InternalSinkRecord;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.SimpleHeaderConverter;
import org.apache.kafka.connect.util.ConnectUtils;
import org.datacenter.kafka.sink.iceberg.IcebergSinkConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class BatchKafkaConsumer {

    private final Logger log = LoggerFactory.getLogger(BatchKafkaConsumer.class);

    AvroConverter keyConverter;
    AvroConverter valueConverter;
    SimpleHeaderConverter headerConverter = new SimpleHeaderConverter();
    String endTimestampString = null;
    Long endTimestamp = -1L;

    public void batchConsumerSinkIceberg(String topic, IcebergSinkConnectorConfig connectorConfig) {

        KafkaConsumer<byte[], byte[]> kafkaConsumer = getKafkaConsumer(connectorConfig);

        HashMap<String, Object> converterConfig = new HashMap<String, Object>() {{
            put("schemas.enable", true);
            put("value.converter", "io.confluent.connect.avro.AvroConverter");
            put("schema.registry.url", "http://slaver:8081");
        }};
        keyConverter = new AvroConverter();
        keyConverter.configure(connectorConfig.values(), true);
        valueConverter = new AvroConverter();
        valueConverter.configure(connectorConfig.values(), false);
        endTimestampString = connectorConfig.getString("endTimestamp");
        endTimestamp = Instant.parse(endTimestampString).toEpochMilli();

        List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(topic);
        Set<TopicPartition> topicPartitionSet = partitionInfos.stream().map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition())).collect(Collectors.toSet());
        Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = kafkaConsumer.committed(topicPartitionSet);
        long consumerOfferset;
        int topicCount = 0;
        int partitionCount;
        for (TopicPartition topicPartition : offsetAndMetadataMap.keySet()) {

            String subscribeString = topicPartition.topic() + "-" + topicPartition.partition();
            OffsetAndMetadata offsetAndMetadata = offsetAndMetadataMap.get(topicPartition);

            boolean isEndPartition = isEndPartition(kafkaConsumer, offsetAndMetadata, topicPartition);
            if (isEndPartition) {
                continue;
            }

            // 订阅主题+分区
            kafkaConsumer.subscribe(List.of(subscribeString));

            consumerOfferset = -1;
            partitionCount = 0;
            boolean isToTimestamp = false;
            do {
                ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(Duration.ofSeconds(180));
                Iterator<ConsumerRecord<byte[], byte[]>> consumerRecordIterator = records.iterator();
                while (consumerRecordIterator.hasNext()) {

                    ConsumerRecord<byte[], byte[]> record = consumerRecordIterator.next();

                    // 如果消息的时间戳大于指定的截止时间点，跳出循环
                    if (record.timestamp() > endTimestamp) {
                        isToTimestamp = true;
                        break;
                    }

                    // 将ConsumerRecord转换成SinkRecord类型
                    SinkRecord transRecord = convertAndTransformRecord(record);
                    consumerOfferset = transRecord.kafkaOffset();
                    partitionCount++;

                    // 处理消息
                    System.out.printf("value = %s, schema = %s \n", transRecord.value(), transRecord.valueSchema());
                }

                isEndPartition = isEndPartition(kafkaConsumer, offsetAndMetadata, topicPartition);

            } while (!isToTimestamp && !isEndPartition);

            if (consumerOfferset > 0) {
                topicCount += partitionCount;
                OffsetAndMetadata nowOffsetAndMetadata = new OffsetAndMetadata(consumerOfferset, offsetAndMetadata.leaderEpoch(), offsetAndMetadata.metadata());
                offsetAndMetadataMap.put(topicPartition, nowOffsetAndMetadata);
                log.info("消费subscribe:{}到达截止时间:{}.本次消费{}条.最后的offerSet为{}", subscribeString, endTimestampString, partitionCount, consumerOfferset);
            } else {
                offsetAndMetadataMap.remove(topicPartition);
                log.info("消费subscribe:{}到达截止时间:{}.没有消费到消息.", subscribeString, endTimestampString);
            }
        }

        // flush iceberg table

        // 提交kafka topic的offerset,超时时间180秒
        kafkaConsumer.commitSync(offsetAndMetadataMap, Duration.ofSeconds(180));

        log.info("消费topic:{}到达截止时间:{}.消费{}条.", topic, endTimestampString, topicCount);
    }

    public KafkaConsumer<byte[], byte[]> getKafkaConsumer(IcebergSinkConnectorConfig connectorConfig) {
        // 创建Kafka消费者配置
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connectorConfig.getString(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, connectorConfig.getString(ConsumerConfig.GROUP_ID_CONFIG));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
//        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100 * 10000);
        // 设置消费者从offset不存在时默认从最开始的位置开始消费
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // 设置消费者的enable.auto.commit参数为false，这样可以手动控制offset
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // 创建Kafka消费者
        return new KafkaConsumer<>(properties);
    }

    /**
     * 检查分区是否已经消费完毕.
     *
     * @param consumer
     * @param offsetAndMetadata
     * @param topicPartition
     * @return
     */
    private boolean isEndPartition(KafkaConsumer<byte[], byte[]> consumer, OffsetAndMetadata offsetAndMetadata, TopicPartition topicPartition) {
        boolean isEndPartition = false;
        long lastOffset;
        long offset;
        // 根据topicPartition获取分区最后一条数据的offset
        lastOffset = -1;
        List<TopicPartition> topicPartitionList = List.of(topicPartition);
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitionList);
        if (endOffsets != null && endOffsets.get(topicPartition) != null) {
            lastOffset = endOffsets.get(topicPartition);
        }

        offset = offsetAndMetadata.offset();
        if (lastOffset == offset) {
            // 已经消费到最后一条数据了.
            isEndPartition = true;
        }
        return isEndPartition;
    }

    private SinkRecord convertAndTransformRecord(final ConsumerRecord<byte[], byte[]> record) {

        SchemaAndValue keyAndSchema = keyConverter.toConnectData(record.topic(), record.headers(), record.key());

        SchemaAndValue valueAndSchema = valueConverter.toConnectData(record.topic(), record.headers(), record.value());

        Headers headers = convertHeadersFor(record);

        Long timestamp = ConnectUtils.checkAndConvertTimestamp(record.timestamp());
        SinkRecord origRecord = new SinkRecord(record.topic(), record.partition(), keyAndSchema.schema(), keyAndSchema.value(), valueAndSchema.schema(), valueAndSchema.value(), record.offset(), timestamp, record.timestampType(), headers);

        return new InternalSinkRecord(record, origRecord);
    }

    private Headers convertHeadersFor(ConsumerRecord<byte[], byte[]> record) {
        Headers result = new ConnectHeaders();
        org.apache.kafka.common.header.Headers recordHeaders = record.headers();
        if (recordHeaders != null) {
            String topic = record.topic();
            for (org.apache.kafka.common.header.Header recordHeader : recordHeaders) {
                SchemaAndValue schemaAndValue = headerConverter.toConnectHeader(topic, recordHeader.key(), recordHeader.value());
                result.add(recordHeader.key(), schemaAndValue);
            }
        }
        return result;
    }
}