package org.datacenter.kafka.source.kafka;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaAvroDataTest {
    private static final String TOPIC = "test12";
    private static final String SCHEMA_REGISTRY_URL = "http://10.255.200.92:8081";
    private static final String USER_SCHEMA =
            "{\"type\":\"record\","
                    + "\"name\":\"myUser\","
                    + "\"fields\":[{\"name\":\"id\",\"type\":\"int\"},"
                    + "{\"name\":\"name\",\"type\":\"string\"},"
                    + "{\"name\":\"age\",\"type\":\"int\"}]}";
    private static final String KEY_SCHEMA =
            "{\"type\":\"record\","
                    + "\"name\":\"Key\","
                    + "\"fields\":[{\"name\":\"key_id\",\"type\":\"int\"}]}";

    @Test
    public void testProductKafkaAvroData() throws RestClientException, IOException {

        Properties properties = new Properties();
        properties.setProperty(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.255.200.92:9092");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "10");
        properties.setProperty(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", SCHEMA_REGISTRY_URL);

        Schema.Parser parser = new Schema.Parser();
        Schema keySchema = parser.parse(KEY_SCHEMA);
        Schema valueSchema = parser.parse(USER_SCHEMA);
        ParsedSchema keyParsedSchema = new AvroSchema(keySchema);
        ParsedSchema valueParsedSchema = new AvroSchema(valueSchema);
        Producer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(properties);

        SchemaRegistryClient schemaRegistry =
                new CachedSchemaRegistryClient(SCHEMA_REGISTRY_URL, 100);
        try {
            schemaRegistry.getAllVersions(TOPIC + "-key");
            schemaRegistry.getAllVersions(TOPIC + "-value");
        } catch (Exception e) {
            System.out.println("schema 不存在，现在注册新的schema.");
            schemaRegistry.register(TOPIC + "-key", keyParsedSchema);
            schemaRegistry.register(TOPIC + "-value", valueParsedSchema);
        }

        int count = 0;
        GenericRecord avroKey;
        GenericRecord avroValue;
        ProducerRecord<GenericRecord, GenericRecord> record;
        for (int i = 0; i < 10_0000; i++) {
            avroKey = new GenericData.Record(keySchema);
            avroKey.put("key_id", i);

            avroValue = new GenericData.Record(valueSchema);
            avroValue.put("id", i);
            avroValue.put("name", "name" + i);
            avroValue.put("age", 18);

            record = new ProducerRecord<>(TOPIC, avroKey, avroValue);
            producer.send(record);
            count++;
        }
        producer.flush();
        producer.close();

        System.out.println("count: " + count);
    }

    @Test
    public void testKafkaStringMessage() {

        // Kafka生产者配置
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.255.200.92:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 创建Kafka生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // 发送消息的主题
        String topic = "test10";

        // 构造消息
        for (int i = 0; i < 100_0000; i++) {
            // 模拟生成JSON数据
            String key = String.format("{\"key_id\":%d}", i);
            String value =
                    String.format(
                            "{\"id\":%d,\"name\":\"test%d\",\"status\":%b}", i, i, i % 2 == 0);

            // 创建ProducerRecord
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            // 发送消息并注册回调
            producer.send(
                    record,
                    (metadata, exception) -> {
                        if (exception != null) {
                            exception.printStackTrace();
                        } else {
                            System.out.printf(
                                    "Sent message to topic %s partition %d offset %d%n",
                                    metadata.topic(), metadata.partition(), metadata.offset());
                        }
                    });
        }

        // 发送完所有消息后关闭生产者
        producer.close();
    }

    @Test
    public void testKafkaAvroConsumerExample() {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.255.200.92:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                KafkaAvroDeserializer.class.getName());
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                KafkaAvroDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(
                KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "http://10.255.200.92:8081");
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);

        Consumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);

        try {
            consumer.subscribe(Collections.singletonList("test12"));

            while (true) {
                ConsumerRecords<String, GenericRecord> records =
                        consumer.poll(Duration.ofMillis(100));
                records.forEach(
                        record -> {
                            System.out.printf(
                                    "offset = %d, key = %s, value = %s%n",
                                    record.offset(), record.key(), record.value());
                        });
            }
        } finally {
            consumer.close();
        }
    }
}
