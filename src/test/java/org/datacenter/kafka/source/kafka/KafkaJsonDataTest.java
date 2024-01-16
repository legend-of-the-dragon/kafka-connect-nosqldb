package org.datacenter.kafka.source.kafka;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

public class KafkaJsonDataTest {

    private static final String TOPIC = "test_json_1";
    private static final String SCHEMA_REGISTRY_URL = "http://10.255.200.92:8081";
    // 定义键（Key）的Schema
    String KEY_SCHEMA =
            "{\"type\":\"object\",\"properties\":{\"key_id\":{\"type\":\"integer\"}},\"required\":[\"key_id\"]}";

    // 定义值（Value）的Schema
    String USER_SCHEMA =
            "{\"type\":\"object\",\"properties\":{"
                    + "\"id\":{\"type\":\"integer\"},"
                    + "\"name\":{\"type\":\"string\"},"
                    + "\"age\":{\"type\":\"integer\"}},"
                    + "\"required\":[\"id\",\"name\",\"age\"]}";

    @Test
    public void testProduceKafkaJsonData() throws IOException, RestClientException {
        Properties properties = new Properties();
        properties.setProperty(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.255.200.92:9092");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "10");
        properties.setProperty(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty("schema.registry.url", SCHEMA_REGISTRY_URL);

        Producer<String, String> producer = new KafkaProducer<>(properties);
        SchemaRegistryClient schemaRegistry =
                new CachedSchemaRegistryClient(SCHEMA_REGISTRY_URL, 100);

        String keySubject = TOPIC + "-key";
        String valueSubject = TOPIC + "-value";

        // Register the schema
        schemaRegistry.register(keySubject, new JsonSchema(KEY_SCHEMA));
        schemaRegistry.register(valueSubject, new JsonSchema(USER_SCHEMA));

        for (int i = 0; i < 10_0000; i++) {
            JSONObject keyJson = new JSONObject();
            keyJson.put("key_id", i);

            JSONObject valueJson = new JSONObject();
            valueJson.put("id", i);
            valueJson.put("name", "name" + i);
            valueJson.put("age", 18);

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(TOPIC, keyJson.toString(), valueJson.toString());
            producer.send(record);
        }
        producer.flush();
        producer.close();
    }
}
