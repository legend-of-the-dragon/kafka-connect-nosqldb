package org.datacenter.kafka.source.kafka;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;

public class KafkaConfigs {
    public static final String SRC_CONSUMER_PREFIX = "src.consumer.";

    public static final String DEST_CONSUMER_PREFIX = "dest.consumer.";

    private static final Map<String, ConfigDef.ConfigKey> ALL_CONSUMER_CONFIGS;

    public enum KafkaCluster {
        SOURCE("Source", "src."),
        DESTINATION("Destination", "dest.");

        private final String type;

        private final String prefix;

        KafkaCluster(String type, String prefix) {
            this.type = type;
            this.prefix = prefix;
        }

        public String toString() {
            return this.type;
        }

        public String prefix() {
            return this.prefix;
        }

        public String bootstrapServersConfig() {
            return this.prefix + "bootstrap.servers";
        }
    }

    static {
        try {
            Field configField = ConsumerConfig.class.getDeclaredField("CONFIG");
            configField.setAccessible(true);
            ConfigDef consumerConfigDef = (ConfigDef)configField.get(ConsumerConfig.class);
            ALL_CONSUMER_CONFIGS = new TreeMap<>(consumerConfigDef.configKeys());
        } catch (NoSuchFieldException|IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static void addDefinitions(ConfigDef configDef) {
        defineShared(configDef, KafkaCluster.SOURCE, false);
        defineSharedSecurity(configDef, KafkaCluster.SOURCE);
        defineSharedSecurity(configDef, KafkaCluster.DESTINATION);
        defineConsumer(configDef);
        defineShared(configDef, KafkaCluster.DESTINATION, true);
    }

    private static void defineShared(ConfigDef configDef, KafkaCluster kafka, boolean optional) {
        String group = kafka.toString() + " Kafka";
        int orderInGroup = 0;
        define(configDef, kafka

                        .prefix(), optional ? "" : null,

                baseConfigKey("bootstrap.servers"), group, ++orderInGroup, ConfigDef.Width.NONE, null, null);
        define(configDef, kafka

                        .prefix(),
                baseConfigKey("client.id"), group, ++orderInGroup, ConfigDef.Width.NONE);
        define(configDef, kafka

                        .prefix(),
                baseConfigKey("request.timeout.ms"), group, ++orderInGroup, ConfigDef.Width.MEDIUM);
        define(configDef, kafka

                        .prefix(),
                baseConfigKey("retry.backoff.ms"), group, ++orderInGroup, ConfigDef.Width.MEDIUM);
        define(configDef, kafka

                        .prefix(),
                baseConfigKey("connections.max.idle.ms"), group, ++orderInGroup, ConfigDef.Width.MEDIUM);
        define(configDef, kafka

                        .prefix(),
                baseConfigKey("reconnect.backoff.ms"), group, ++orderInGroup, ConfigDef.Width.MEDIUM);
        define(configDef, kafka

                        .prefix(),
                baseConfigKey("metric.reporters"), group, ++orderInGroup, ConfigDef.Width.MEDIUM);
        define(configDef, kafka

                        .prefix(),
                baseConfigKey("metrics.num.samples"), group, ++orderInGroup, ConfigDef.Width.MEDIUM);
        define(configDef, kafka

                        .prefix(),
                baseConfigKey("metrics.sample.window.ms"), group, ++orderInGroup, ConfigDef.Width.MEDIUM);
        define(configDef, kafka

                        .prefix(),
                baseConfigKey("send.buffer.bytes"), group, ++orderInGroup, ConfigDef.Width.MEDIUM);
        define(configDef, kafka

                        .prefix(),
                baseConfigKey("receive.buffer.bytes"), group, ++orderInGroup, ConfigDef.Width.MEDIUM);
    }

    private static void defineSharedSecurity(ConfigDef configDef, KafkaCluster kafka) {
        String group = kafka.toString() + " Kafka: Security";
        int orderInGroup = 0;
        define(configDef, kafka

                        .prefix(), null,

                baseConfigKey("security.protocol"), group, ++orderInGroup, ConfigDef.Width.NONE,

                (ConfigDef.Validator)ConfigDef.ValidString.in(SecurityProtocolRecommender.VALID_VALUES
                        .<String>toArray(
                                new String[SecurityProtocolRecommender.VALID_VALUES.size()])), new SecurityProtocolRecommender());
        SecurityProtocolSpecificRecommender saslRecommender = new SecurityProtocolSpecificRecommender(kafka.prefix() + "security.protocol", new SecurityProtocol[] { SecurityProtocol.SASL_PLAINTEXT, SecurityProtocol.SASL_SSL });
        for (ConfigDef.ConfigKey saslConfigKey : saslConfigs().values())
            define(configDef, kafka

                    .prefix(), null, saslConfigKey, group, ++orderInGroup, ConfigDef.Width.NONE, null, saslRecommender);
        SecurityProtocolSpecificRecommender sslRecommender = new SecurityProtocolSpecificRecommender(kafka.prefix() + "security.protocol", new SecurityProtocol[] { SecurityProtocol.SASL_SSL, SecurityProtocol.SSL });
        for (ConfigDef.ConfigKey sslConfigKey : sslConfigs().values())
            define(configDef, kafka

                    .prefix(), null, sslConfigKey, group, ++orderInGroup, ConfigDef.Width.NONE, null, sslRecommender);
    }

    private static void defineConsumer(ConfigDef configDef) {
        String group = "Source Kafka: Consumer";
        int orderInGroup = 0;
        define(configDef, "src.consumer.",

                baseConfigKey("interceptor.classes"), "Source Kafka: Consumer", ++orderInGroup, ConfigDef.Width.NONE);
        define(configDef, "src.consumer.",

                baseConfigKey("fetch.max.wait.ms"), "Source Kafka: Consumer", ++orderInGroup, ConfigDef.Width.MEDIUM);
        define(configDef, "src.consumer.",

                baseConfigKey("fetch.min.bytes"), "Source Kafka: Consumer", ++orderInGroup, ConfigDef.Width.MEDIUM);
        define(configDef, "src.consumer.",

                baseConfigKey("fetch.max.bytes"), "Source Kafka: Consumer", ++orderInGroup, ConfigDef.Width.MEDIUM);
        define(configDef, "src.consumer.",

                baseConfigKey("max.partition.fetch.bytes"), "Source Kafka: Consumer", ++orderInGroup, ConfigDef.Width.MEDIUM);
        define(configDef, "src.consumer.",

                baseConfigKey("max.poll.interval.ms"), "Source Kafka: Consumer", ++orderInGroup, ConfigDef.Width.MEDIUM);
        define(configDef, "src.consumer.",

                baseConfigKey("max.poll.records"), "Source Kafka: Consumer", ++orderInGroup, ConfigDef.Width.MEDIUM);
        define(configDef, "src.consumer.",

                baseConfigKey("check.crcs"), "Source Kafka: Consumer", ++orderInGroup, ConfigDef.Width.NONE);
    }

    private static ConfigDef.ConfigKey baseConfigKey(String key) {
        return ALL_CONSUMER_CONFIGS.get(key);
    }

    private static void define(ConfigDef configDef, String prefix, ConfigDef.ConfigKey baseConfigKey, String group, int orderInGroup, ConfigDef.Width width) {
        define(configDef, prefix, null, baseConfigKey, group, orderInGroup, width, null, null);
    }

    private static void define(ConfigDef configDef, String prefix, Object defaultValueOverride, ConfigDef.ConfigKey baseConfigKey, String group, int orderInGroup, ConfigDef.Width width, ConfigDef.Validator validator, ConfigDef.Recommender recommender) {
        configDef.define(prefix + baseConfigKey.name, baseConfigKey.type, (defaultValueOverride == null) ? baseConfigKey.defaultValue : defaultValueOverride, (validator != null) ? validator : baseConfigKey.validator, baseConfigKey.importance,

                htmlToRst(baseConfigKey.documentation), group, orderInGroup, width, ".." + baseConfigKey.name, recommender);
    }

    private static Map<String, ConfigDef.ConfigKey> sslConfigs() {
        ConfigDef cfg = new ConfigDef();
        SslConfigs.addClientSslSupport(cfg);
        return cfg.configKeys();
    }

    private static Map<String, ConfigDef.ConfigKey> saslConfigs() {
        ConfigDef cfg = new ConfigDef();
        SaslConfigs.addClientSaslSupport(cfg);
        return cfg.configKeys();
    }

    private static String htmlToRst(String html) {
        return html.trim().replace("<code>", "``").replace("</code>", "``").replace("&mdash;", " ");
    }

    private static class SecurityProtocolSpecificRecommender implements ConfigDef.Recommender {
        private final String configKey;

        private final Set<String> protocolNames;

        SecurityProtocolSpecificRecommender(String configKey, SecurityProtocol... protocols) {
            this.configKey = configKey;
            this.protocolNames = new HashSet<>(protocols.length);
            for (SecurityProtocol protocol : protocols)
                this.protocolNames.add(protocol.name());
        }

        public List<Object> validValues(String s, Map<String, Object> map) {
            return Collections.emptyList();
        }

        public boolean visible(String s, Map<String, Object> map) {
            return this.protocolNames.contains(map.get(this.configKey));
        }
    }

    private static class SecurityProtocolRecommender implements ConfigDef.Recommender {
        private SecurityProtocolRecommender() {}

        static final List<Object> VALID_VALUES = new ArrayList();

        static {
            for (SecurityProtocol protocol : SecurityProtocol.values())
                VALID_VALUES.add(protocol.name());
        }

        public List<Object> validValues(String s, Map<String, Object> map) {
            return VALID_VALUES;
        }

        public boolean visible(String s, Map<String, Object> map) {
            return true;
        }
    }
}