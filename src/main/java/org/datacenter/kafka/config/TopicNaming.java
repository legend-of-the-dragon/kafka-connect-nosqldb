package org.datacenter.kafka.config; //

public final class TopicNaming {
    private final String topicPrefix;
    private final String tableNamePrefix;

    public TopicNaming(String topicPrefix, String tableNamePrefix) {
        this.topicPrefix = topicPrefix;
        this.tableNamePrefix = tableNamePrefix;
    }

    public String topicName(String cacheName) {
        if (cacheName == null) {
            throw new IllegalArgumentException("cacheName must not be null.");
        } else {
            return this.topicPrefix == null
                    ? cacheName
                    : String.format("%s%s", this.topicPrefix, cacheName);
        }
    }

    public String tableName(String topicName) {

        if (topicName == null) {
            throw new IllegalArgumentException("topicName must not be null.");
        } else {

            String tempTableName =
                    this.topicPrefix != null && topicName.startsWith(this.topicPrefix)
                            ? topicName.substring(this.topicPrefix.length())
                            : topicName;

            if (this.tableNamePrefix != null) {
                return this.tableNamePrefix + tempTableName;
            } else {
                return tempTableName;
            }
        }
    }
}
