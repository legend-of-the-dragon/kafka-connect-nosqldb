package org.datacenter.kafka; //

public final class TopicNaming {
    private final String topicReplacePrefix;
    private final String tableNamePrefix;

    private final String tableNameFormat;

    public TopicNaming(String topicReplacePrefix, String tableNamePrefix, String tableNameFormat) {
        this.topicReplacePrefix = topicReplacePrefix;
        this.tableNamePrefix = tableNamePrefix;
        this.tableNameFormat = tableNameFormat;
    }

    public String tableName(String topicName) {

        if (topicName == null) {
            throw new IllegalArgumentException("topicName must not be null.");
        } else {
            String tempTableName =
                    this.topicReplacePrefix != null && topicName.startsWith(this.topicReplacePrefix)
                            ? topicName.substring(this.topicReplacePrefix.length())
                            : topicName;

            if (tableNameFormat != null) {
                tempTableName = tempTableName.replaceAll("\\.", tableNameFormat);
            }

            if (this.tableNamePrefix != null) {
                return this.tableNamePrefix + tempTableName;
            } else {
                return tempTableName;
            }
        }
    }
}
