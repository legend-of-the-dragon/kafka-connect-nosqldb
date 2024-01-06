package org.datacenter.kafka.source.kafka;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.utils.Time;

public class DeadlineManager {
    public static final long RETRY_RESUME_FREQUENCY_MS = 5000L;

    public static final long METADATA_MAX_AGE_MS = 30000L;

    public static final long READY_STATE_MAX_AGE_MS = 10000L;

    public enum DeadlineType {
        TRY_RESUME, RETRY_TOPIC_EXPANSION, TOPIC_CONFIG_CHECK;
    }

    private final Map<DeadlineType, Long> deadlines = new HashMap<>();

    private Time time = Time.SYSTEM;

    public synchronized Long get(DeadlineType type) {
        return this.deadlines.get(type);
    }

    public synchronized void set(DeadlineType type, Long deadline) {
        this.deadlines.put(type, deadline);
    }

    public Time getTime() {
        return this.time;
    }

    public void setTime(Time time) {
        this.time = time;
    }

    public long getMilliSeconds() {
        return this.time.milliseconds();
    }
}