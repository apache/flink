package org.apache.flink.mongodb;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Objects;

/** Mongodb sink batch options. */
@PublicEvolving
public class MongodbExecutionOptions implements Serializable {
    public static final int DEFAULT_MAX_RETRY_TIMES = 3;
    private static final int DEFAULT_INTERVAL_MILLIS = 0;
    public static final int DEFAULT_SIZE = 5000;

    private final long batchIntervalMs;
    private final int batchSize;
    private final int maxRetries;

    private MongodbExecutionOptions(long batchIntervalMs, int batchSize, int maxRetries) {
        Preconditions.checkArgument(maxRetries >= 0);
        this.batchIntervalMs = batchIntervalMs;
        this.batchSize = batchSize;
        this.maxRetries = maxRetries;
    }

    public long getBatchIntervalMs() {
        return batchIntervalMs;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MongodbExecutionOptions that = (MongodbExecutionOptions) o;
        return batchIntervalMs == that.batchIntervalMs
                && batchSize == that.batchSize
                && maxRetries == that.maxRetries;
    }

    @Override
    public int hashCode() {
        return Objects.hash(batchIntervalMs, batchSize, maxRetries);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static MongodbExecutionOptions defaults() {
        return builder().build();
    }

    /** Builder for {@link MongodbExecutionOptions}. */
    public static final class Builder {
        private long intervalMs = DEFAULT_INTERVAL_MILLIS;
        private int size = DEFAULT_SIZE;
        private int maxRetries = DEFAULT_MAX_RETRY_TIMES;

        public Builder withBatchSize(int size) {
            this.size = size;
            return this;
        }

        public Builder withBatchIntervalMs(long intervalMs) {
            this.intervalMs = intervalMs;
            return this;
        }

        public Builder withMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public MongodbExecutionOptions build() {
            return new MongodbExecutionOptions(intervalMs, size, maxRetries);
        }
    }
}
