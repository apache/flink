package org.apache.flink.mongodb.internal.options;

import org.apache.flink.mongodb.MongodbExecutionOptions;

import java.io.Serializable;
import java.util.Objects;

/** Options for the Mongodb lookup. */
public class MongodbLookupOptions implements Serializable {
    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private final int maxRetryTimes;
    private final boolean cacheMissingKey;

    public MongodbLookupOptions(
            long cacheMaxSize, long cacheExpireMs, int maxRetryTimes, boolean cacheMissingKey) {
        this.cacheMaxSize = cacheMaxSize;
        this.cacheExpireMs = cacheExpireMs;
        this.maxRetryTimes = maxRetryTimes;
        this.cacheMissingKey = cacheMissingKey;
    }

    public long getCacheMaxSize() {
        return cacheMaxSize;
    }

    public long getCacheExpireMs() {
        return cacheExpireMs;
    }

    public int getMaxRetryTimes() {
        return maxRetryTimes;
    }

    public boolean getCacheMissingKey() {
        return cacheMissingKey;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof MongodbLookupOptions) {
            MongodbLookupOptions options = (MongodbLookupOptions) o;
            return Objects.equals(cacheMaxSize, options.cacheMaxSize)
                    && Objects.equals(cacheExpireMs, options.cacheExpireMs)
                    && Objects.equals(maxRetryTimes, options.maxRetryTimes)
                    && Objects.equals(cacheMissingKey, options.cacheMissingKey);
        } else {
            return false;
        }
    }

    /** Builder of {@link MongodbLookupOptions}. */
    public static class Builder {
        private long cacheMaxSize = -1L;
        private long cacheExpireMs = -1L;
        private int maxRetryTimes = MongodbExecutionOptions.DEFAULT_MAX_RETRY_TIMES;
        private boolean cacheMissingKey = true;

        /** optional, lookup cache max size, over this value, the old data will be eliminated. */
        public Builder setCacheMaxSize(long cacheMaxSize) {
            this.cacheMaxSize = cacheMaxSize;
            return this;
        }

        /** optional, lookup cache expire mills, over this time, the old data will expire. */
        public Builder setCacheExpireMs(long cacheExpireMs) {
            this.cacheExpireMs = cacheExpireMs;
            return this;
        }

        /** optional, max retry times for jdbc connector. */
        public Builder setMaxRetryTimes(int maxRetryTimes) {
            this.maxRetryTimes = maxRetryTimes;
            return this;
        }

        /** optional, whether to exclude empty result. */
        public Builder setCacheMissingKey(boolean cacheMissingKey) {
            this.cacheMissingKey = cacheMissingKey;
            return this;
        }

        public MongodbLookupOptions build() {
            return new MongodbLookupOptions(
                    cacheMaxSize, cacheExpireMs, maxRetryTimes, cacheMissingKey);
        }
    }
}
