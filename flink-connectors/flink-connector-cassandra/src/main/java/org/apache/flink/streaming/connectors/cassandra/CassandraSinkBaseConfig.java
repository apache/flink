/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.cassandra;

import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.time.Duration;

/** Configuration for {@link CassandraSinkBase}. */
public final class CassandraSinkBaseConfig implements Serializable {
    // ------------------------ Default Configurations ------------------------

    /** The default maximum number of concurrent requests. By default, {@code Integer.MAX_VALUE}. */
    public static final int DEFAULT_MAX_CONCURRENT_REQUESTS = Integer.MAX_VALUE;

    /**
     * The default timeout duration when acquiring a permit to execute. By default, {@code
     * Long.MAX_VALUE}.
     */
    public static final Duration DEFAULT_MAX_CONCURRENT_REQUESTS_TIMEOUT =
            Duration.ofMillis(Long.MAX_VALUE);

    /** The default option to ignore null fields on insertion. By default, {@code false}. */
    public static final boolean DEFAULT_IGNORE_NULL_FIELDS = false;

    // ------------------------- Configuration Fields -------------------------

    /** Maximum number of concurrent requests allowed. */
    private final int maxConcurrentRequests;

    /** Timeout duration when acquiring a permit to execute. */
    private final Duration maxConcurrentRequestsTimeout;

    /** Whether to ignore null fields on insert. */
    private final boolean ignoreNullFields;

    private CassandraSinkBaseConfig(
            int maxConcurrentRequests,
            Duration maxConcurrentRequestsTimeout,
            boolean ignoreNullFields) {
        Preconditions.checkArgument(
                maxConcurrentRequests > 0, "Max concurrent requests is expected to be positive");
        Preconditions.checkNotNull(
                maxConcurrentRequestsTimeout, "Max concurrent requests timeout cannot be null");
        Preconditions.checkArgument(
                !maxConcurrentRequestsTimeout.isNegative(),
                "Max concurrent requests timeout is expected to be positive");
        this.maxConcurrentRequests = maxConcurrentRequests;
        this.maxConcurrentRequestsTimeout = maxConcurrentRequestsTimeout;
        this.ignoreNullFields = ignoreNullFields;
    }

    public int getMaxConcurrentRequests() {
        return maxConcurrentRequests;
    }

    public Duration getMaxConcurrentRequestsTimeout() {
        return maxConcurrentRequestsTimeout;
    }

    public boolean getIgnoreNullFields() {
        return ignoreNullFields;
    }

    @Override
    public String toString() {
        return "CassandraSinkBaseConfig{"
                + "maxConcurrentRequests="
                + maxConcurrentRequests
                + ", maxConcurrentRequestsTimeout="
                + maxConcurrentRequestsTimeout
                + ", ignoreNullFields="
                + ignoreNullFields
                + '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /** Builder for the {@link CassandraSinkBaseConfig}. */
    public static class Builder {
        private int maxConcurrentRequests = DEFAULT_MAX_CONCURRENT_REQUESTS;
        private Duration maxConcurrentRequestsTimeout = DEFAULT_MAX_CONCURRENT_REQUESTS_TIMEOUT;
        private boolean ignoreNullFields = DEFAULT_IGNORE_NULL_FIELDS;

        Builder() {}

        public Builder setMaxConcurrentRequests(int maxConcurrentRequests) {
            this.maxConcurrentRequests = maxConcurrentRequests;
            return this;
        }

        public Builder setMaxConcurrentRequestsTimeout(Duration timeout) {
            this.maxConcurrentRequestsTimeout = timeout;
            return this;
        }

        public Builder setIgnoreNullFields(boolean ignoreNullFields) {
            this.ignoreNullFields = ignoreNullFields;
            return this;
        }

        public CassandraSinkBaseConfig build() {
            return new CassandraSinkBaseConfig(
                    maxConcurrentRequests, maxConcurrentRequestsTimeout, ignoreNullFields);
        }
    }
}
