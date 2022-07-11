/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.base.sink.writer.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.sink.writer.strategy.AIMDScalingStrategy;
import org.apache.flink.connector.base.sink.writer.strategy.CongestionControlRateLimitingStrategy;
import org.apache.flink.connector.base.sink.writer.strategy.RateLimitingStrategy;

/**
 * {@link AsyncSinkWriterConfiguration} to configure the {@link
 * org.apache.flink.connector.base.sink.writer.AsyncSinkWriter}.
 */
@PublicEvolving
public class AsyncSinkWriterConfiguration {
    private final int maxBatchSize;
    private final int maxInFlightRequests;
    private final int maxBufferedRequests;
    private final long maxBatchSizeInBytes;
    private final long maxTimeInBufferMS;
    private final long maxRecordSizeInBytes;
    private final RateLimitingStrategy rateLimitingStrategy;

    private AsyncSinkWriterConfiguration(
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            RateLimitingStrategy rateLimitingStrategy) {
        this.maxBatchSize = maxBatchSize;
        this.maxInFlightRequests = maxInFlightRequests;
        this.maxBufferedRequests = maxBufferedRequests;
        this.maxBatchSizeInBytes = maxBatchSizeInBytes;
        this.maxTimeInBufferMS = maxTimeInBufferMS;
        this.maxRecordSizeInBytes = maxRecordSizeInBytes;
        this.rateLimitingStrategy = rateLimitingStrategy;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public int getMaxInFlightRequests() {
        return maxInFlightRequests;
    }

    public int getMaxBufferedRequests() {
        return maxBufferedRequests;
    }

    public long getMaxBatchSizeInBytes() {
        return maxBatchSizeInBytes;
    }

    public long getMaxTimeInBufferMS() {
        return maxTimeInBufferMS;
    }

    public long getMaxRecordSizeInBytes() {
        return maxRecordSizeInBytes;
    }

    public RateLimitingStrategy getRateLimitingStrategy() {
        return rateLimitingStrategy;
    }

    @PublicEvolving
    public static ConfigurationMaxBatchSize builder() {
        return new AsyncSinkWriterConfigurationBuilder();
    }

    /** Builder for {@link AsyncSinkWriterConfiguration}. */
    @PublicEvolving
    public static class AsyncSinkWriterConfigurationBuilder
            implements ConfigurationMaxBatchSize,
                    ConfigurationMaxInFlightRequests,
                    ConfigurationMaxBufferedRequests,
                    ConfigurationMaxBatchSizeInBytes,
                    ConfigurationMaxTimeInBufferMS,
                    ConfigurationMaxRecordSizeInBytes {

        private int maxBatchSize;
        private int maxInFlightRequests;
        private int maxBufferedRequests;
        private long maxBatchSizeInBytes;
        private long maxTimeInBufferMS;
        private long maxRecordSizeInBytes;
        private RateLimitingStrategy rateLimitingStrategy;

        @Override
        public ConfigurationMaxBatchSizeInBytes setMaxBatchSize(int maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        @Override
        public ConfigurationMaxInFlightRequests setMaxBatchSizeInBytes(long maxBatchSizeInBytes) {
            this.maxBatchSizeInBytes = maxBatchSizeInBytes;
            return this;
        }

        @Override
        public ConfigurationMaxBufferedRequests setMaxInFlightRequests(int maxInFlightRequests) {
            this.maxInFlightRequests = maxInFlightRequests;
            return this;
        }

        @Override
        public ConfigurationMaxTimeInBufferMS setMaxBufferedRequests(int maxBufferedRequests) {
            this.maxBufferedRequests = maxBufferedRequests;
            return this;
        }

        @Override
        public ConfigurationMaxRecordSizeInBytes setMaxTimeInBufferMS(long maxTimeInBufferMS) {
            this.maxTimeInBufferMS = maxTimeInBufferMS;
            return this;
        }

        @Override
        public AsyncSinkWriterConfigurationBuilder setMaxRecordSizeInBytes(
                long maxRecordSizeInBytes) {
            this.maxRecordSizeInBytes = maxRecordSizeInBytes;
            return this;
        }

        public AsyncSinkWriterConfigurationBuilder setRateLimitingStrategy(
                RateLimitingStrategy rateLimitingStrategy) {
            this.rateLimitingStrategy = rateLimitingStrategy;
            return this;
        }

        public AsyncSinkWriterConfiguration build() {
            if (rateLimitingStrategy == null) {
                rateLimitingStrategy =
                        CongestionControlRateLimitingStrategy.builder()
                                .setMaxInFlightRequests(maxInFlightRequests)
                                .setInitialMaxInFlightMessages(maxBatchSize)
                                .setScalingStrategy(
                                        AIMDScalingStrategy.builder(
                                                        maxBatchSize * maxInFlightRequests)
                                                .build())
                                .build();
            }

            return new AsyncSinkWriterConfiguration(
                    maxBatchSize,
                    maxInFlightRequests,
                    maxBufferedRequests,
                    maxBatchSizeInBytes,
                    maxTimeInBufferMS,
                    maxRecordSizeInBytes,
                    rateLimitingStrategy);
        }
    }

    /** Required MaxBatchSize parameter for {@link AsyncSinkWriterConfiguration}. */
    @PublicEvolving
    public interface ConfigurationMaxBatchSize {
        ConfigurationMaxBatchSizeInBytes setMaxBatchSize(int maxBatchSize);
    }

    /** Required MaxBatchSizeInBytes parameter for {@link AsyncSinkWriterConfiguration}. */
    @PublicEvolving
    public interface ConfigurationMaxBatchSizeInBytes {
        ConfigurationMaxInFlightRequests setMaxBatchSizeInBytes(long maxBatchSizeInBytes);
    }

    /** Required MaxInFlightRequests parameter for {@link AsyncSinkWriterConfiguration}. */
    @PublicEvolving
    public interface ConfigurationMaxInFlightRequests {
        ConfigurationMaxBufferedRequests setMaxInFlightRequests(int maxInFlightRequests);
    }

    /** Required MaxBufferedRequests parameter for {@link AsyncSinkWriterConfiguration}. */
    @PublicEvolving
    public interface ConfigurationMaxBufferedRequests {
        ConfigurationMaxTimeInBufferMS setMaxBufferedRequests(int maxBufferedRequests);
    }

    /** Required MaxTimeInBufferMS parameter for {@link AsyncSinkWriterConfiguration}. */
    @PublicEvolving
    public interface ConfigurationMaxTimeInBufferMS {
        ConfigurationMaxRecordSizeInBytes setMaxTimeInBufferMS(long maxTimeInBufferMS);
    }

    /** Required MaxRecordSizeInBytes parameter for {@link AsyncSinkWriterConfiguration}. */
    @PublicEvolving
    public interface ConfigurationMaxRecordSizeInBytes {
        AsyncSinkWriterConfigurationBuilder setMaxRecordSizeInBytes(long maxRecordSizeInBytes);
    }
}
