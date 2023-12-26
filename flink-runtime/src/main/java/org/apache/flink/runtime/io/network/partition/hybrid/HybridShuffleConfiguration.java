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

package org.apache.flink.runtime.io.network.partition.hybrid;

import java.time.Duration;

/** Configuration for hybrid shuffle mode. */
public class HybridShuffleConfiguration {
    private static final int DEFAULT_MAX_BUFFERS_READ_AHEAD = 5;

    private static final Duration DEFAULT_BUFFER_REQUEST_TIMEOUT = Duration.ofMinutes(5);

    private static final float DEFAULT_SELECTIVE_STRATEGY_SPILL_THRESHOLD = 0.7f;

    private static final float DEFAULT_SELECTIVE_STRATEGY_SPILL_BUFFER_RATIO = 0.4f;

    private static final float DEFAULT_FULL_STRATEGY_NUM_BUFFERS_TRIGGER_SPILLED_RATIO = 0.5f;

    private static final float DEFAULT_FULL_STRATEGY_RELEASE_THRESHOLD = 0.7f;

    private static final float DEFAULT_FULL_STRATEGY_RELEASE_BUFFER_RATIO = 0.4f;

    private static final long DEFAULT_BUFFER_POLL_SIZE_CHECK_INTERVAL_MS = 1000;

    private static final long DEFAULT_NUM_RETAINED_IN_MEMORY_REGIONS_MAX = Long.MAX_VALUE;

    private static final int DEFAULT_REGION_GROUP_SIZE_IN_BYTES = 256;

    private static final SpillingStrategyType DEFAULT_SPILLING_STRATEGY_NAME =
            SpillingStrategyType.FULL;

    private final int maxBuffersReadAhead;

    private final Duration bufferRequestTimeout;

    private final int maxRequestedBuffers;

    private final SpillingStrategyType spillingStrategyType;

    private final long numRetainedInMemoryRegionsMax;

    private final int regionGroupSizeInBytes;

    private final long bufferPoolSizeCheckIntervalMs;

    // ----------------------------------------
    //        Selective Spilling Strategy
    // ----------------------------------------
    private final float selectiveStrategySpillThreshold;

    private final float selectiveStrategySpillBufferRatio;

    // ----------------------------------------
    //        Full Spilling Strategy
    // ----------------------------------------
    private final float fullStrategyNumBuffersTriggerSpillingRatio;

    private final float fullStrategyReleaseThreshold;

    private final float fullStrategyReleaseBufferRatio;

    private HybridShuffleConfiguration(
            int maxBuffersReadAhead,
            Duration bufferRequestTimeout,
            int maxRequestedBuffers,
            float selectiveStrategySpillThreshold,
            float selectiveStrategySpillBufferRatio,
            float fullStrategyNumBuffersTriggerSpillingRatio,
            float fullStrategyReleaseThreshold,
            float fullStrategyReleaseBufferRatio,
            SpillingStrategyType spillingStrategyType,
            long bufferPoolSizeCheckIntervalMs,
            long numRetainedInMemoryRegionsMax,
            int regionGroupSizeInBytes) {
        this.maxBuffersReadAhead = maxBuffersReadAhead;
        this.bufferRequestTimeout = bufferRequestTimeout;
        this.maxRequestedBuffers = maxRequestedBuffers;
        this.selectiveStrategySpillThreshold = selectiveStrategySpillThreshold;
        this.selectiveStrategySpillBufferRatio = selectiveStrategySpillBufferRatio;
        this.fullStrategyNumBuffersTriggerSpillingRatio =
                fullStrategyNumBuffersTriggerSpillingRatio;
        this.fullStrategyReleaseThreshold = fullStrategyReleaseThreshold;
        this.fullStrategyReleaseBufferRatio = fullStrategyReleaseBufferRatio;
        this.spillingStrategyType = spillingStrategyType;
        this.bufferPoolSizeCheckIntervalMs = bufferPoolSizeCheckIntervalMs;
        this.numRetainedInMemoryRegionsMax = numRetainedInMemoryRegionsMax;
        this.regionGroupSizeInBytes = regionGroupSizeInBytes;
    }

    public static Builder builder(int numSubpartitions, int numBuffersPerRequest) {
        return new Builder(numSubpartitions, numBuffersPerRequest);
    }

    /** Get {@link SpillingStrategyType} for hybrid shuffle mode. */
    public SpillingStrategyType getSpillingStrategyType() {
        return spillingStrategyType;
    }

    public int getMaxRequestedBuffers() {
        return maxRequestedBuffers;
    }

    /**
     * Determine how many buffers to read ahead at most for each subpartition to prevent other
     * consumers from starving.
     */
    public int getMaxBuffersReadAhead() {
        return maxBuffersReadAhead;
    }

    /**
     * Maximum time to wait when requesting read buffers from the buffer pool before throwing an
     * exception.
     */
    public Duration getBufferRequestTimeout() {
        return bufferRequestTimeout;
    }

    /**
     * When the number of buffers that have been requested exceeds this threshold, trigger the
     * spilling operation. Used by {@link HsSelectiveSpillingStrategy}.
     */
    public float getSelectiveStrategySpillThreshold() {
        return selectiveStrategySpillThreshold;
    }

    /** The proportion of buffers to be spilled. Used by {@link HsSelectiveSpillingStrategy}. */
    public float getSelectiveStrategySpillBufferRatio() {
        return selectiveStrategySpillBufferRatio;
    }

    /**
     * When the number of unSpilled buffers equal to this ratio times pool size, trigger the
     * spilling operation. Used by {@link HsFullSpillingStrategy}.
     */
    public float getFullStrategyNumBuffersTriggerSpillingRatio() {
        return fullStrategyNumBuffersTriggerSpillingRatio;
    }

    /**
     * When the number of buffers that have been requested exceeds this threshold, trigger the
     * release operation. Used by {@link HsFullSpillingStrategy}.
     */
    public float getFullStrategyReleaseThreshold() {
        return fullStrategyReleaseThreshold;
    }

    /** The proportion of buffers to be released. Used by {@link HsFullSpillingStrategy}. */
    public float getFullStrategyReleaseBufferRatio() {
        return fullStrategyReleaseBufferRatio;
    }

    /** Check interval of buffer pool's size. */
    public long getBufferPoolSizeCheckIntervalMs() {
        return bufferPoolSizeCheckIntervalMs;
    }

    /** Segment size of hybrid spilled file data index. */
    public int getRegionGroupSizeInBytes() {
        return regionGroupSizeInBytes;
    }

    /** Max number of hybrid retained regions in memory. */
    public long getNumRetainedInMemoryRegionsMax() {
        return numRetainedInMemoryRegionsMax;
    }

    /** Type of {@link HsSpillingStrategy}. */
    public enum SpillingStrategyType {
        FULL,
        SELECTIVE
    }

    /** Builder for {@link HybridShuffleConfiguration}. */
    public static class Builder {
        private int maxBuffersReadAhead = DEFAULT_MAX_BUFFERS_READ_AHEAD;

        private Duration bufferRequestTimeout = DEFAULT_BUFFER_REQUEST_TIMEOUT;

        private float selectiveStrategySpillThreshold = DEFAULT_SELECTIVE_STRATEGY_SPILL_THRESHOLD;

        private float selectiveStrategySpillBufferRatio =
                DEFAULT_SELECTIVE_STRATEGY_SPILL_BUFFER_RATIO;

        private float fullStrategyNumBuffersTriggerSpillingRatio =
                DEFAULT_FULL_STRATEGY_NUM_BUFFERS_TRIGGER_SPILLED_RATIO;

        private float fullStrategyReleaseThreshold = DEFAULT_FULL_STRATEGY_RELEASE_THRESHOLD;

        private float fullStrategyReleaseBufferRatio = DEFAULT_FULL_STRATEGY_RELEASE_BUFFER_RATIO;

        private long bufferPoolSizeCheckIntervalMs = DEFAULT_BUFFER_POLL_SIZE_CHECK_INTERVAL_MS;

        private SpillingStrategyType spillingStrategyType = DEFAULT_SPILLING_STRATEGY_NAME;

        private long numRetainedInMemoryRegionsMax = DEFAULT_NUM_RETAINED_IN_MEMORY_REGIONS_MAX;

        private int regionGroupSizeInBytes = DEFAULT_REGION_GROUP_SIZE_IN_BYTES;

        private final int numSubpartitions;

        private final int numBuffersPerRequest;

        private Builder(int numSubpartitions, int numBuffersPerRequest) {
            this.numSubpartitions = numSubpartitions;
            this.numBuffersPerRequest = numBuffersPerRequest;
        }

        public Builder setMaxBuffersReadAhead(int maxBuffersReadAhead) {
            this.maxBuffersReadAhead = maxBuffersReadAhead;
            return this;
        }

        public Builder setBufferRequestTimeout(Duration bufferRequestTimeout) {
            this.bufferRequestTimeout = bufferRequestTimeout;
            return this;
        }

        public Builder setSelectiveStrategySpillThreshold(float selectiveStrategySpillThreshold) {
            this.selectiveStrategySpillThreshold = selectiveStrategySpillThreshold;
            return this;
        }

        public Builder setSelectiveStrategySpillBufferRatio(
                float selectiveStrategySpillBufferRatio) {
            this.selectiveStrategySpillBufferRatio = selectiveStrategySpillBufferRatio;
            return this;
        }

        public Builder setFullStrategyNumBuffersTriggerSpillingRatio(
                float fullStrategyNumBuffersTriggerSpillingRatio) {
            this.fullStrategyNumBuffersTriggerSpillingRatio =
                    fullStrategyNumBuffersTriggerSpillingRatio;
            return this;
        }

        public Builder setFullStrategyReleaseThreshold(float fullStrategyReleaseThreshold) {
            this.fullStrategyReleaseThreshold = fullStrategyReleaseThreshold;
            return this;
        }

        public Builder setFullStrategyReleaseBufferRatio(float fullStrategyReleaseBufferRatio) {
            this.fullStrategyReleaseBufferRatio = fullStrategyReleaseBufferRatio;
            return this;
        }

        public Builder setSpillingStrategyType(SpillingStrategyType spillingStrategyType) {
            this.spillingStrategyType = spillingStrategyType;
            return this;
        }

        public Builder setBufferPoolSizeCheckIntervalMs(long bufferPoolSizeCheckIntervalMs) {
            this.bufferPoolSizeCheckIntervalMs = bufferPoolSizeCheckIntervalMs;
            return this;
        }

        public Builder setNumRetainedInMemoryRegionsMax(long numRetainedInMemoryRegionsMax) {
            this.numRetainedInMemoryRegionsMax = numRetainedInMemoryRegionsMax;
            return this;
        }

        public Builder setRegionGroupSizeInBytes(int regionGroupSizeInBytes) {
            this.regionGroupSizeInBytes = regionGroupSizeInBytes;
            return this;
        }

        public HybridShuffleConfiguration build() {
            return new HybridShuffleConfiguration(
                    maxBuffersReadAhead,
                    bufferRequestTimeout,
                    Math.max(2 * numBuffersPerRequest, numSubpartitions),
                    selectiveStrategySpillThreshold,
                    selectiveStrategySpillBufferRatio,
                    fullStrategyNumBuffersTriggerSpillingRatio,
                    fullStrategyReleaseThreshold,
                    fullStrategyReleaseBufferRatio,
                    spillingStrategyType,
                    bufferPoolSizeCheckIntervalMs,
                    numRetainedInMemoryRegionsMax,
                    regionGroupSizeInBytes);
        }
    }
}
