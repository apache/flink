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

package org.apache.flink.runtime.rest.messages.taskmanager;

import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Contains information about the TaskManager metrics. */
public class TaskManagerMetricsInfo {

    public static final String FIELD_NAME_HEAP_USED = "heapUsed";

    public static final String FIELD_NAME_HEAP_COMMITTED = "heapCommitted";

    public static final String FIELD_NAME_HEAP_MAX = "heapMax";

    public static final String FIELD_NAME_NON_HEAP_USED = "nonHeapUsed";

    public static final String FIELD_NAME_NON_HEAP_COMMITTED = "nonHeapCommitted";

    public static final String FIELD_NAME_NON_HEAP_MAX = "nonHeapMax";

    public static final String FIELD_NAME_DIRECT_COUNT = "directCount";

    public static final String FIELD_NAME_DIRECT_USED = "directUsed";

    public static final String FIELD_NAME_DIRECT_MAX = "directMax";

    public static final String FIELD_NAME_MAPPED_COUNT = "mappedCount";

    public static final String FIELD_NAME_MAPPED_USED = "mappedUsed";

    public static final String FIELD_NAME_MAPPED_MAX = "mappedMax";

    @Deprecated
    public static final String FIELD_NAME_NETWORK_MEMORY_SEGMENTS_AVAILABLE =
            "memorySegmentsAvailable";

    @Deprecated
    public static final String FIELD_NAME_NETWORK_MEMORY_SEGMENTS_TOTAL = "memorySegmentsTotal";

    public static final String FIELD_NAME_SHUFFLE_MEMORY_SEGMENTS_AVAILABLE =
            "nettyShuffleMemorySegmentsAvailable";

    public static final String FIELD_NAME_SHUFFLE_MEMORY_SEGMENTS_USED =
            "nettyShuffleMemorySegmentsUsed";

    public static final String FIELD_NAME_SHUFFLE_MEMORY_SEGMENTS_TOTAL =
            "nettyShuffleMemorySegmentsTotal";

    public static final String FIELD_NAME_SHUFFLE_MEMORY_AVAILABLE = "nettyShuffleMemoryAvailable";

    public static final String FIELD_NAME_SHUFFLE_MEMORY_USED = "nettyShuffleMemoryUsed";

    public static final String FIELD_NAME_SHUFFLE_MEMORY_TOTAL = "nettyShuffleMemoryTotal";

    public static final String FIELD_NAME_GARBAGE_COLLECTORS = "garbageCollectors";

    // --------- Heap memory -------------

    @JsonProperty(FIELD_NAME_HEAP_USED)
    private final long heapUsed;

    @JsonProperty(FIELD_NAME_HEAP_COMMITTED)
    private final long heapCommitted;

    @JsonProperty(FIELD_NAME_HEAP_MAX)
    private final long heapMax;

    // --------- Non heap memory -------------

    @JsonProperty(FIELD_NAME_NON_HEAP_USED)
    private final long nonHeapUsed;

    @JsonProperty(FIELD_NAME_NON_HEAP_COMMITTED)
    private final long nonHeapCommitted;

    @JsonProperty(FIELD_NAME_NON_HEAP_MAX)
    private final long nonHeapMax;

    // --------- Direct buffer pool -------------

    @JsonProperty(FIELD_NAME_DIRECT_COUNT)
    private final long directCount;

    @JsonProperty(FIELD_NAME_DIRECT_USED)
    private final long directUsed;

    @JsonProperty(FIELD_NAME_DIRECT_MAX)
    private final long directMax;

    // --------- Mapped buffer pool -------------

    @JsonProperty(FIELD_NAME_MAPPED_COUNT)
    private final long mappedCount;

    @JsonProperty(FIELD_NAME_MAPPED_USED)
    private final long mappedUsed;

    @JsonProperty(FIELD_NAME_MAPPED_MAX)
    private final long mappedMax;

    // --------- Shuffle Netty buffer pool -------------

    @JsonProperty(FIELD_NAME_SHUFFLE_MEMORY_SEGMENTS_AVAILABLE)
    private final long shuffleMemorySegmentsAvailable;

    @JsonProperty(FIELD_NAME_SHUFFLE_MEMORY_SEGMENTS_USED)
    private final long shuffleMemorySegmentsUsed;

    @JsonProperty(FIELD_NAME_SHUFFLE_MEMORY_SEGMENTS_TOTAL)
    private final long shuffleMemorySegmentsTotal;

    @JsonProperty(FIELD_NAME_SHUFFLE_MEMORY_AVAILABLE)
    private final long shuffleMemoryAvailable;

    @JsonProperty(FIELD_NAME_SHUFFLE_MEMORY_USED)
    private final long shuffleMemoryUsed;

    @JsonProperty(FIELD_NAME_SHUFFLE_MEMORY_TOTAL)
    private final long shuffleMemoryTotal;

    // --------- Garbage collectors -------------

    @JsonProperty(FIELD_NAME_GARBAGE_COLLECTORS)
    private final List<GarbageCollectorInfo> garbageCollectorsInfo;

    public TaskManagerMetricsInfo(
            long heapUsed,
            long heapCommitted,
            long heapMax,
            long nonHeapUsed,
            long nonHeapCommitted,
            long nonHeapMax,
            long directCount,
            long directUsed,
            long directMax,
            long mappedCount,
            long mappedUsed,
            long mappedMax,
            long shuffleMemorySegmentsAvailable,
            long shuffleMemorySegmentsUsed,
            long shuffleMemorySegmentsTotal,
            long shuffleMemoryAvailable,
            long shuffleMemoryUsed,
            long shuffleMemoryTotal,
            List<GarbageCollectorInfo> garbageCollectorsInfo) {
        this(
                heapUsed,
                heapCommitted,
                heapMax,
                nonHeapUsed,
                nonHeapCommitted,
                nonHeapMax,
                directCount,
                directUsed,
                directMax,
                mappedCount,
                mappedUsed,
                mappedMax,
                -1,
                -1,
                shuffleMemorySegmentsAvailable,
                shuffleMemorySegmentsUsed,
                shuffleMemorySegmentsTotal,
                shuffleMemoryAvailable,
                shuffleMemoryUsed,
                shuffleMemoryTotal,
                garbageCollectorsInfo);
    }

    @JsonCreator
    public TaskManagerMetricsInfo(
            @JsonProperty(FIELD_NAME_HEAP_USED) long heapUsed,
            @JsonProperty(FIELD_NAME_HEAP_COMMITTED) long heapCommitted,
            @JsonProperty(FIELD_NAME_HEAP_MAX) long heapMax,
            @JsonProperty(FIELD_NAME_NON_HEAP_USED) long nonHeapUsed,
            @JsonProperty(FIELD_NAME_NON_HEAP_COMMITTED) long nonHeapCommitted,
            @JsonProperty(FIELD_NAME_NON_HEAP_MAX) long nonHeapMax,
            @JsonProperty(FIELD_NAME_DIRECT_COUNT) long directCount,
            @JsonProperty(FIELD_NAME_DIRECT_USED) long directUsed,
            @JsonProperty(FIELD_NAME_DIRECT_MAX) long directMax,
            @JsonProperty(FIELD_NAME_MAPPED_COUNT) long mappedCount,
            @JsonProperty(FIELD_NAME_MAPPED_USED) long mappedUsed,
            @JsonProperty(FIELD_NAME_MAPPED_MAX) long mappedMax,
            @JsonProperty(FIELD_NAME_NETWORK_MEMORY_SEGMENTS_AVAILABLE)
                    long ignoredNetworkMemorySegmentsAvailable,
            @JsonProperty(FIELD_NAME_NETWORK_MEMORY_SEGMENTS_TOTAL)
                    long ignoredNetworkMemorySegmentsTotal,
            @JsonProperty(FIELD_NAME_SHUFFLE_MEMORY_SEGMENTS_AVAILABLE)
                    long shuffleMemorySegmentsAvailable,
            @JsonProperty(FIELD_NAME_SHUFFLE_MEMORY_SEGMENTS_USED) long shuffleMemorySegmentsUsed,
            @JsonProperty(FIELD_NAME_SHUFFLE_MEMORY_SEGMENTS_TOTAL) long shuffleMemorySegmentsTotal,
            @JsonProperty(FIELD_NAME_SHUFFLE_MEMORY_AVAILABLE) long shuffleMemoryAvailable,
            @JsonProperty(FIELD_NAME_SHUFFLE_MEMORY_USED) long shuffleMemoryUsed,
            @JsonProperty(FIELD_NAME_SHUFFLE_MEMORY_TOTAL) long shuffleMemoryTotal,
            @JsonProperty(FIELD_NAME_GARBAGE_COLLECTORS)
                    List<GarbageCollectorInfo> garbageCollectorsInfo) {
        this.heapUsed = heapUsed;
        this.heapCommitted = heapCommitted;
        this.heapMax = heapMax;
        this.nonHeapUsed = nonHeapUsed;
        this.nonHeapCommitted = nonHeapCommitted;
        this.nonHeapMax = nonHeapMax;
        this.directCount = directCount;
        this.directUsed = directUsed;
        this.directMax = directMax;
        this.mappedCount = mappedCount;
        this.mappedUsed = mappedUsed;
        this.mappedMax = mappedMax;
        this.shuffleMemorySegmentsAvailable = shuffleMemorySegmentsAvailable;
        this.shuffleMemorySegmentsUsed = shuffleMemorySegmentsUsed;
        this.shuffleMemorySegmentsTotal = shuffleMemorySegmentsTotal;
        this.shuffleMemoryAvailable = shuffleMemoryAvailable;
        this.shuffleMemoryUsed = shuffleMemoryUsed;
        this.shuffleMemoryTotal = shuffleMemoryTotal;
        this.garbageCollectorsInfo = Preconditions.checkNotNull(garbageCollectorsInfo);
    }

    @Deprecated
    @JsonProperty(FIELD_NAME_NETWORK_MEMORY_SEGMENTS_AVAILABLE)
    private long getMemorySegmentsAvailable() {
        return this.shuffleMemorySegmentsAvailable;
    }

    @Deprecated
    @JsonProperty(FIELD_NAME_NETWORK_MEMORY_SEGMENTS_TOTAL)
    private long getMemorySegmentsTotal() {
        return this.shuffleMemorySegmentsTotal;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TaskManagerMetricsInfo that = (TaskManagerMetricsInfo) o;
        return heapUsed == that.heapUsed
                && heapCommitted == that.heapCommitted
                && heapMax == that.heapMax
                && nonHeapUsed == that.nonHeapUsed
                && nonHeapCommitted == that.nonHeapCommitted
                && nonHeapMax == that.nonHeapMax
                && directCount == that.directCount
                && directUsed == that.directUsed
                && directMax == that.directMax
                && mappedCount == that.mappedCount
                && mappedUsed == that.mappedUsed
                && mappedMax == that.mappedMax
                && shuffleMemorySegmentsAvailable == that.shuffleMemorySegmentsAvailable
                && shuffleMemorySegmentsUsed == that.shuffleMemorySegmentsUsed
                && shuffleMemorySegmentsTotal == that.shuffleMemorySegmentsTotal
                && shuffleMemoryAvailable == that.shuffleMemoryAvailable
                && shuffleMemoryUsed == that.shuffleMemoryUsed
                && shuffleMemoryTotal == that.shuffleMemoryTotal
                && Objects.equals(garbageCollectorsInfo, that.garbageCollectorsInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                heapUsed,
                heapCommitted,
                heapMax,
                nonHeapUsed,
                nonHeapCommitted,
                nonHeapMax,
                directCount,
                directUsed,
                directMax,
                mappedCount,
                mappedUsed,
                mappedMax,
                shuffleMemorySegmentsAvailable,
                shuffleMemorySegmentsUsed,
                shuffleMemorySegmentsTotal,
                shuffleMemoryAvailable,
                shuffleMemoryUsed,
                shuffleMemoryTotal,
                garbageCollectorsInfo);
    }

    /** Information about the garbage collector metrics. */
    public static class GarbageCollectorInfo {

        public static final String FIELD_NAME_NAME = "name";

        public static final String FIELD_NAME_COUNT = "count";

        public static final String FIELD_NAME_TIME = "time";

        @JsonProperty(FIELD_NAME_NAME)
        private final String name;

        @JsonProperty(FIELD_NAME_COUNT)
        private final long count;

        @JsonProperty(FIELD_NAME_TIME)
        private final long time;

        @JsonCreator
        public GarbageCollectorInfo(
                @JsonProperty(FIELD_NAME_NAME) String name,
                @JsonProperty(FIELD_NAME_COUNT) long count,
                @JsonProperty(FIELD_NAME_TIME) long time) {
            this.name = Preconditions.checkNotNull(name);
            this.count = count;
            this.time = time;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            GarbageCollectorInfo that = (GarbageCollectorInfo) o;
            return count == that.count && time == that.time && Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, count, time);
        }
    }

    public static TaskManagerMetricsInfo empty() {
        return new TaskManagerMetricsInfo(
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                Collections.emptyList());
    }
}
