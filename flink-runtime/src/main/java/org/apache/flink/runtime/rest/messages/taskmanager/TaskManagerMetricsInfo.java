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

import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Contains information about the TaskManager metrics.
 */
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

	public static final String FIELD_NAME_NETWORK_MEMORY_SEGMENTS_AVAILABLE = "memorySegmentsAvailable";

	public static final String FIELD_NAME_NETWORK_MEMROY_SEGMENTS_TOTAL = "memorySegmentsTotal";

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

	// --------- Network buffer pool -------------

	@JsonProperty(FIELD_NAME_NETWORK_MEMORY_SEGMENTS_AVAILABLE)
	private final long memorySegmentsAvailable;

	@JsonProperty(FIELD_NAME_NETWORK_MEMROY_SEGMENTS_TOTAL)
	private final long memorySegmentsTotal;

	// --------- Garbage collectors -------------

	@JsonProperty(FIELD_NAME_GARBAGE_COLLECTORS)
	private final List<GarbageCollectorInfo> garbageCollectorsInfo;

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
			@JsonProperty(FIELD_NAME_NETWORK_MEMORY_SEGMENTS_AVAILABLE) long memorySegmentsAvailable,
			@JsonProperty(FIELD_NAME_NETWORK_MEMROY_SEGMENTS_TOTAL) long memorySegmentsTotal,
			@JsonProperty(FIELD_NAME_GARBAGE_COLLECTORS) List<GarbageCollectorInfo> garbageCollectorsInfo) {
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
		this.memorySegmentsAvailable = memorySegmentsAvailable;
		this.memorySegmentsTotal = memorySegmentsTotal;
		this.garbageCollectorsInfo = Preconditions.checkNotNull(garbageCollectorsInfo);
	}

	public TaskManagerMetricsInfo (MetricStore.TaskManagerMetricStore tmMetrics) {

		Preconditions.checkNotNull(tmMetrics);

		long heapUsed = Long.valueOf(tmMetrics.getMetric("Status.JVM.Memory.Heap.Used", "0"));
		long heapCommitted = Long.valueOf(tmMetrics.getMetric("Status.JVM.Memory.Heap.Committed", "0"));
		long heapTotal = Long.valueOf(tmMetrics.getMetric("Status.JVM.Memory.Heap.Max", "0"));

		long nonHeapUsed = Long.valueOf(tmMetrics.getMetric("Status.JVM.Memory.NonHeap.Used", "0"));
		long nonHeapCommitted = Long.valueOf(tmMetrics.getMetric("Status.JVM.Memory.NonHeap.Committed", "0"));
		long nonHeapTotal = Long.valueOf(tmMetrics.getMetric("Status.JVM.Memory.NonHeap.Max", "0"));

		long directCount = Long.valueOf(tmMetrics.getMetric("Status.JVM.Memory.Direct.Count", "0"));
		long directUsed = Long.valueOf(tmMetrics.getMetric("Status.JVM.Memory.Direct.MemoryUsed", "0"));
		long directMax = Long.valueOf(tmMetrics.getMetric("Status.JVM.Memory.Direct.TotalCapacity", "0"));

		long mappedCount = Long.valueOf(tmMetrics.getMetric("Status.JVM.Memory.Mapped.Count", "0"));
		long mappedUsed = Long.valueOf(tmMetrics.getMetric("Status.JVM.Memory.Mapped.MemoryUsed", "0"));
		long mappedMax = Long.valueOf(tmMetrics.getMetric("Status.JVM.Memory.Mapped.TotalCapacity", "0"));

		long memorySegmentsAvailable = Long.valueOf(tmMetrics.getMetric("Status.Network.AvailableMemorySegments", "0"));
		long memorySegmentsTotal = Long.valueOf(tmMetrics.getMetric("Status.Network.TotalMemorySegments", "0"));

		final List<TaskManagerMetricsInfo.GarbageCollectorInfo> garbageCollectorInfo = createGarbageCollectorInfo(tmMetrics);

		this.heapUsed = heapUsed;
		this.heapCommitted = heapCommitted;
		this.heapMax = heapTotal;
		this.nonHeapUsed = nonHeapUsed;
		this.nonHeapCommitted = nonHeapCommitted;
		this.nonHeapMax = nonHeapTotal;
		this.directCount = directCount;
		this.directUsed = directUsed;
		this.directMax = directMax;
		this.mappedCount = mappedCount;
		this.mappedUsed = mappedUsed;
		this.mappedMax = mappedMax;
		this.memorySegmentsAvailable = memorySegmentsAvailable;
		this.memorySegmentsTotal = memorySegmentsTotal;
		this.garbageCollectorsInfo = Preconditions.checkNotNull(garbageCollectorInfo);
	}

	private static List<TaskManagerMetricsInfo.GarbageCollectorInfo> createGarbageCollectorInfo(MetricStore.TaskManagerMetricStore taskManagerMetricStore) {
		Preconditions.checkNotNull(taskManagerMetricStore);

		ArrayList<GarbageCollectorInfo> garbageCollectorInfos = new ArrayList<>(taskManagerMetricStore.garbageCollectorNames.size());

		for (String garbageCollectorName: taskManagerMetricStore.garbageCollectorNames) {
			final String count = taskManagerMetricStore.getMetric("Status.JVM.GarbageCollector." + garbageCollectorName + ".Count", null);
			final String time = taskManagerMetricStore.getMetric("Status.JVM.GarbageCollector." + garbageCollectorName + ".Time", null);

			if (count != null && time != null) {
				garbageCollectorInfos.add(
					new TaskManagerMetricsInfo.GarbageCollectorInfo(
						garbageCollectorName,
						Long.valueOf(count),
						Long.valueOf(time)));
			}
		}

		return garbageCollectorInfos;
	}

	@JsonIgnore
	public long getHeapUsed() {
		return heapUsed;
	}

	@JsonIgnore
	public long getHeapCommitted() {
		return heapCommitted;
	}

	public long getHeapMax() {
		return heapMax;
	}

	@JsonIgnore
	public long getNonHeapUsed() {
		return nonHeapUsed;
	}

	@JsonIgnore
	public long getNonHeapCommitted() {
		return nonHeapCommitted;
	}

	@JsonIgnore
	public long getNonHeapMax() {
		return nonHeapMax;
	}

	@JsonIgnore
	public long getDirectCount() {
		return directCount;
	}

	@JsonIgnore
	public long getDirectUsed() {
		return directUsed;
	}

	@JsonIgnore
	public long getDirectMax() {
		return directMax;
	}

	@JsonIgnore
	public long getMappedCount() {
		return mappedCount;
	}

	@JsonIgnore
	public long getMappedUsed() {
		return mappedUsed;
	}

	@JsonIgnore
	public long getMappedMax() {
		return mappedMax;
	}

	@JsonIgnore
	public long getMemorySegmentsAvailable() {
		return memorySegmentsAvailable;
	}

	@JsonIgnore
	public long getMemorySegmentsTotal() {
		return memorySegmentsTotal;
	}

	@JsonIgnore
	public List<GarbageCollectorInfo> getGarbageCollectorsInfo() {
		return garbageCollectorsInfo;
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
		return heapUsed == that.heapUsed &&
			heapCommitted == that.heapCommitted &&
			heapMax == that.heapMax &&
			nonHeapUsed == that.nonHeapUsed &&
			nonHeapCommitted == that.nonHeapCommitted &&
			nonHeapMax == that.nonHeapMax &&
			directCount == that.directCount &&
			directUsed == that.directUsed &&
			directMax == that.directMax &&
			mappedCount == that.mappedCount &&
			mappedUsed == that.mappedUsed &&
			mappedMax == that.mappedMax &&
			memorySegmentsAvailable == that.memorySegmentsAvailable &&
			memorySegmentsTotal == that.memorySegmentsTotal &&
			Objects.equals(garbageCollectorsInfo, that.garbageCollectorsInfo);
	}

	@Override
	public int hashCode() {
		return Objects.hash(heapUsed, heapCommitted, heapMax, nonHeapUsed, nonHeapCommitted, nonHeapMax, directCount, directUsed, directMax, mappedCount, mappedUsed, mappedMax, memorySegmentsAvailable, memorySegmentsTotal, garbageCollectorsInfo);
	}

	/**
	 * Information about the garbage collector metrics.
	 */
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
			return count == that.count &&
				time == that.time &&
				Objects.equals(name, that.name);
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
			Collections.emptyList());
	}
}
