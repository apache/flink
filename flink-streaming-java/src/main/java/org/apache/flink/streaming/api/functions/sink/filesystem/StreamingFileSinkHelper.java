/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import javax.annotation.Nullable;

/**
 * Helper for {@link StreamingFileSink}.
 * This helper can be used by {@link RichSinkFunction} or {@link StreamOperator}.
 */
@Internal
public class StreamingFileSinkHelper<IN> implements ProcessingTimeCallback {

	// -------------------------- state descriptors ---------------------------

	private static final ListStateDescriptor<byte[]> BUCKET_STATE_DESC =
			new ListStateDescriptor<>("bucket-states", BytePrimitiveArraySerializer.INSTANCE);

	private static final ListStateDescriptor<Long> MAX_PART_COUNTER_STATE_DESC =
			new ListStateDescriptor<>("max-part-counter", LongSerializer.INSTANCE);

	// --------------------------- fields -----------------------------

	private final long bucketCheckInterval;

	private final ProcessingTimeService procTimeService;

	private final Buckets<IN, ?> buckets;

	private final ListState<byte[]> bucketStates;

	private final ListState<Long> maxPartCountersState;

	public StreamingFileSinkHelper(
			Buckets<IN, ?> buckets,
			boolean isRestored,
			OperatorStateStore stateStore,
			ProcessingTimeService procTimeService,
			long bucketCheckInterval) throws Exception {
		this.bucketCheckInterval = bucketCheckInterval;
		this.buckets = buckets;
		this.bucketStates = stateStore.getListState(BUCKET_STATE_DESC);
		this.maxPartCountersState = stateStore.getUnionListState(MAX_PART_COUNTER_STATE_DESC);
		this.procTimeService = procTimeService;

		if (isRestored) {
			buckets.initializeState(bucketStates, maxPartCountersState);
		}

		long currentProcessingTime = procTimeService.getCurrentProcessingTime();
		procTimeService.registerTimer(currentProcessingTime + bucketCheckInterval, this);
	}

	public void commitUpToCheckpoint(long checkpointId) throws Exception {
		buckets.commitUpToCheckpoint(checkpointId);
	}

	public void snapshotState(long checkpointId) throws Exception {
		buckets.snapshotState(
				checkpointId,
				bucketStates,
				maxPartCountersState);
	}

	@Override
	public void onProcessingTime(long timestamp) throws Exception {
		final long currentTime = procTimeService.getCurrentProcessingTime();
		buckets.onProcessingTime(currentTime);
		procTimeService.registerTimer(currentTime + bucketCheckInterval, this);
	}

	public void onElement(
			IN value,
			long currentProcessingTime,
			@Nullable Long elementTimestamp,
			long currentWatermark) throws Exception {
		buckets.onElement(value, currentProcessingTime, elementTimestamp, currentWatermark);
	}

	public void close() {
		this.buckets.close();
	}
}
