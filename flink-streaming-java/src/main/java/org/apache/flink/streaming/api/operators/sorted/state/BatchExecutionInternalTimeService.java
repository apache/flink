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

package org.apache.flink.streaming.api.operators.sorted.state;

import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.PriorityComparator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.TimerHeapInternalTimer;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.function.BiConsumerWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of a {@link InternalTimerService} that manages timers with a single active key at a time.
 * Can be used in a BATCH execution mode.
 */
public class BatchExecutionInternalTimeService<K, N> implements InternalTimerService<N> {
	private static final Logger LOG = LoggerFactory.getLogger(BatchExecutionInternalTimeService.class);

	private final ProcessingTimeService processingTimeService;

	/**
	 * Processing time timers that are currently in-flight.
	 */
	private final KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> processingTimeTimersQueue;

	/**
	 * Event time timers that are currently in-flight.
	 */
	private final KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> eventTimeTimersQueue;

	/**
	 * The local event time, as denoted by the last received
	 * {@link org.apache.flink.streaming.api.watermark.Watermark Watermark}.
	 */
	private long currentWatermark = Long.MIN_VALUE;

	private final Triggerable<K, N> triggerTarget;

	private K currentKey;

	BatchExecutionInternalTimeService(
			ProcessingTimeService processingTimeService,
			Triggerable<K, N> triggerTarget) {

		this.processingTimeService = checkNotNull(processingTimeService);
		this.triggerTarget = checkNotNull(triggerTarget);

		this.processingTimeTimersQueue = new BatchExecutionInternalPriorityQueueSet<>(
			PriorityComparator.forPriorityComparableObjects(),
			128
		);
		this.eventTimeTimersQueue = new BatchExecutionInternalPriorityQueueSet<>(
			PriorityComparator.forPriorityComparableObjects(),
			128
		);
	}

	@Override
	public long currentProcessingTime() {
		return processingTimeService.getCurrentProcessingTime();
	}

	@Override
	public long currentWatermark() {
		return currentWatermark;
	}

	@Override
	public void registerProcessingTimeTimer(N namespace, long time) {
		// the currentWatermark == Long.MAX_VALUE indicates the timer was registered from the callback
		// we quiesce the TimerService to prohibit infinite loops at the end of a key
		if (currentWatermark == Long.MAX_VALUE) {
			LOG.warn("Timer service is quiesced. Processing time timer for timestamp '{}' will be ignored.", time);
			return;
		}
		processingTimeTimersQueue.add(new TimerHeapInternalTimer<>(time, currentKey, namespace));
	}

	@Override
	public void registerEventTimeTimer(N namespace, long time) {
		// the currentWatermark == Long.MAX_VALUE indicates the timer was registered from the callback
		// we quiesce the TimerService to prohibit infinite loops at the end of a key
		if (currentWatermark == Long.MAX_VALUE) {
			LOG.warn("Timer service is quiesced. Event time timer for timestamp '{}' will be ignored.", time);
			return;
		}
		eventTimeTimersQueue.add(new TimerHeapInternalTimer<>(time, currentKey, namespace));
	}

	@Override
	public void deleteProcessingTimeTimer(N namespace, long time) {
		processingTimeTimersQueue.remove(new TimerHeapInternalTimer<>(time, currentKey, namespace));
	}

	@Override
	public void deleteEventTimeTimer(N namespace, long time) {
		eventTimeTimersQueue.remove(new TimerHeapInternalTimer<>(time, currentKey, namespace));
	}

	@Override
	public void forEachEventTimeTimer(BiConsumerWithException<N, Long, Exception> consumer) {
		throw new UnsupportedOperationException(
			"The BatchExecutionInternalTimeService should not be used in State Processor API.");
	}

	@Override
	public void forEachProcessingTimeTimer(BiConsumerWithException<N, Long, Exception> consumer) {
		throw new UnsupportedOperationException(
			"The BatchExecutionInternalTimeService should not be used in State Processor API.");
	}

	public void setCurrentKey(K currentKey) throws Exception {
		if (currentKey != null && currentKey.equals(this.currentKey)) {
			return;
		}

		currentWatermark = Long.MAX_VALUE;
		InternalTimer<K, N> timer;
		while ((timer = eventTimeTimersQueue.poll()) != null) {
			triggerTarget.onEventTime(timer);
		}
		while ((timer = processingTimeTimersQueue.poll()) != null) {
			triggerTarget.onProcessingTime(timer);
		}
		currentWatermark = Long.MIN_VALUE;
		this.currentKey = currentKey;
	}
}
