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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.PriorityQueueSetFactory;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * An entity keeping all the time-related services available to all operators extending the
 * {@link AbstractStreamOperator}. Right now, this is only a
 * {@link InternalTimerServiceImpl timer services}.
 *
 * <b>NOTE:</b> These services are only available to keyed operators.
 *
 * @param <K> The type of keys used for the timers and the registry.
 */
@Internal
public class InternalTimeServiceManager<K> {

	@VisibleForTesting
	static final String TIMER_STATE_PREFIX = "_timer_state";
	@VisibleForTesting
	static final String PROCESSING_TIMER_PREFIX = TIMER_STATE_PREFIX + "/processing_";
	@VisibleForTesting
	static final String EVENT_TIMER_PREFIX = TIMER_STATE_PREFIX + "/event_";

	private final KeyGroupRange localKeyGroupRange;
	private final KeyContext keyContext;

	private final PriorityQueueSetFactory priorityQueueSetFactory;
	private final ProcessingTimeService processingTimeService;

	private final Map<String, InternalTimerServiceImpl<K, ?>> timerServices;

	private final boolean useLegacySynchronousSnapshots;

	InternalTimeServiceManager(
		KeyGroupRange localKeyGroupRange,
		KeyContext keyContext,
		PriorityQueueSetFactory priorityQueueSetFactory,
		ProcessingTimeService processingTimeService, boolean useLegacySynchronousSnapshots) {

		this.localKeyGroupRange = Preconditions.checkNotNull(localKeyGroupRange);
		this.priorityQueueSetFactory = Preconditions.checkNotNull(priorityQueueSetFactory);
		this.keyContext = Preconditions.checkNotNull(keyContext);
		this.processingTimeService = Preconditions.checkNotNull(processingTimeService);
		this.useLegacySynchronousSnapshots = useLegacySynchronousSnapshots;

		this.timerServices = new HashMap<>();
	}

	@SuppressWarnings("unchecked")
	public <N> InternalTimerService<N> getInternalTimerService(
		String name,
		TimerSerializer<K, N> timerSerializer,
		Triggerable<K, N> triggerable) {

		InternalTimerServiceImpl<K, N> timerService = registerOrGetTimerService(name, timerSerializer);

		timerService.startTimerService(
			timerSerializer.getKeySerializer(),
			timerSerializer.getNamespaceSerializer(),
			triggerable);

		return timerService;
	}

	@SuppressWarnings("unchecked")
	<N> InternalTimerServiceImpl<K, N> registerOrGetTimerService(String name, TimerSerializer<K, N> timerSerializer) {
		InternalTimerServiceImpl<K, N> timerService = (InternalTimerServiceImpl<K, N>) timerServices.get(name);
		if (timerService == null) {

			timerService = new InternalTimerServiceImpl<>(
				localKeyGroupRange,
				keyContext,
				processingTimeService,
				createTimerPriorityQueue(PROCESSING_TIMER_PREFIX + name, timerSerializer),
				createTimerPriorityQueue(EVENT_TIMER_PREFIX + name, timerSerializer));

			timerServices.put(name, timerService);
		}
		return timerService;
	}

	Map<String, InternalTimerServiceImpl<K, ?>> getRegisteredTimerServices() {
		return Collections.unmodifiableMap(timerServices);
	}

	private <N> KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> createTimerPriorityQueue(
		String name,
		TimerSerializer<K, N> timerSerializer) {
		return priorityQueueSetFactory.create(
			name,
			timerSerializer);
	}

	public void advanceWatermark(Watermark watermark) throws Exception {
		for (InternalTimerServiceImpl<?, ?> service : timerServices.values()) {
			service.advanceWatermark(watermark.getTimestamp());
		}
	}

	//////////////////				Fault Tolerance Methods				///////////////////

	public void snapshotStateForKeyGroup(DataOutputView stream, int keyGroupIdx) throws IOException {
		Preconditions.checkState(useLegacySynchronousSnapshots);
		InternalTimerServiceSerializationProxy<K> serializationProxy =
			new InternalTimerServiceSerializationProxy<>(this, keyGroupIdx);

		serializationProxy.write(stream);
	}

	public void restoreStateForKeyGroup(
			InputStream stream,
			int keyGroupIdx,
			ClassLoader userCodeClassLoader) throws IOException {

		InternalTimerServiceSerializationProxy<K> serializationProxy =
			new InternalTimerServiceSerializationProxy<>(
				this,
				userCodeClassLoader,
				keyGroupIdx);

		serializationProxy.read(stream);
	}

	////////////////////			Methods used ONLY IN TESTS				////////////////////

	@VisibleForTesting
	public int numProcessingTimeTimers() {
		int count = 0;
		for (InternalTimerServiceImpl<?, ?> timerService : timerServices.values()) {
			count += timerService.numProcessingTimeTimers();
		}
		return count;
	}

	@VisibleForTesting
	public int numEventTimeTimers() {
		int count = 0;
		for (InternalTimerServiceImpl<?, ?> timerService : timerServices.values()) {
			count += timerService.numEventTimeTimers();
		}
		return count;
	}
}
