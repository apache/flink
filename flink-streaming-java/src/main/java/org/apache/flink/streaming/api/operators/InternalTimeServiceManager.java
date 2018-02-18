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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.KeyGroupsList;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * An entity keeping all the time-related services available to all operators extending the
 * {@link AbstractStreamOperator}. Right now, this is only a
 * {@link HeapInternalTimerService timer services}.
 *
 * <b>NOTE:</b> These services are only available to keyed operators.
 *
 * @param <K> The type of keys used for the timers and the registry.
 * @param <N> The type of namespace used for the timers.
 */
@Internal
public class InternalTimeServiceManager<K, N> {

	private final int totalKeyGroups;
	private final KeyGroupsList localKeyGroupRange;
	private final KeyContext keyContext;

	private final ProcessingTimeService processingTimeService;

	private final Map<String, HeapInternalTimerService<K, N>> timerServices;

	InternalTimeServiceManager(
			int totalKeyGroups,
			KeyGroupsList localKeyGroupRange,
			KeyContext keyContext,
			ProcessingTimeService processingTimeService) {

		Preconditions.checkArgument(totalKeyGroups > 0);
		this.totalKeyGroups = totalKeyGroups;
		this.localKeyGroupRange = Preconditions.checkNotNull(localKeyGroupRange);

		this.keyContext = Preconditions.checkNotNull(keyContext);
		this.processingTimeService = Preconditions.checkNotNull(processingTimeService);

		this.timerServices = new HashMap<>();
	}

	/**
	 * Returns a {@link InternalTimerService} that can be used to query current processing time
	 * and event time and to set timers. An operator can have several timer services, where
	 * each has its own namespace serializer. Timer services are differentiated by the string
	 * key that is given when requesting them, if you call this method with the same key
	 * multiple times you will get the same timer service instance in subsequent requests.
	 *
	 * <p>Timers are always scoped to a key, the currently active key of a keyed stream operation.
	 * When a timer fires, this key will also be set as the currently active key.
	 *
	 * <p>Each timer has attached metadata, the namespace. Different timer services
	 * can have a different namespace type. If you don't need namespace differentiation you
	 * can use {@link VoidNamespaceSerializer} as the namespace serializer.
	 *
	 * @param name The name of the requested timer service. If no service exists under the given
	 *             name a new one will be created and returned.
	 * @param keySerializer {@code TypeSerializer} for the timer keys.
	 * @param namespaceSerializer {@code TypeSerializer} for the timer namespace.
	 * @param triggerable The {@link Triggerable} that should be invoked when timers fire
	 */
	public InternalTimerService<N> getInternalTimerService(String name, TypeSerializer<K> keySerializer,
														TypeSerializer<N> namespaceSerializer, Triggerable<K, N> triggerable) {

		HeapInternalTimerService<K, N> timerService = timerServices.get(name);
		if (timerService == null) {
			timerService = new HeapInternalTimerService<>(totalKeyGroups,
				localKeyGroupRange, keyContext, processingTimeService);
			timerServices.put(name, timerService);
		}
		timerService.startTimerService(keySerializer, namespaceSerializer, triggerable);
		return timerService;
	}

	public void advanceWatermark(Watermark watermark) throws Exception {
		for (HeapInternalTimerService<?, ?> service : timerServices.values()) {
			service.advanceWatermark(watermark.getTimestamp());
		}
	}

	//////////////////				Fault Tolerance Methods				///////////////////

	public void snapshotStateForKeyGroup(DataOutputView stream, int keyGroupIdx) throws IOException {
		InternalTimerServiceSerializationProxy<K, N> serializationProxy =
			new InternalTimerServiceSerializationProxy<>(timerServices, keyGroupIdx);

		serializationProxy.write(stream);
	}

	public void restoreStateForKeyGroup(
			InputStream stream,
			int keyGroupIdx,
			ClassLoader userCodeClassLoader) throws IOException {

		InternalTimerServiceSerializationProxy<K, N> serializationProxy =
			new InternalTimerServiceSerializationProxy<>(
				timerServices,
				userCodeClassLoader,
				totalKeyGroups,
				localKeyGroupRange,
				keyContext,
				processingTimeService,
				keyGroupIdx);

		serializationProxy.read(stream);
	}

	////////////////////			Methods used ONLY IN TESTS				////////////////////

	@VisibleForTesting
	public int numProcessingTimeTimers() {
		int count = 0;
		for (HeapInternalTimerService<?, ?> timerService : timerServices.values()) {
			count += timerService.numProcessingTimeTimers();
		}
		return count;
	}

	@VisibleForTesting
	public int numEventTimeTimers() {
		int count = 0;
		for (HeapInternalTimerService<?, ?> timerService : timerServices.values()) {
			count += timerService.numEventTimeTimers();
		}
		return count;
	}
}
