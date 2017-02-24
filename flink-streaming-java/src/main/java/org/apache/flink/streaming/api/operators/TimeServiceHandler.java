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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyGroupsList;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A handler keeping all the time-related services available to all operators extending the
 * {@link AbstractStreamOperator}. These are the different {@link HeapInternalTimerService timer services}
 * and the {@link KeyRegistry}.
 *
 * <b>NOTE:</b> These services are only available to keyed operators.
 *
 * @param <K> The type of keys used for the timers and the registry.
 * @param <N> The type of namespace used for the timers.
 */
public class TimeServiceHandler<K, N> {

	private final int totalKeyGroups;
	private final KeyGroupsList localKeyGroupRange;

	private final KeyContext keyContext;
	private final ProcessingTimeService processingTimeService;

	private final Map<String, HeapInternalTimerService<K, N>> timerServices;

	private KeyRegistry<K> keyRegistry;

	public TimeServiceHandler(
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
		this.keyRegistry = new KeyRegistry<>(totalKeyGroups, localKeyGroupRange, keyContext);
	}

	/**
	 * Registers a {@link KeyRegistry.OnWatermarkCallback} with the current {@link TimeServiceHandler}.
	 * This callback is going to be invoked upon reception of a {@link Watermark} for all keys
	 * registered using the {@link #registerKeyForWatermarkCallback(Object)}.
	 *
	 * @param callback The callback to be registered.
	 * @param keySerializer A serializer for the registered keys.
	 */
	public void registerOnWatermarkCallback(
			KeyRegistry.OnWatermarkCallback<K> callback,
			TypeSerializer<K> keySerializer) {
		keyRegistry.setWatermarkCallback(callback, keySerializer);
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

	/**
	 * Registers a key with the service. This will lead to the {@link KeyRegistry.OnWatermarkCallback}
	 * being invoked for this key upon reception of each subsequent watermark.
	 *
	 * @param key The key to be registered.
	 */
	public void registerKeyForWatermarkCallback(K key) {
		try {
			keyRegistry.registerKeyForWatermarkCallback(key);
		} catch (Exception e) {
			throw new RuntimeException("Exception occurred while registering key " + key + " with the key registry.", e);
		}
	}

	/**
	 * Unregisters the provided key from the service. From now on, the callback will not
	 * be invoked for this key on subsequent watermarks.
	 *
	 * @param key The key to be unregistered.
	 */
	public void unregisterKeyForWatermarkCallback(K key) {
		try {
			keyRegistry.unregisterKeyFromWatermarkCallback(key);
		} catch (Exception e) {
			throw new RuntimeException("Exception occurred while unregistering key " + key + " with the key registry.", e);
		}
	}

	public void advanceWatermark(Watermark watermark) throws Exception {
		for (HeapInternalTimerService<?, ?> service : timerServices.values()) {
			service.advanceWatermark(watermark.getTimestamp());
		}
		keyRegistry.invokeOnWatermarkCallback(watermark);
	}

	public void snapshotTimersForKeyGroup(DataOutputViewStreamWrapper stream, int keyGroupIdx) throws Exception {
		stream.writeInt(timerServices.size());

		for (Map.Entry<String, HeapInternalTimerService<K, N>> entry : timerServices.entrySet()) {
			String serviceName = entry.getKey();
			HeapInternalTimerService<?, ?> timerService = entry.getValue();

			stream.writeUTF(serviceName);
			timerService.snapshotTimersForKeyGroup(stream, keyGroupIdx);
		}

		// write a byte indicating if there was a key
		// registry service instantiated (1) or not (0).
		if (keyRegistry != null) {
			stream.writeByte(1);
			keyRegistry.snapshotKeysForKeyGroup(stream, keyGroupIdx);
		} else {
			stream.writeByte(0);
		}
	}

	public void restoreTimersForKeyGroup(DataInputViewStreamWrapper stream, int keyGroupIdx,
										ClassLoader userCodeClassLoader) throws IOException, ClassNotFoundException {

		int noOfTimerServices = stream.readInt();
		for (int i = 0; i < noOfTimerServices; i++) {
			String serviceName = stream.readUTF();

			HeapInternalTimerService<K, N> timerService = timerServices.get(serviceName);
			if (timerService == null) {
				timerService = new HeapInternalTimerService<>(
					totalKeyGroups,
					localKeyGroupRange,
					keyContext,
					processingTimeService);
				timerServices.put(serviceName, timerService);
			}
			timerService.restoreTimersForKeyGroup(stream, keyGroupIdx, userCodeClassLoader);
		}

		byte hadKeyRegistry = stream.readByte();
		if (hadKeyRegistry == 1) {

			if (keyRegistry == null) {
				keyRegistry = new KeyRegistry<>(totalKeyGroups, localKeyGroupRange, keyContext);
			}
			keyRegistry.restoreKeysForKeyGroup(stream, keyGroupIdx, userCodeClassLoader);
		}
	}

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
