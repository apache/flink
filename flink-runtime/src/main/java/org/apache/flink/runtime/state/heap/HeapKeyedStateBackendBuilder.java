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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackendBuilder;
import org.apache.flink.runtime.state.BackendBuildingException;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Builder class for {@link HeapKeyedStateBackend} which handles all necessary initializations and clean ups.
 *
 * @param <K> The data type that the key serializer serializes.
 */
public class HeapKeyedStateBackendBuilder<K> extends AbstractKeyedStateBackendBuilder<K> {
	/**
	 * The configuration of local recovery.
	 */
	private final LocalRecoveryConfig localRecoveryConfig;
	/**
	 * Factory for state that is organized as priority queue.
	 */
	private final HeapPriorityQueueSetFactory priorityQueueSetFactory;
	/**
	 * Whether asynchronous snapshot is enabled.
	 */
	private final boolean asynchronousSnapshots;


	public HeapKeyedStateBackendBuilder(
		TaskKvStateRegistry kvStateRegistry,
		TypeSerializer<K> keySerializer,
		ClassLoader userCodeClassLoader,
		int numberOfKeyGroups,
		KeyGroupRange keyGroupRange,
		ExecutionConfig executionConfig,
		TtlTimeProvider ttlTimeProvider,
		@Nonnull Collection<KeyedStateHandle> stateHandles,
		StreamCompressionDecorator keyGroupCompressionDecorator,
		LocalRecoveryConfig localRecoveryConfig,
		HeapPriorityQueueSetFactory priorityQueueSetFactory,
		boolean asynchronousSnapshots,
		CloseableRegistry cancelStreamRegistry) {
		super(
			kvStateRegistry,
			keySerializer,
			userCodeClassLoader,
			numberOfKeyGroups,
			keyGroupRange,
			executionConfig,
			ttlTimeProvider,
			stateHandles,
			keyGroupCompressionDecorator,
			cancelStreamRegistry);
		this.localRecoveryConfig = localRecoveryConfig;
		this.priorityQueueSetFactory = priorityQueueSetFactory;
		this.asynchronousSnapshots = asynchronousSnapshots;
	}

	@Override
	public HeapKeyedStateBackend<K> build() throws BackendBuildingException {
		// Map of registered Key/Value states
		Map<String, StateTable<K, ?, ?>> registeredKVStates = new HashMap<>();
		// Map of registered priority queue set states
		Map<String, HeapPriorityQueueSnapshotRestoreWrapper> registeredPQStates = new HashMap<>();
		CloseableRegistry cancelStreamRegistryForBackend = new CloseableRegistry();
		HeapSnapshotStrategy<K> snapshotStrategy = initSnapshotStrategy(
			asynchronousSnapshots, registeredKVStates, registeredPQStates, cancelStreamRegistryForBackend);
		InternalKeyContext<K> keyContext = new InternalKeyContextImpl<>(
			keyGroupRange,
			numberOfKeyGroups
		);
		HeapRestoreOperation<K> restoreOperation = new HeapRestoreOperation<>(
			restoreStateHandles,
			keySerializerProvider,
			userCodeClassLoader,
			registeredKVStates,
			registeredPQStates,
			cancelStreamRegistry,
			priorityQueueSetFactory,
			keyGroupRange,
			numberOfKeyGroups,
			snapshotStrategy,
			keyContext);
		try {
			restoreOperation.restore();
			logger.info("Finished to build heap keyed state-backend.");
		} catch (Exception e) {
			throw new BackendBuildingException("Failed when trying to restore heap backend", e);
		}
		return new HeapKeyedStateBackend<>(
			kvStateRegistry,
			keySerializerProvider.currentSchemaSerializer(),
			userCodeClassLoader,
			executionConfig,
			ttlTimeProvider,
			cancelStreamRegistryForBackend,
			keyGroupCompressionDecorator,
			registeredKVStates,
			registeredPQStates,
			localRecoveryConfig,
			priorityQueueSetFactory,
			snapshotStrategy,
			keyContext);
	}

	private HeapSnapshotStrategy<K> initSnapshotStrategy(
		boolean asynchronousSnapshots,
		Map<String, StateTable<K, ?, ?>> registeredKVStates,
		Map<String, HeapPriorityQueueSnapshotRestoreWrapper> registeredPQStates,
		CloseableRegistry cancelStreamRegistry) {
		SnapshotStrategySynchronicityBehavior<K> synchronicityTrait = asynchronousSnapshots ?
			new AsyncSnapshotStrategySynchronicityBehavior<>() :
			new SyncSnapshotStrategySynchronicityBehavior<>();
		return new HeapSnapshotStrategy<>(
			synchronicityTrait,
			registeredKVStates,
			registeredPQStates,
			keyGroupCompressionDecorator,
			localRecoveryConfig,
			keyGroupRange,
			cancelStreamRegistry,
			keySerializerProvider);
	}
}
