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

package org.apache.flink.runtime.state.heap.remote;

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
import org.apache.flink.runtime.state.heap.AsyncSnapshotStrategySynchronicityBehavior;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.heap.HeapSnapshotStrategy;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.heap.InternalKeyContextImpl;
import org.apache.flink.runtime.state.heap.SnapshotStrategySynchronicityBehavior;
import org.apache.flink.runtime.state.heap.StateTable;
import org.apache.flink.runtime.state.heap.SyncSnapshotStrategySynchronicityBehavior;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Builder class for {@link HeapKeyedStateBackend} which handles all necessary initializations and clean ups.
 *
 * @param <K> The data type that the key serializer serializes.
 */
public class RemoteHeapKeyedStateBackendBuilder<K> extends AbstractKeyedStateBackendBuilder<K> {
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

	private long writeBatchSize = RemoteHeapConfigurationOptions.WRITE_BATCH_SIZE
		.defaultValue()
		.getBytes();

	public RemoteHeapKeyedStateBackendBuilder(
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
	public RemoteHeapKeyedStateBackend<K> build() throws BackendBuildingException {

		// Map of registered Key/Value states
		Map<String, StateTable<K, ?, ?>> registeredKVStates = new HashMap<>();
		// Map of registered priority queue set states
		Map<String, HeapPriorityQueueSnapshotRestoreWrapper> registeredPQStates = new HashMap<>();
		LinkedHashMap<String, RemoteHeapKeyedStateBackend.RemoteHeapKvStateInfo> kvStateInformation = new LinkedHashMap<>();
		CloseableRegistry cancelStreamRegistryForBackend = new CloseableRegistry();
		HeapSnapshotStrategy<K> snapshotStrategy = initSnapshotStrategy(
			asynchronousSnapshots,
			registeredKVStates,
			registeredPQStates,
			cancelStreamRegistryForBackend);
		InternalKeyContext<K> keyContext = new InternalKeyContextImpl<>(
			keyGroupRange,
			numberOfKeyGroups
		);
		int keyGroupPrefixBytes = RemoteHeapKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(
			numberOfKeyGroups);
		RemoteHeapWriteBatchWrapper writeBatchWrapper = null;
		RemoteHeapSerializedCompositeKeyBuilder<K> sharedREMKeyBuilder = new RemoteHeapSerializedCompositeKeyBuilder<>(
			keySerializerProvider.currentSchemaSerializer(),
			keyGroupPrefixBytes,
			32);
		RemoteHeapRestoreOperation<K> restoreOperation = new RemoteHeapRestoreOperation<>(
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
			keyContext, "localhost");

		try {
			restoreOperation.restore();
		} catch (Exception e) {
			throw new BackendBuildingException("Failed when trying to restore heap backend", e);
		}

		writeBatchWrapper = new RemoteHeapWriteBatchWrapper(restoreOperation.syncDBClient, writeBatchSize);
		return new RemoteHeapKeyedStateBackend<K>(
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
			keyContext,
			sharedREMKeyBuilder,
			keyGroupPrefixBytes,
			writeBatchSize,
			kvStateInformation,
			writeBatchWrapper,
			restoreOperation.syncDBClient,
			restoreOperation.asyncDBClient
		);
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

	RemoteHeapKeyedStateBackendBuilder<K> setWriteBatchSize(long writeBatchSize) {
		checkArgument(writeBatchSize >= 0, "Write batch size should be non negative.");
		this.writeBatchSize = writeBatchSize;
		return this;
	}
}
