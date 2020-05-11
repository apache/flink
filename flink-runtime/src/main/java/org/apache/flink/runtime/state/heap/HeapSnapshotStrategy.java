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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.AbstractSnapshotStrategy;
import org.apache.flink.runtime.state.AsyncSnapshotCallable;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.DoneFuture;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.runtime.state.StateSnapshotRestore;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;

/**
 * Base class for the snapshots of the heap backend that outlines the algorithm and offers some hooks to realize
 * the concrete strategies. Subclasses must be threadsafe.
 */
class HeapSnapshotStrategy<K>
	extends AbstractSnapshotStrategy<KeyedStateHandle> implements SnapshotStrategySynchronicityBehavior<K> {

	private final SnapshotStrategySynchronicityBehavior<K> snapshotStrategySynchronicityTrait;
	private final Map<String, StateTable<K, ?, ?>> registeredKVStates;
	private final Map<String, HeapPriorityQueueSnapshotRestoreWrapper> registeredPQStates;
	private final StreamCompressionDecorator keyGroupCompressionDecorator;
	private final LocalRecoveryConfig localRecoveryConfig;
	private final KeyGroupRange keyGroupRange;
	private final CloseableRegistry cancelStreamRegistry;
	private final StateSerializerProvider<K> keySerializerProvider;

	HeapSnapshotStrategy(
		SnapshotStrategySynchronicityBehavior<K> snapshotStrategySynchronicityTrait,
		Map<String, StateTable<K, ?, ?>> registeredKVStates,
		Map<String, HeapPriorityQueueSnapshotRestoreWrapper> registeredPQStates,
		StreamCompressionDecorator keyGroupCompressionDecorator,
		LocalRecoveryConfig localRecoveryConfig,
		KeyGroupRange keyGroupRange,
		CloseableRegistry cancelStreamRegistry,
		StateSerializerProvider<K> keySerializerProvider) {
		super("Heap backend snapshot");
		this.snapshotStrategySynchronicityTrait = snapshotStrategySynchronicityTrait;
		this.registeredKVStates = registeredKVStates;
		this.registeredPQStates = registeredPQStates;
		this.keyGroupCompressionDecorator = keyGroupCompressionDecorator;
		this.localRecoveryConfig = localRecoveryConfig;
		this.keyGroupRange = keyGroupRange;
		this.cancelStreamRegistry = cancelStreamRegistry;
		this.keySerializerProvider = keySerializerProvider;
	}

	@Nonnull
	@Override
	public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
		long checkpointId,
		long timestamp,
		@Nonnull CheckpointStreamFactory primaryStreamFactory,
		@Nonnull CheckpointOptions checkpointOptions) throws IOException {

		if (!hasRegisteredState()) {
			return DoneFuture.of(SnapshotResult.empty());
		}

		int numStates = registeredKVStates.size() + registeredPQStates.size();

		Preconditions.checkState(numStates <= Short.MAX_VALUE,
			"Too many states: " + numStates +
				". Currently at most " + Short.MAX_VALUE + " states are supported");

		final List<StateMetaInfoSnapshot> metaInfoSnapshots = new ArrayList<>(numStates);
		final Map<StateUID, Integer> stateNamesToId =
			new HashMap<>(numStates);
		final Map<StateUID, StateSnapshot> cowStateStableSnapshots =
			new HashMap<>(numStates);

		processSnapshotMetaInfoForAllStates(
			metaInfoSnapshots,
			cowStateStableSnapshots,
			stateNamesToId,
			registeredKVStates,
			StateMetaInfoSnapshot.BackendStateType.KEY_VALUE);

		processSnapshotMetaInfoForAllStates(
			metaInfoSnapshots,
			cowStateStableSnapshots,
			stateNamesToId,
			registeredPQStates,
			StateMetaInfoSnapshot.BackendStateType.PRIORITY_QUEUE);

		final KeyedBackendSerializationProxy<K> serializationProxy =
			new KeyedBackendSerializationProxy<>(
				// TODO: this code assumes that writing a serializer is threadsafe, we should support to
				// get a serialized form already at state registration time in the future
				getKeySerializer(),
				metaInfoSnapshots,
				!Objects.equals(UncompressedStreamCompressionDecorator.INSTANCE, keyGroupCompressionDecorator));

		final SupplierWithException<CheckpointStreamWithResultProvider, Exception> checkpointStreamSupplier =

			localRecoveryConfig.isLocalRecoveryEnabled() && !checkpointOptions.getCheckpointType().isSavepoint() ?

				() -> CheckpointStreamWithResultProvider.createDuplicatingStream(
					checkpointId,
					CheckpointedStateScope.EXCLUSIVE,
					primaryStreamFactory,
					localRecoveryConfig.getLocalStateDirectoryProvider()) :

				() -> CheckpointStreamWithResultProvider.createSimpleStream(
					CheckpointedStateScope.EXCLUSIVE,
					primaryStreamFactory);

		//--------------------------------------------------- this becomes the end of sync part

		final AsyncSnapshotCallable<SnapshotResult<KeyedStateHandle>> asyncSnapshotCallable =
			new AsyncSnapshotCallable<SnapshotResult<KeyedStateHandle>>() {
				@Override
				protected SnapshotResult<KeyedStateHandle> callInternal() throws Exception {

					final CheckpointStreamWithResultProvider streamWithResultProvider =
						checkpointStreamSupplier.get();

					snapshotCloseableRegistry.registerCloseable(streamWithResultProvider);

					final CheckpointStreamFactory.CheckpointStateOutputStream localStream =
						streamWithResultProvider.getCheckpointOutputStream();

					final DataOutputViewStreamWrapper outView = new DataOutputViewStreamWrapper(localStream);
					serializationProxy.write(outView);

					final long[] keyGroupRangeOffsets = new long[keyGroupRange.getNumberOfKeyGroups()];

					for (int keyGroupPos = 0; keyGroupPos < keyGroupRange.getNumberOfKeyGroups(); ++keyGroupPos) {
						int keyGroupId = keyGroupRange.getKeyGroupId(keyGroupPos);
						keyGroupRangeOffsets[keyGroupPos] = localStream.getPos();
						outView.writeInt(keyGroupId);

						for (Map.Entry<StateUID, StateSnapshot> stateSnapshot :
							cowStateStableSnapshots.entrySet()) {
							StateSnapshot.StateKeyGroupWriter partitionedSnapshot =

								stateSnapshot.getValue().getKeyGroupWriter();
							try (
								OutputStream kgCompressionOut =
									keyGroupCompressionDecorator.decorateWithCompression(localStream)) {
								DataOutputViewStreamWrapper kgCompressionView =
									new DataOutputViewStreamWrapper(kgCompressionOut);
								kgCompressionView.writeShort(stateNamesToId.get(stateSnapshot.getKey()));
								partitionedSnapshot.writeStateInKeyGroup(kgCompressionView, keyGroupId);
							} // this will just close the outer compression stream
						}
					}

					if (snapshotCloseableRegistry.unregisterCloseable(streamWithResultProvider)) {
						KeyGroupRangeOffsets kgOffs = new KeyGroupRangeOffsets(keyGroupRange, keyGroupRangeOffsets);
						SnapshotResult<StreamStateHandle> result =
							streamWithResultProvider.closeAndFinalizeCheckpointStreamResult();
						return CheckpointStreamWithResultProvider.toKeyedStateHandleSnapshotResult(result, kgOffs);
					} else {
						throw new IOException("Stream already unregistered.");
					}
				}

				@Override
				protected void cleanupProvidedResources() {
					for (StateSnapshot tableSnapshot : cowStateStableSnapshots.values()) {
						tableSnapshot.release();
					}
				}

				@Override
				protected void logAsyncSnapshotComplete(long startTime) {
					if (snapshotStrategySynchronicityTrait.isAsynchronous()) {
						logAsyncCompleted(primaryStreamFactory, startTime);
					}
				}
			};

		final FutureTask<SnapshotResult<KeyedStateHandle>> task =
			asyncSnapshotCallable.toAsyncSnapshotFutureTask(cancelStreamRegistry);
		finalizeSnapshotBeforeReturnHook(task);

		return task;
	}

	@Override
	public void finalizeSnapshotBeforeReturnHook(Runnable runnable) {
		snapshotStrategySynchronicityTrait.finalizeSnapshotBeforeReturnHook(runnable);
	}

	@Override
	public boolean isAsynchronous() {
		return snapshotStrategySynchronicityTrait.isAsynchronous();
	}

	@Override
	public <N, V> StateTable<K, N, V> newStateTable(
		InternalKeyContext<K> keyContext,
		RegisteredKeyValueStateBackendMetaInfo<N, V> newMetaInfo,
		TypeSerializer<K> keySerializer) {
		return snapshotStrategySynchronicityTrait.newStateTable(keyContext, newMetaInfo, keySerializer);
	}

	private void processSnapshotMetaInfoForAllStates(
		List<StateMetaInfoSnapshot> metaInfoSnapshots,
		Map<StateUID, StateSnapshot> cowStateStableSnapshots,
		Map<StateUID, Integer> stateNamesToId,
		Map<String, ? extends StateSnapshotRestore> registeredStates,
		StateMetaInfoSnapshot.BackendStateType stateType) {

		for (Map.Entry<String, ? extends StateSnapshotRestore> kvState : registeredStates.entrySet()) {
			final StateUID stateUid = StateUID.of(kvState.getKey(), stateType);
			stateNamesToId.put(stateUid, stateNamesToId.size());
			StateSnapshotRestore state = kvState.getValue();
			if (null != state) {
				final StateSnapshot stateSnapshot = state.stateSnapshot();
				metaInfoSnapshots.add(stateSnapshot.getMetaInfoSnapshot());
				cowStateStableSnapshots.put(stateUid, stateSnapshot);
			}
		}
	}

	private boolean hasRegisteredState() {
		return !(registeredKVStates.isEmpty() && registeredPQStates.isEmpty());
	}

	public TypeSerializer<K> getKeySerializer() {
		return keySerializerProvider.currentSchemaSerializer();
	}
}
