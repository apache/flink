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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.CompatibilityUtil;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.io.async.AbstractAsyncCallableWithResources;
import org.apache.flink.runtime.io.async.AsyncStoppableTaskWithCallback;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.DoneFuture;
import org.apache.flink.runtime.state.HashMapSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.RegisteredKeyedBackendStateMetaInfo;
import org.apache.flink.runtime.state.SnappyStreamCompressionDecorator;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategy;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.internal.InternalFoldingState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalReducingState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StateMigrationException;
import org.apache.flink.util.function.SupplierWithException;

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Stream;

/**
 * A {@link AbstractKeyedStateBackend} that keeps state on the Java Heap and will serialize state to
 * streams provided by a {@link org.apache.flink.runtime.state.CheckpointStreamFactory} upon
 * checkpointing.
 *
 * @param <K> The key by which state is keyed.
 */
public class HeapKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {

	private static final Logger LOG = LoggerFactory.getLogger(HeapKeyedStateBackend.class);

	/**
	 * Map of state tables that stores all state of key/value states. We store it centrally so
	 * that we can easily checkpoint/restore it.
	 *
	 * <p>The actual parameters of StateTable are {@code StateTable<NamespaceT, Map<KeyT, StateT>>}
	 * but we can't put them here because different key/value states with different types and
	 * namespace types share this central list of tables.
	 */
	private final HashMap<String, StateTable<K, ?, ?>> stateTables = new HashMap<>();

	/**
	 * Map of state names to their corresponding restored state meta info.
	 *
	 * <p>
	 * TODO this map can be removed when eager-state registration is in place.
	 * TODO we currently need this cached to check state migration strategies when new serializers are registered.
	 */
	private final Map<String, RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> restoredKvStateMetaInfos;

	/**
	 * The configuration for local recovery.
	 */
	private final LocalRecoveryConfig localRecoveryConfig;

	/**
	 * The snapshot strategy for this backend. This determines, e.g., if snapshots are synchronous or asynchronous.
	 */
	private final HeapSnapshotStrategy snapshotStrategy;

	public HeapKeyedStateBackend(
			TaskKvStateRegistry kvStateRegistry,
			TypeSerializer<K> keySerializer,
			ClassLoader userCodeClassLoader,
			int numberOfKeyGroups,
			KeyGroupRange keyGroupRange,
			boolean asynchronousSnapshots,
			ExecutionConfig executionConfig,
			LocalRecoveryConfig localRecoveryConfig) {

		super(kvStateRegistry, keySerializer, userCodeClassLoader, numberOfKeyGroups, keyGroupRange, executionConfig);
		this.localRecoveryConfig = Preconditions.checkNotNull(localRecoveryConfig);

		SnapshotStrategySynchronicityBehavior<K> synchronicityTrait = asynchronousSnapshots ?
			new AsyncSnapshotStrategySynchronicityBehavior() :
			new SyncSnapshotStrategySynchronicityBehavior();

		this.snapshotStrategy = new HeapSnapshotStrategy(synchronicityTrait);
		LOG.info("Initializing heap keyed state backend with stream factory.");
		this.restoredKvStateMetaInfos = new HashMap<>();
	}

	// ------------------------------------------------------------------------
	//  state backend operations
	// ------------------------------------------------------------------------

	private <N, V> StateTable<K, N, V> tryRegisterStateTable(
			TypeSerializer<N> namespaceSerializer, StateDescriptor<?, V> stateDesc) throws StateMigrationException {

		return tryRegisterStateTable(
				stateDesc.getName(), stateDesc.getType(),
				namespaceSerializer, stateDesc.getSerializer());
	}

	private <N, V> StateTable<K, N, V> tryRegisterStateTable(
			String stateName,
			StateDescriptor.Type stateType,
			TypeSerializer<N> namespaceSerializer,
			TypeSerializer<V> valueSerializer) throws StateMigrationException {

		final RegisteredKeyedBackendStateMetaInfo<N, V> newMetaInfo =
				new RegisteredKeyedBackendStateMetaInfo<>(stateType, stateName, namespaceSerializer, valueSerializer);

		@SuppressWarnings("unchecked")
		StateTable<K, N, V> stateTable = (StateTable<K, N, V>) stateTables.get(stateName);

		if (stateTable == null) {
			stateTable = snapshotStrategy.newStateTable(newMetaInfo);
			stateTables.put(stateName, stateTable);
		} else {
			// TODO with eager registration in place, these checks should be moved to restorePartitionedState()

			Preconditions.checkState(
				stateName.equals(stateTable.getMetaInfo().getName()),
				"Incompatible state names. " +
					"Was [" + stateTable.getMetaInfo().getName() + "], " +
					"registered with [" + newMetaInfo.getName() + "].");

			if (!newMetaInfo.getStateType().equals(StateDescriptor.Type.UNKNOWN)
					&& !stateTable.getMetaInfo().getStateType().equals(StateDescriptor.Type.UNKNOWN)) {

				Preconditions.checkState(
					newMetaInfo.getStateType().equals(stateTable.getMetaInfo().getStateType()),
					"Incompatible state types. " +
						"Was [" + stateTable.getMetaInfo().getStateType() + "], " +
						"registered with [" + newMetaInfo.getStateType() + "].");
			}

			@SuppressWarnings("unchecked")
			RegisteredKeyedBackendStateMetaInfo.Snapshot<N, V> restoredMetaInfo =
				(RegisteredKeyedBackendStateMetaInfo.Snapshot<N, V>) restoredKvStateMetaInfos.get(stateName);

			// check compatibility results to determine if state migration is required
			CompatibilityResult<N> namespaceCompatibility = CompatibilityUtil.resolveCompatibilityResult(
					restoredMetaInfo.getNamespaceSerializer(),
					null,
					restoredMetaInfo.getNamespaceSerializerConfigSnapshot(),
					newMetaInfo.getNamespaceSerializer());

			CompatibilityResult<V> stateCompatibility = CompatibilityUtil.resolveCompatibilityResult(
					restoredMetaInfo.getStateSerializer(),
					UnloadableDummyTypeSerializer.class,
					restoredMetaInfo.getStateSerializerConfigSnapshot(),
					newMetaInfo.getStateSerializer());

			if (!namespaceCompatibility.isRequiresMigration() && !stateCompatibility.isRequiresMigration()) {
				// new serializers are compatible; use them to replace the old serializers
				stateTable.setMetaInfo(newMetaInfo);
			} else {
				// TODO state migration currently isn't possible.

				// NOTE: for heap backends, it is actually fine to proceed here without failing the restore,
				// since the state has already been deserialized to objects and we can just continue with
				// the new serializer; we're deliberately failing here for now to have equal functionality with
				// the RocksDB backend to avoid confusion for users.

				throw new StateMigrationException("State migration isn't supported, yet.");
			}
		}

		return stateTable;
	}

	@Override
	public <N> Stream<K> getKeys(String state, N namespace) {
		if (!stateTables.containsKey(state)) {
			return Stream.empty();
		}
		StateTable<K, N, ?> table = (StateTable<K, N, ?>) stateTables.get(state);
		return table.getKeys(namespace);
	}

	private boolean hasRegisteredState() {
		return !stateTables.isEmpty();
	}

	@Override
	public <N, V> InternalValueState<N, V> createValueState(
			TypeSerializer<N> namespaceSerializer,
			ValueStateDescriptor<V> stateDesc) throws Exception {

		StateTable<K, N, V> stateTable = tryRegisterStateTable(namespaceSerializer, stateDesc);
		return new HeapValueState<>(stateDesc, stateTable, keySerializer, namespaceSerializer);
	}

	@Override
	public <N, T> InternalListState<N, T> createListState(
			TypeSerializer<N> namespaceSerializer,
			ListStateDescriptor<T> stateDesc) throws Exception {

		StateTable<K, N, List<T>> stateTable = tryRegisterStateTable(namespaceSerializer, stateDesc);
		return new HeapListState<>(stateDesc, stateTable, keySerializer, namespaceSerializer);
	}

	@Override
	public <N, T> InternalReducingState<N, T> createReducingState(
			TypeSerializer<N> namespaceSerializer,
			ReducingStateDescriptor<T> stateDesc) throws Exception {

		StateTable<K, N, T> stateTable = tryRegisterStateTable(namespaceSerializer, stateDesc);
		return new HeapReducingState<>(stateDesc, stateTable, keySerializer, namespaceSerializer);
	}

	@Override
	public <N, T, ACC, R> InternalAggregatingState<N, T, R> createAggregatingState(
			TypeSerializer<N> namespaceSerializer,
			AggregatingStateDescriptor<T, ACC, R> stateDesc) throws Exception {

		StateTable<K, N, ACC> stateTable = tryRegisterStateTable(namespaceSerializer, stateDesc);
		return new HeapAggregatingState<>(stateDesc, stateTable, keySerializer, namespaceSerializer);
	}

	@Override
	public <N, T, ACC> InternalFoldingState<N, T, ACC> createFoldingState(
			TypeSerializer<N> namespaceSerializer,
			FoldingStateDescriptor<T, ACC> stateDesc) throws Exception {

		StateTable<K, N, ACC> stateTable = tryRegisterStateTable(namespaceSerializer, stateDesc);
		return new HeapFoldingState<>(stateDesc, stateTable, keySerializer, namespaceSerializer);
	}

	@Override
	public <N, UK, UV> InternalMapState<N, UK, UV> createMapState(TypeSerializer<N> namespaceSerializer,
			MapStateDescriptor<UK, UV> stateDesc) throws Exception {

		StateTable<K, N, HashMap<UK, UV>> stateTable = tryRegisterStateTable(
				stateDesc.getName(),
				stateDesc.getType(),
				namespaceSerializer,
				new HashMapSerializer<>(stateDesc.getKeySerializer(), stateDesc.getValueSerializer()));

		return new HeapMapState<>(stateDesc, stateTable, keySerializer, namespaceSerializer);
	}

	@Override
	@SuppressWarnings("unchecked")
	public  RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
			final long checkpointId,
			final long timestamp,
			final CheckpointStreamFactory streamFactory,
			CheckpointOptions checkpointOptions) {

		return snapshotStrategy.performSnapshot(checkpointId, timestamp, streamFactory, checkpointOptions);
	}

	@SuppressWarnings("deprecation")
	public void restore(Collection<KeyedStateHandle> restoredState) throws Exception {
		if (restoredState == null || restoredState.isEmpty()) {
			return;
		}

		LOG.info("Initializing heap keyed state backend from snapshot.");

		if (LOG.isDebugEnabled()) {
			LOG.debug("Restoring snapshot from state handles: {}.", restoredState);
		}

		restorePartitionedState(restoredState);
	}

	@SuppressWarnings({"unchecked"})
	private void restorePartitionedState(Collection<KeyedStateHandle> state) throws Exception {

		final Map<Integer, String> kvStatesById = new HashMap<>();
		int numRegisteredKvStates = 0;
		stateTables.clear();

		boolean keySerializerRestored = false;

		for (KeyedStateHandle keyedStateHandle : state) {

			if (keyedStateHandle == null) {
				continue;
			}

			if (!(keyedStateHandle instanceof KeyGroupsStateHandle)) {
				throw new IllegalStateException("Unexpected state handle type, " +
						"expected: " + KeyGroupsStateHandle.class +
						", but found: " + keyedStateHandle.getClass());
			}

			KeyGroupsStateHandle keyGroupsStateHandle = (KeyGroupsStateHandle) keyedStateHandle;
			FSDataInputStream fsDataInputStream = keyGroupsStateHandle.openInputStream();
			cancelStreamRegistry.registerCloseable(fsDataInputStream);

			try {
				DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(fsDataInputStream);

				KeyedBackendSerializationProxy<K> serializationProxy =
						new KeyedBackendSerializationProxy<>(userCodeClassLoader);

				serializationProxy.read(inView);

				if (!keySerializerRestored) {
					// check for key serializer compatibility; this also reconfigures the
					// key serializer to be compatible, if it is required and is possible
					if (CompatibilityUtil.resolveCompatibilityResult(
							serializationProxy.getKeySerializer(),
							UnloadableDummyTypeSerializer.class,
							serializationProxy.getKeySerializerConfigSnapshot(),
							keySerializer)
						.isRequiresMigration()) {

						// TODO replace with state migration; note that key hash codes need to remain the same after migration
						throw new StateMigrationException("The new key serializer is not compatible to read previous keys. " +
							"Aborting now since state migration is currently not available");
					}

					keySerializerRestored = true;
				}

				List<RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> restoredMetaInfos =
						serializationProxy.getStateMetaInfoSnapshots();

				for (RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?> restoredMetaInfo : restoredMetaInfos) {

					if (restoredMetaInfo.getStateSerializer() == null ||
							restoredMetaInfo.getStateSerializer() instanceof UnloadableDummyTypeSerializer) {

						// must fail now if the previous serializer cannot be restored because there is no serializer
						// capable of reading previous state
						// TODO when eager state registration is in place, we can try to get a convert deserializer
						// TODO from the newly registered serializer instead of simply failing here

						throw new IOException("Unable to restore keyed state [" + restoredMetaInfo.getName() + "]." +
							" For memory-backed keyed state, the previous serializer of the keyed state must be" +
							" present; the serializer could have been removed from the classpath, or its implementation" +
							" have changed and could not be loaded. This is a temporary restriction that will be fixed" +
							" in future versions.");
					}

					restoredKvStateMetaInfos.put(restoredMetaInfo.getName(), restoredMetaInfo);

					StateTable<K, ?, ?> stateTable = stateTables.get(restoredMetaInfo.getName());

					//important: only create a new table we did not already create it previously
					if (null == stateTable) {

						RegisteredKeyedBackendStateMetaInfo<?, ?> registeredKeyedBackendStateMetaInfo =
								new RegisteredKeyedBackendStateMetaInfo<>(
									restoredMetaInfo.getStateType(),
									restoredMetaInfo.getName(),
									restoredMetaInfo.getNamespaceSerializer(),
									restoredMetaInfo.getStateSerializer());

						stateTable = snapshotStrategy.newStateTable(registeredKeyedBackendStateMetaInfo);
						stateTables.put(restoredMetaInfo.getName(), stateTable);
						kvStatesById.put(numRegisteredKvStates, restoredMetaInfo.getName());
						++numRegisteredKvStates;
					} else {
						// TODO with eager state registration in place, check here for serializer migration strategies
					}
				}

				final StreamCompressionDecorator streamCompressionDecorator = serializationProxy.isUsingKeyGroupCompression() ?
					SnappyStreamCompressionDecorator.INSTANCE : UncompressedStreamCompressionDecorator.INSTANCE;

				for (Tuple2<Integer, Long> groupOffset : keyGroupsStateHandle.getGroupRangeOffsets()) {
					int keyGroupIndex = groupOffset.f0;
					long offset = groupOffset.f1;

					// Check that restored key groups all belong to the backend.
					Preconditions.checkState(keyGroupRange.contains(keyGroupIndex), "The key group must belong to the backend.");

					fsDataInputStream.seek(offset);

					int writtenKeyGroupIndex = inView.readInt();

					try (InputStream kgCompressionInStream =
							 streamCompressionDecorator.decorateWithCompression(fsDataInputStream)) {

						DataInputViewStreamWrapper kgCompressionInView =
							new DataInputViewStreamWrapper(kgCompressionInStream);

						Preconditions.checkState(writtenKeyGroupIndex == keyGroupIndex,
							"Unexpected key-group in restore.");

						for (int i = 0; i < restoredMetaInfos.size(); i++) {
							int kvStateId = kgCompressionInView.readShort();
							StateTable<K, ?, ?> stateTable = stateTables.get(kvStatesById.get(kvStateId));

							StateTableByKeyGroupReader keyGroupReader =
								StateTableByKeyGroupReaders.readerForVersion(
									stateTable,
									serializationProxy.getReadVersion());

							keyGroupReader.readMappingsInKeyGroup(kgCompressionInView, keyGroupIndex);
						}
					}
				}
			} finally {
				if (cancelStreamRegistry.unregisterCloseable(fsDataInputStream)) {
					IOUtils.closeQuietly(fsDataInputStream);
				}
			}
		}
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) {
		//Nothing to do
	}

	@Override
	public String toString() {
		return "HeapKeyedStateBackend";
	}

	/**
	 * Returns the total number of state entries across all keys/namespaces.
	 */
	@VisibleForTesting
	@SuppressWarnings("unchecked")
	@Override
	public int numStateEntries() {
		int sum = 0;
		for (StateTable<K, ?, ?> stateTable : stateTables.values()) {
			sum += stateTable.size();
		}
		return sum;
	}

	/**
	 * Returns the total number of state entries across all keys for the given namespace.
	 */
	@VisibleForTesting
	public int numStateEntries(Object namespace) {
		int sum = 0;
		for (StateTable<K, ?, ?> stateTable : stateTables.values()) {
			sum += stateTable.sizeOfNamespace(namespace);
		}
		return sum;
	}

	@Override
	public boolean supportsAsynchronousSnapshots() {
		return snapshotStrategy.isAsynchronous();
	}

	@VisibleForTesting
	public LocalRecoveryConfig getLocalRecoveryConfig() {
		return localRecoveryConfig;
	}

	private interface SnapshotStrategySynchronicityBehavior<K> {

		default void finalizeSnapshotBeforeReturnHook(Runnable runnable) {

		}

		default void logOperationCompleted(CheckpointStreamFactory streamFactory, long startTime) {

		}

		boolean isAsynchronous();

		<N, V> StateTable<K, N, V> newStateTable(RegisteredKeyedBackendStateMetaInfo<N, V> newMetaInfo);
	}

	private class AsyncSnapshotStrategySynchronicityBehavior implements SnapshotStrategySynchronicityBehavior<K> {

		@Override
		public void logOperationCompleted(CheckpointStreamFactory streamFactory, long startTime) {
			LOG.info("Heap backend snapshot ({}, asynchronous part) in thread {} took {} ms.",
				streamFactory, Thread.currentThread(), (System.currentTimeMillis() - startTime));
		}

		@Override
		public boolean isAsynchronous() {
			return true;
		}

		@Override
		public <N, V> StateTable<K, N, V> newStateTable(RegisteredKeyedBackendStateMetaInfo<N, V> newMetaInfo) {
			return new CopyOnWriteStateTable<>(HeapKeyedStateBackend.this, newMetaInfo);
		}
	}

	private class SyncSnapshotStrategySynchronicityBehavior implements SnapshotStrategySynchronicityBehavior<K> {

		@Override
		public void finalizeSnapshotBeforeReturnHook(Runnable runnable) {
			// this triggers a synchronous execution from the main checkpointing thread.
			runnable.run();
		}

		@Override
		public boolean isAsynchronous() {
			return false;
		}

		@Override
		public <N, V> StateTable<K, N, V> newStateTable(RegisteredKeyedBackendStateMetaInfo<N, V> newMetaInfo) {
			return new NestedMapsStateTable<>(HeapKeyedStateBackend.this, newMetaInfo);
		}
	}

	/**
	 * Base class for the snapshots of the heap backend that outlines the algorithm and offers some hooks to realize
	 * the concrete strategies. Subclasses must be threadsafe.
	 */
	private class HeapSnapshotStrategy
		implements SnapshotStrategy<SnapshotResult<KeyedStateHandle>>, SnapshotStrategySynchronicityBehavior<K> {

		private final SnapshotStrategySynchronicityBehavior<K> snapshotStrategySynchronicityTrait;

		public HeapSnapshotStrategy(
			SnapshotStrategySynchronicityBehavior<K> snapshotStrategySynchronicityTrait) {
			this.snapshotStrategySynchronicityTrait = snapshotStrategySynchronicityTrait;
		}

		@Override
		public RunnableFuture<SnapshotResult<KeyedStateHandle>> performSnapshot(
			long checkpointId,
			long timestamp,
			CheckpointStreamFactory primaryStreamFactory,
			CheckpointOptions checkpointOptions) {

			if (!hasRegisteredState()) {
				return DoneFuture.of(SnapshotResult.empty());
			}

			long syncStartTime = System.currentTimeMillis();

			Preconditions.checkState(stateTables.size() <= Short.MAX_VALUE,
				"Too many KV-States: " + stateTables.size() +
					". Currently at most " + Short.MAX_VALUE + " states are supported");

			List<RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> metaInfoSnapshots =
				new ArrayList<>(stateTables.size());

			final Map<String, Integer> kVStateToId = new HashMap<>(stateTables.size());

			final Map<StateTable<K, ?, ?>, StateTableSnapshot> cowStateStableSnapshots =
				new HashedMap(stateTables.size());

			for (Map.Entry<String, StateTable<K, ?, ?>> kvState : stateTables.entrySet()) {
				kVStateToId.put(kvState.getKey(), kVStateToId.size());
				StateTable<K, ?, ?> stateTable = kvState.getValue();
				if (null != stateTable) {
					metaInfoSnapshots.add(stateTable.getMetaInfo().snapshot());
					cowStateStableSnapshots.put(stateTable, stateTable.createSnapshot());
				}
			}

			final KeyedBackendSerializationProxy<K> serializationProxy =
				new KeyedBackendSerializationProxy<>(
					keySerializer,
					metaInfoSnapshots,
					!Objects.equals(UncompressedStreamCompressionDecorator.INSTANCE, keyGroupCompressionDecorator));

			final SupplierWithException<CheckpointStreamWithResultProvider, Exception> checkpointStreamSupplier =

				LocalRecoveryConfig.LocalRecoveryMode.ENABLE_FILE_BASED.equals(
					localRecoveryConfig.getLocalRecoveryMode()) ?

					() -> CheckpointStreamWithResultProvider.createDuplicatingStream(
						checkpointId,
						CheckpointedStateScope.EXCLUSIVE,
						primaryStreamFactory,
						localRecoveryConfig.getLocalStateDirectoryProvider()) :

					() -> CheckpointStreamWithResultProvider.createSimpleStream(
						CheckpointedStateScope.EXCLUSIVE,
						primaryStreamFactory);

			//--------------------------------------------------- this becomes the end of sync part

			// implementation of the async IO operation, based on FutureTask
			final AbstractAsyncCallableWithResources<SnapshotResult<KeyedStateHandle>> ioCallable =
				new AbstractAsyncCallableWithResources<SnapshotResult<KeyedStateHandle>>() {

					CheckpointStreamWithResultProvider streamAndResultExtractor = null;

					@Override
					protected void acquireResources() throws Exception {
						streamAndResultExtractor = checkpointStreamSupplier.get();
						cancelStreamRegistry.registerCloseable(streamAndResultExtractor);
					}

					@Override
					protected void releaseResources() {

						unregisterAndCloseStreamAndResultExtractor();

						for (StateTableSnapshot tableSnapshot : cowStateStableSnapshots.values()) {
							tableSnapshot.release();
						}
					}

					@Override
					protected void stopOperation() {
						unregisterAndCloseStreamAndResultExtractor();
					}

					private void unregisterAndCloseStreamAndResultExtractor() {
						if (cancelStreamRegistry.unregisterCloseable(streamAndResultExtractor)) {
							IOUtils.closeQuietly(streamAndResultExtractor);
							streamAndResultExtractor = null;
						}
					}

					@Nonnull
					@Override
					protected SnapshotResult<KeyedStateHandle> performOperation() throws Exception {

						long startTime = System.currentTimeMillis();

						CheckpointStreamFactory.CheckpointStateOutputStream localStream =
							this.streamAndResultExtractor.getCheckpointOutputStream();

						DataOutputViewStreamWrapper outView = new DataOutputViewStreamWrapper(localStream);
						serializationProxy.write(outView);

						long[] keyGroupRangeOffsets = new long[keyGroupRange.getNumberOfKeyGroups()];

						for (int keyGroupPos = 0; keyGroupPos < keyGroupRange.getNumberOfKeyGroups(); ++keyGroupPos) {
							int keyGroupId = keyGroupRange.getKeyGroupId(keyGroupPos);
							keyGroupRangeOffsets[keyGroupPos] = localStream.getPos();
							outView.writeInt(keyGroupId);

							for (Map.Entry<String, StateTable<K, ?, ?>> kvState : stateTables.entrySet()) {
								try (OutputStream kgCompressionOut = keyGroupCompressionDecorator.decorateWithCompression(localStream)) {
									DataOutputViewStreamWrapper kgCompressionView = new DataOutputViewStreamWrapper(kgCompressionOut);
									kgCompressionView.writeShort(kVStateToId.get(kvState.getKey()));
									cowStateStableSnapshots.get(kvState.getValue()).writeMappingsInKeyGroup(kgCompressionView, keyGroupId);
								} // this will just close the outer compression stream
							}
						}

						if (cancelStreamRegistry.unregisterCloseable(streamAndResultExtractor)) {
							KeyGroupRangeOffsets kgOffs = new KeyGroupRangeOffsets(keyGroupRange, keyGroupRangeOffsets);
							SnapshotResult<StreamStateHandle> result =
								streamAndResultExtractor.closeAndFinalizeCheckpointStreamResult();
							streamAndResultExtractor = null;
							logOperationCompleted(primaryStreamFactory, startTime);
							return CheckpointStreamWithResultProvider.toKeyedStateHandleSnapshotResult(result, kgOffs);
						}

						return SnapshotResult.empty();
					}
				};

			AsyncStoppableTaskWithCallback<SnapshotResult<KeyedStateHandle>> task =
				AsyncStoppableTaskWithCallback.from(ioCallable);

			finalizeSnapshotBeforeReturnHook(task);

			LOG.info("Heap backend snapshot (" + primaryStreamFactory + ", synchronous part) in thread " +
				Thread.currentThread() + " took " + (System.currentTimeMillis() - syncStartTime) + " ms.");

			return task;
		}

		@Override
		public void finalizeSnapshotBeforeReturnHook(Runnable runnable) {
			snapshotStrategySynchronicityTrait.finalizeSnapshotBeforeReturnHook(runnable);
		}

		@Override
		public void logOperationCompleted(CheckpointStreamFactory streamFactory, long startTime) {
			snapshotStrategySynchronicityTrait.logOperationCompleted(streamFactory, startTime);
		}

		@Override
		public boolean isAsynchronous() {
			return snapshotStrategySynchronicityTrait.isAsynchronous();
		}

		@Override
		public <N, V> StateTable<K, N, V> newStateTable(RegisteredKeyedBackendStateMetaInfo<N, V> newMetaInfo) {
			return snapshotStrategySynchronicityTrait.newStateTable(newMetaInfo);
		}
	}
}
