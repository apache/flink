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
import org.apache.flink.api.common.state.State;
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
import org.apache.flink.runtime.state.KeyExtractorFunction;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredPriorityQueueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SnappyStreamCompressionDecorator;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategy;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.runtime.state.StateSnapshotKeyGroupReader;
import org.apache.flink.runtime.state.StateSnapshotRestore;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StateMigrationException;
import org.apache.flink.util.function.SupplierWithException;

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
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A {@link AbstractKeyedStateBackend} that keeps state on the Java Heap and will serialize state to
 * streams provided by a {@link CheckpointStreamFactory} upon checkpointing.
 *
 * @param <K> The key by which state is keyed.
 */
public class HeapKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {

	private static final Logger LOG = LoggerFactory.getLogger(HeapKeyedStateBackend.class);

	private static final Map<Class<? extends StateDescriptor>, StateFactory> STATE_FACTORIES =
		Stream.of(
			Tuple2.of(ValueStateDescriptor.class, (StateFactory) HeapValueState::create),
			Tuple2.of(ListStateDescriptor.class, (StateFactory) HeapListState::create),
			Tuple2.of(MapStateDescriptor.class, (StateFactory) HeapMapState::create),
			Tuple2.of(AggregatingStateDescriptor.class, (StateFactory) HeapAggregatingState::create),
			Tuple2.of(ReducingStateDescriptor.class, (StateFactory) HeapReducingState::create),
			Tuple2.of(FoldingStateDescriptor.class, (StateFactory) HeapFoldingState::create)
		).collect(Collectors.toMap(t -> t.f0, t -> t.f1));

	/**
	 * Map of registered Key/Value states.
	 */
	private final Map<String, StateTable<K, ?, ?>> registeredKVStates;

	/**
	 * Map of registered priority queue set states.
	 */
	private final Map<String, HeapPriorityQueueSnapshotRestoreWrapper> registeredPQStates;

	/**
	 * Map of state names to their corresponding restored state meta info.
	 *
	 * <p>TODO this map can be removed when eager-state registration is in place.
	 * TODO we currently need this cached to check state migration strategies when new serializers are registered.
	 */
	private final Map<StateUID, StateMetaInfoSnapshot> restoredStateMetaInfo;

	/**
	 * The configuration for local recovery.
	 */
	private final LocalRecoveryConfig localRecoveryConfig;

	/**
	 * The snapshot strategy for this backend. This determines, e.g., if snapshots are synchronous or asynchronous.
	 */
	private final HeapSnapshotStrategy snapshotStrategy;

	/**
	 * Factory for state that is organized as priority queue.
	 */
	private final HeapPriorityQueueSetFactory priorityQueueSetFactory;

	public HeapKeyedStateBackend(
			TaskKvStateRegistry kvStateRegistry,
			TypeSerializer<K> keySerializer,
			ClassLoader userCodeClassLoader,
			int numberOfKeyGroups,
			KeyGroupRange keyGroupRange,
			boolean asynchronousSnapshots,
			ExecutionConfig executionConfig,
			LocalRecoveryConfig localRecoveryConfig,
			HeapPriorityQueueSetFactory priorityQueueSetFactory,
			TtlTimeProvider ttlTimeProvider) {

		super(kvStateRegistry, keySerializer, userCodeClassLoader,
			numberOfKeyGroups, keyGroupRange, executionConfig, ttlTimeProvider);

		this.registeredKVStates = new HashMap<>();
		this.registeredPQStates = new HashMap<>();
		this.localRecoveryConfig = Preconditions.checkNotNull(localRecoveryConfig);

		SnapshotStrategySynchronicityBehavior<K> synchronicityTrait = asynchronousSnapshots ?
			new AsyncSnapshotStrategySynchronicityBehavior() :
			new SyncSnapshotStrategySynchronicityBehavior();

		this.snapshotStrategy = new HeapSnapshotStrategy(synchronicityTrait);
		LOG.info("Initializing heap keyed state backend with stream factory.");
		this.restoredStateMetaInfo = new HashMap<>();
		this.priorityQueueSetFactory = priorityQueueSetFactory;
	}

	// ------------------------------------------------------------------------
	//  state backend operations
	// ------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	@Nonnull
	@Override
	public <T extends HeapPriorityQueueElement & PriorityComparable & Keyed> KeyGroupedInternalPriorityQueue<T> create(
		@Nonnull String stateName,
		@Nonnull TypeSerializer<T> byteOrderedElementSerializer) {

		final HeapPriorityQueueSnapshotRestoreWrapper existingState = registeredPQStates.get(stateName);

		if (existingState != null) {
			// TODO we implement the simple way of supporting the current functionality, mimicking keyed state
			// because this should be reworked in FLINK-9376 and then we should have a common algorithm over
			// StateMetaInfoSnapshot that avoids this code duplication.
			StateMetaInfoSnapshot restoredMetaInfoSnapshot =
				restoredStateMetaInfo.get(StateUID.of(stateName, StateMetaInfoSnapshot.BackendStateType.PRIORITY_QUEUE));

			Preconditions.checkState(
				restoredMetaInfoSnapshot != null,
				"Requested to check compatibility of a restored RegisteredKeyedBackendStateMetaInfo," +
					" but its corresponding restored snapshot cannot be found.");

			StateMetaInfoSnapshot.CommonSerializerKeys serializerKey =
				StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER;

			CompatibilityResult<T> compatibilityResult = CompatibilityUtil.resolveCompatibilityResult(
				restoredMetaInfoSnapshot.getTypeSerializer(serializerKey),
				null,
				restoredMetaInfoSnapshot.getTypeSerializerConfigSnapshot(serializerKey),
				byteOrderedElementSerializer);

			if (compatibilityResult.isRequiresMigration()) {
				throw new FlinkRuntimeException(StateMigrationException.notSupported());
			} else {
				registeredPQStates.put(
					stateName,
					existingState.forUpdatedSerializer(byteOrderedElementSerializer));
			}

			return existingState.getPriorityQueue();
		} else {
			final RegisteredPriorityQueueStateBackendMetaInfo<T> metaInfo =
				new RegisteredPriorityQueueStateBackendMetaInfo<>(stateName, byteOrderedElementSerializer);
			return createInternal(metaInfo);
		}
	}

	@Nonnull
	private <T extends HeapPriorityQueueElement & PriorityComparable & Keyed> KeyGroupedInternalPriorityQueue<T> createInternal(
		RegisteredPriorityQueueStateBackendMetaInfo<T> metaInfo) {

		final String stateName = metaInfo.getName();
		final HeapPriorityQueueSet<T> priorityQueue = priorityQueueSetFactory.create(
			stateName,
			metaInfo.getElementSerializer());

		HeapPriorityQueueSnapshotRestoreWrapper<T> wrapper =
			new HeapPriorityQueueSnapshotRestoreWrapper<>(
				priorityQueue,
				metaInfo,
				KeyExtractorFunction.forKeyedObjects(),
				keyGroupRange,
				numberOfKeyGroups);

		registeredPQStates.put(stateName, wrapper);
		return priorityQueue;
	}

	private <N, V> StateTable<K, N, V> tryRegisterStateTable(
			TypeSerializer<N> namespaceSerializer, StateDescriptor<?, V> stateDesc) throws StateMigrationException {

		@SuppressWarnings("unchecked")
		StateTable<K, N, V> stateTable = (StateTable<K, N, V>) registeredKVStates.get(stateDesc.getName());

		RegisteredKeyValueStateBackendMetaInfo<N, V> newMetaInfo;
		if (stateTable != null) {
			@SuppressWarnings("unchecked")
			StateMetaInfoSnapshot restoredMetaInfoSnapshot =
				restoredStateMetaInfo.get(
					StateUID.of(stateDesc.getName(), StateMetaInfoSnapshot.BackendStateType.KEY_VALUE));

			Preconditions.checkState(
				restoredMetaInfoSnapshot != null,
				"Requested to check compatibility of a restored RegisteredKeyedBackendStateMetaInfo," +
					" but its corresponding restored snapshot cannot be found.");

			newMetaInfo = RegisteredKeyValueStateBackendMetaInfo.resolveKvStateCompatibility(
				restoredMetaInfoSnapshot,
				namespaceSerializer,
				stateDesc);

			stateTable.setMetaInfo(newMetaInfo);
		} else {
			newMetaInfo = new RegisteredKeyValueStateBackendMetaInfo<>(
				stateDesc.getType(),
				stateDesc.getName(),
				namespaceSerializer,
				stateDesc.getSerializer());

			stateTable = snapshotStrategy.newStateTable(newMetaInfo);
			registeredKVStates.put(stateDesc.getName(), stateTable);
		}

		return stateTable;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <N> Stream<K> getKeys(String state, N namespace) {
		if (!registeredKVStates.containsKey(state)) {
			return Stream.empty();
		}

		final StateSnapshotRestore stateSnapshotRestore = registeredKVStates.get(state);
		StateTable<K, N, ?> table = (StateTable<K, N, ?>) stateSnapshotRestore;
		return table.getKeys(namespace);
	}

	private boolean hasRegisteredState() {
		return !(registeredKVStates.isEmpty() && registeredPQStates.isEmpty());
	}

	@Override
	public <N, SV, S extends State, IS extends S> IS createInternalState(
		TypeSerializer<N> namespaceSerializer,
		StateDescriptor<S, SV> stateDesc) throws Exception {
		StateFactory stateFactory = STATE_FACTORIES.get(stateDesc.getClass());
		if (stateFactory == null) {
			String message = String.format("State %s is not supported by %s",
				stateDesc.getClass(), this.getClass());
			throw new FlinkRuntimeException(message);
		}
		StateTable<K, N, SV> stateTable = tryRegisterStateTable(namespaceSerializer, stateDesc);
		return stateFactory.createState(stateDesc, stateTable, keySerializer);
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

		final Map<Integer, StateMetaInfoSnapshot> kvStatesById = new HashMap<>();
		registeredKVStates.clear();
		registeredPQStates.clear();

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

				// isSerializerPresenceRequired flag is set to true, since for the heap state backend,
				// deserialization of state happens eagerly at restore time
				KeyedBackendSerializationProxy<K> serializationProxy =
						new KeyedBackendSerializationProxy<>(userCodeClassLoader, true);

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

				List<StateMetaInfoSnapshot> restoredMetaInfos =
					serializationProxy.getStateMetaInfoSnapshots();

				createOrCheckStateForMetaInfo(restoredMetaInfos, kvStatesById);

				readStateHandleStateData(
					fsDataInputStream,
					inView,
					keyGroupsStateHandle.getGroupRangeOffsets(),
					kvStatesById, restoredMetaInfos.size(),
					serializationProxy.getReadVersion(),
					serializationProxy.isUsingKeyGroupCompression());
			} finally {
				if (cancelStreamRegistry.unregisterCloseable(fsDataInputStream)) {
					IOUtils.closeQuietly(fsDataInputStream);
				}
			}
		}
	}

	private void readStateHandleStateData(
		FSDataInputStream fsDataInputStream,
		DataInputViewStreamWrapper inView,
		KeyGroupRangeOffsets keyGroupOffsets,
		Map<Integer, StateMetaInfoSnapshot> kvStatesById,
		int numStates,
		int readVersion,
		boolean isCompressed) throws IOException {

		final StreamCompressionDecorator streamCompressionDecorator = isCompressed ?
			SnappyStreamCompressionDecorator.INSTANCE : UncompressedStreamCompressionDecorator.INSTANCE;

		for (Tuple2<Integer, Long> groupOffset : keyGroupOffsets) {
			int keyGroupIndex = groupOffset.f0;
			long offset = groupOffset.f1;

			// Check that restored key groups all belong to the backend.
			Preconditions.checkState(keyGroupRange.contains(keyGroupIndex), "The key group must belong to the backend.");

			fsDataInputStream.seek(offset);

			int writtenKeyGroupIndex = inView.readInt();
			Preconditions.checkState(writtenKeyGroupIndex == keyGroupIndex,
				"Unexpected key-group in restore.");

			try (InputStream kgCompressionInStream =
					 streamCompressionDecorator.decorateWithCompression(fsDataInputStream)) {

				readKeyGroupStateData(
					kgCompressionInStream,
					kvStatesById,
					keyGroupIndex,
					numStates,
					readVersion);
			}
		}
	}

	private void readKeyGroupStateData(
		InputStream inputStream,
		Map<Integer, StateMetaInfoSnapshot> kvStatesById,
		int keyGroupIndex,
		int numStates,
		int readVersion) throws IOException {

		DataInputViewStreamWrapper inView =
			new DataInputViewStreamWrapper(inputStream);

		for (int i = 0; i < numStates; i++) {

			final int kvStateId = inView.readShort();
			final StateMetaInfoSnapshot stateMetaInfoSnapshot = kvStatesById.get(kvStateId);
			final StateSnapshotRestore registeredState;

			switch (stateMetaInfoSnapshot.getBackendStateType()) {
				case KEY_VALUE:
					registeredState = registeredKVStates.get(stateMetaInfoSnapshot.getName());
					break;
				case PRIORITY_QUEUE:
					registeredState = registeredPQStates.get(stateMetaInfoSnapshot.getName());
					break;
				default:
					throw new IllegalStateException("Unexpected state type: " +
						stateMetaInfoSnapshot.getBackendStateType() + ".");
			}

			StateSnapshotKeyGroupReader keyGroupReader = registeredState.keyGroupReader(readVersion);
			keyGroupReader.readMappingsInKeyGroup(inView, keyGroupIndex);
		}
	}

	private void createOrCheckStateForMetaInfo(
		List<StateMetaInfoSnapshot> restoredMetaInfo,
		Map<Integer, StateMetaInfoSnapshot> kvStatesById) {

		for (StateMetaInfoSnapshot metaInfoSnapshot : restoredMetaInfo) {
			restoredStateMetaInfo.put(
				StateUID.of(metaInfoSnapshot.getName(), metaInfoSnapshot.getBackendStateType()),
				metaInfoSnapshot);

			final StateSnapshotRestore registeredState;

			switch (metaInfoSnapshot.getBackendStateType()) {
				case KEY_VALUE:
					registeredState = registeredKVStates.get(metaInfoSnapshot.getName());
					if (registeredState == null) {
						RegisteredKeyValueStateBackendMetaInfo<?, ?> registeredKeyedBackendStateMetaInfo =
							new RegisteredKeyValueStateBackendMetaInfo<>(metaInfoSnapshot);
						registeredKVStates.put(
							metaInfoSnapshot.getName(),
							snapshotStrategy.newStateTable(registeredKeyedBackendStateMetaInfo));
					}
					break;
				case PRIORITY_QUEUE:
					registeredState = registeredPQStates.get(metaInfoSnapshot.getName());
					if (registeredState == null) {
						createInternal(new RegisteredPriorityQueueStateBackendMetaInfo<>(metaInfoSnapshot));
					}
					break;
				default:
					throw new IllegalStateException("Unexpected state type: " +
						metaInfoSnapshot.getBackendStateType() + ".");
			}

			if (registeredState == null) {
				kvStatesById.put(kvStatesById.size(), metaInfoSnapshot);
			}
		}
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) {
		//Nothing to do
	}

	@Override
	public <N, S extends State, T> void applyToAllKeys(
			final N namespace,
			final TypeSerializer<N> namespaceSerializer,
			final StateDescriptor<S, T> stateDescriptor,
			final KeyedStateFunction<K, S> function) throws Exception {

		try (Stream<K> keyStream = getKeys(stateDescriptor.getName(), namespace)) {

			// we copy the keys into list to avoid the concurrency problem
			// when state.clear() is invoked in function.process().
			final List<K> keys = keyStream.collect(Collectors.toList());

			final S state = getPartitionedState(
					namespace,
					namespaceSerializer,
					stateDescriptor);

			for (K key : keys) {
				setCurrentKey(key);
				function.process(key, state);
			}
		}
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
	public int numKeyValueStateEntries() {
		int sum = 0;
		for (StateSnapshotRestore state : registeredKVStates.values()) {
			sum += ((StateTable<?, ?, ?>) state).size();
		}
		return sum;
	}

	/**
	 * Returns the total number of state entries across all keys for the given namespace.
	 */
	@VisibleForTesting
	public int numKeyValueStateEntries(Object namespace) {
		int sum = 0;
		for (StateTable<?, ?, ?> state : registeredKVStates.values()) {
			sum += state.sizeOfNamespace(namespace);
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

		<N, V> StateTable<K, N, V> newStateTable(RegisteredKeyValueStateBackendMetaInfo<N, V> newMetaInfo);
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
		public <N, V> StateTable<K, N, V> newStateTable(RegisteredKeyValueStateBackendMetaInfo<N, V> newMetaInfo) {
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
		public <N, V> StateTable<K, N, V> newStateTable(RegisteredKeyValueStateBackendMetaInfo<N, V> newMetaInfo) {
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

		HeapSnapshotStrategy(
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
					keySerializer,
					metaInfoSnapshots,
					!Objects.equals(UncompressedStreamCompressionDecorator.INSTANCE, keyGroupCompressionDecorator));

			final SupplierWithException<CheckpointStreamWithResultProvider, Exception> checkpointStreamSupplier =

				localRecoveryConfig.isLocalRecoveryEnabled() ?

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

						for (StateSnapshot tableSnapshot : cowStateStableSnapshots.values()) {
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

							for (Map.Entry<StateUID, StateSnapshot> stateSnapshot :
								cowStateStableSnapshots.entrySet()) {
								StateSnapshot.StateKeyGroupWriter partitionedSnapshot =

									stateSnapshot.getValue().getKeyGroupWriter();
								try (OutputStream kgCompressionOut = keyGroupCompressionDecorator.decorateWithCompression(localStream)) {
									DataOutputViewStreamWrapper kgCompressionView = new DataOutputViewStreamWrapper(kgCompressionOut);
									kgCompressionView.writeShort(stateNamesToId.get(stateSnapshot.getKey()));
									partitionedSnapshot.writeStateInKeyGroup(kgCompressionView, keyGroupId);
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
		public <N, V> StateTable<K, N, V> newStateTable(RegisteredKeyValueStateBackendMetaInfo<N, V> newMetaInfo) {
			return snapshotStrategySynchronicityTrait.newStateTable(newMetaInfo);
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
	}

	private interface StateFactory {
		<K, N, SV, S extends State, IS extends S> IS createState(
			StateDescriptor<S, SV> stateDesc,
			StateTable<K, N, SV> stateTable,
			TypeSerializer<K> keySerializer) throws Exception;
	}

	/**
	 * Unique identifier for registered state in this backend.
	 */
	private static final class StateUID {

		@Nonnull
		private final String stateName;

		@Nonnull
		private final StateMetaInfoSnapshot.BackendStateType stateType;

		StateUID(@Nonnull String stateName, @Nonnull StateMetaInfoSnapshot.BackendStateType stateType) {
			this.stateName = stateName;
			this.stateType = stateType;
		}

		@Nonnull
		public String getStateName() {
			return stateName;
		}

		@Nonnull
		public StateMetaInfoSnapshot.BackendStateType getStateType() {
			return stateType;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			StateUID uid = (StateUID) o;
			return Objects.equals(getStateName(), uid.getStateName()) &&
				getStateType() == uid.getStateType();
		}

		@Override
		public int hashCode() {
			return Objects.hash(getStateName(), getStateType());
		}

		public static StateUID of(@Nonnull String stateName, @Nonnull StateMetaInfoSnapshot.BackendStateType stateType) {
			return new StateUID(stateName, stateType);
		}
	}
}
