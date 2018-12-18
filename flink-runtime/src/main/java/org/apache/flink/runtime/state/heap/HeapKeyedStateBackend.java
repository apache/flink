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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractSnapshotStrategy;
import org.apache.flink.runtime.state.AsyncSnapshotCallable;
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
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.runtime.state.StateSnapshotKeyGroupReader;
import org.apache.flink.runtime.state.StateSnapshotRestore;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;
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
import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.FutureTask;
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

			TypeSerializerSchemaCompatibility<T> compatibilityResult =
				existingState.getMetaInfo().updateElementSerializer(byteOrderedElementSerializer);

			if (compatibilityResult.isIncompatible()) {
				throw new FlinkRuntimeException(new StateMigrationException("For heap backends, the new priority queue serializer must not be incompatible."));
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
			TypeSerializer<N> namespaceSerializer,
			StateDescriptor<?, V> stateDesc,
			@Nullable StateSnapshotTransformer<V> snapshotTransformer) throws StateMigrationException {

		@SuppressWarnings("unchecked")
		StateTable<K, N, V> stateTable = (StateTable<K, N, V>) registeredKVStates.get(stateDesc.getName());

		TypeSerializer<V> newStateSerializer = stateDesc.getSerializer();

		if (stateTable != null) {
			RegisteredKeyValueStateBackendMetaInfo<N, V> restoredKvMetaInfo = stateTable.getMetaInfo();

			restoredKvMetaInfo.updateSnapshotTransformer(snapshotTransformer);

			TypeSerializerSchemaCompatibility<N> namespaceCompatibility =
				restoredKvMetaInfo.updateNamespaceSerializer(namespaceSerializer);
			if (!namespaceCompatibility.isCompatibleAsIs()) {
				throw new StateMigrationException("For heap backends, the new namespace serializer must be compatible.");
			}

			restoredKvMetaInfo.checkStateMetaInfo(stateDesc);

			TypeSerializerSchemaCompatibility<V> stateCompatibility =
				restoredKvMetaInfo.updateStateSerializer(newStateSerializer);

			if (stateCompatibility.isIncompatible()) {
				throw new StateMigrationException("For heap backends, the new state serializer must not be incompatible.");
			}

			stateTable.setMetaInfo(restoredKvMetaInfo);
		} else {
			RegisteredKeyValueStateBackendMetaInfo<N, V> newMetaInfo = new RegisteredKeyValueStateBackendMetaInfo<>(
				stateDesc.getType(),
				stateDesc.getName(),
				namespaceSerializer,
				newStateSerializer,
				snapshotTransformer);

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
	@Nonnull
	public <N, SV, SEV, S extends State, IS extends S> IS createInternalState(
		@Nonnull TypeSerializer<N> namespaceSerializer,
		@Nonnull StateDescriptor<S, SV> stateDesc,
		@Nonnull StateSnapshotTransformFactory<SEV> snapshotTransformFactory) throws Exception {
		StateFactory stateFactory = STATE_FACTORIES.get(stateDesc.getClass());
		if (stateFactory == null) {
			String message = String.format("State %s is not supported by %s",
				stateDesc.getClass(), this.getClass());
			throw new FlinkRuntimeException(message);
		}
		StateTable<K, N, SV> stateTable = tryRegisterStateTable(
			namespaceSerializer, stateDesc, getStateSnapshotTransformer(stateDesc, snapshotTransformFactory));
		return stateFactory.createState(stateDesc, stateTable, keySerializer);
	}

	@SuppressWarnings("unchecked")
	private <SV, SEV> StateSnapshotTransformer<SV> getStateSnapshotTransformer(
		StateDescriptor<?, SV> stateDesc,
		StateSnapshotTransformFactory<SEV> snapshotTransformFactory) {
		Optional<StateSnapshotTransformer<SEV>> original = snapshotTransformFactory.createForDeserializedState();
		if (original.isPresent()) {
			if (stateDesc instanceof ListStateDescriptor) {
				return (StateSnapshotTransformer<SV>) new StateSnapshotTransformer
					.ListStateSnapshotTransformer<>(original.get());
			} else if (stateDesc instanceof MapStateDescriptor) {
				return (StateSnapshotTransformer<SV>) new StateSnapshotTransformer
					.MapStateSnapshotTransformer<>(original.get());
			} else {
				return (StateSnapshotTransformer<SV>) original.get();
			}
		} else {
			return null;
		}
	}

	@Nonnull
	@Override
	@SuppressWarnings("unchecked")
	public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
		final long checkpointId,
		final long timestamp,
		@Nonnull final CheckpointStreamFactory streamFactory,
		@Nonnull CheckpointOptions checkpointOptions) throws IOException {

		long startTime = System.currentTimeMillis();

		final RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotRunner =
			snapshotStrategy.snapshot(checkpointId, timestamp, streamFactory, checkpointOptions);

		snapshotStrategy.logSyncCompleted(streamFactory, startTime);
		return snapshotRunner;
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

				KeyedBackendSerializationProxy<K> serializationProxy =
						new KeyedBackendSerializationProxy<>(userCodeClassLoader);

				serializationProxy.read(inView);

				if (!keySerializerRestored) {
					// check for key serializer compatibility; this also reconfigures the
					// key serializer to be compatible, if it is required and is possible
					if (!serializationProxy.getKeySerializerConfigSnapshot()
							.resolveSchemaCompatibility(keySerializer).isCompatibleAsIs()) {
						throw new StateMigrationException("The new key serializer must be compatible.");
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


		boolean isAsynchronous();

		<N, V> StateTable<K, N, V> newStateTable(RegisteredKeyValueStateBackendMetaInfo<N, V> newMetaInfo);
	}

	private class AsyncSnapshotStrategySynchronicityBehavior implements SnapshotStrategySynchronicityBehavior<K> {

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
		extends AbstractSnapshotStrategy<KeyedStateHandle> implements SnapshotStrategySynchronicityBehavior<K> {

		private final SnapshotStrategySynchronicityBehavior<K> snapshotStrategySynchronicityTrait;

		HeapSnapshotStrategy(
			SnapshotStrategySynchronicityBehavior<K> snapshotStrategySynchronicityTrait) {
			super("Heap backend snapshot");
			this.snapshotStrategySynchronicityTrait = snapshotStrategySynchronicityTrait;
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

			final AsyncSnapshotCallable<SnapshotResult<KeyedStateHandle>> asyncSnapshotCallable =
				new AsyncSnapshotCallable<SnapshotResult<KeyedStateHandle>>() {
					@Override
					protected SnapshotResult<KeyedStateHandle> callInternal() throws Exception {

						final CheckpointStreamWithResultProvider streamWithResultProvider =
							checkpointStreamSupplier.get();

						registerCloseableForCancellation(streamWithResultProvider);

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

						if (unregisterCloseableFromCancellation(streamWithResultProvider)) {
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
