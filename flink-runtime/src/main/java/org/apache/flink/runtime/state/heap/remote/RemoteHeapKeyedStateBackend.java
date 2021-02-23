package org.apache.flink.runtime.state.heap.remote;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.AsyncValueStateDescriptor;
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
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyExtractorFunction;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredPriorityQueueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSet;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.heap.HeapSnapshotStrategy;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.heap.StateTable;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.StateMigrationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class RemoteHeapKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {

	private static final Logger LOG = LoggerFactory.getLogger(RemoteHeapKeyedStateBackend.class);

	protected final RemoteKVSyncClient syncRemClient;

	protected final RemoteKVAsyncClient asyncRemClient;

	private final RemoteHeapSerializedCompositeKeyBuilder<K> sharedREMKeyBuilder;

	/** Number of bytes required to prefix the key groups. */
	private final int keyGroupPrefixBytes;

	/**
	 * The max memory size for one batch in remote heap.
	 */
	private final long writeBatchSize;

	/**
	 * Information about the k/v states, maintained in the order as we create them. This is used to retrieve the
	 * column family that is used for a state and also for sanity checks when restoring.
	 */
	private final LinkedHashMap<String, RemoteHeapKvStateInfo> kvStateInformation;

	/**
	 * Map of registered priority queue set states.
	 */
	private final Map<String, HeapPriorityQueueSnapshotRestoreWrapper> registeredPQStates;

	/**
	 * Factory for state that is organized as priority queue.
	 */
	private final HeapPriorityQueueSetFactory priorityQueueSetFactory;

	/**
	 * The snapshot strategy for this backend. This determines, e.g., if snapshots are synchronous or asynchronous.
	 */
	private final HeapSnapshotStrategy<K> snapshotStrategy;

	/** Shared wrapper for batch writes to the RocksDB instance. */
	private final RemoteHeapWriteBatchWrapper writeBatchWrapper;

	private static final Map<Class<? extends StateDescriptor>, RemoteHeapKeyedStateBackend.StateFactory> STATE_FACTORIES =
		Stream.of(
			Tuple2.of(
				ValueStateDescriptor.class,
				(RemoteHeapKeyedStateBackend.StateFactory) RemoteHeapValueState::create),
			Tuple2.of(
				AsyncValueStateDescriptor.class,
				(RemoteHeapKeyedStateBackend.StateFactory) RemoteHeapAsyncValueState::create),
			Tuple2.of(
				ListStateDescriptor.class,
				(RemoteHeapKeyedStateBackend.StateFactory) RemoteHeapListState::create),
			Tuple2.of(
				MapStateDescriptor.class,
				(RemoteHeapKeyedStateBackend.StateFactory) RemoteHeapMapState::create),
			Tuple2.of(
				AggregatingStateDescriptor.class,
				(RemoteHeapKeyedStateBackend.StateFactory) RemoteHeapAggregatingState::create),
			Tuple2.of(
				ReducingStateDescriptor.class,
				(RemoteHeapKeyedStateBackend.StateFactory) RemoteHeapReducingState::create),
			Tuple2.of(
				FoldingStateDescriptor.class,
				(RemoteHeapKeyedStateBackend.StateFactory) RemoteHeapFoldingState::create)
		).collect(Collectors.toMap(t -> t.f0, t -> t.f1));

	public RemoteHeapKeyedStateBackend(
		TaskKvStateRegistry kvStateRegistry,
		TypeSerializer<K> keySerializer,
		ClassLoader userCodeClassLoader,
		ExecutionConfig executionConfig,
		TtlTimeProvider ttlTimeProvider,
		CloseableRegistry cancelStreamRegistry,
		StreamCompressionDecorator keyGroupCompressionDecorator,
		Map<String, StateTable<K, ?, ?>> registeredKVStates,
		Map<String, HeapPriorityQueueSnapshotRestoreWrapper> registeredPQStates,
		LocalRecoveryConfig localRecoveryConfig,
		HeapPriorityQueueSetFactory priorityQueueSetFactory,
		HeapSnapshotStrategy<K> snapshotStrategy,
		InternalKeyContext<K> keyContext,
		RemoteHeapSerializedCompositeKeyBuilder<K> sharedREMKeyBuilder,
		int keyGroupPrefixBytes,
		@Nonnegative long writeBatchSize,
		LinkedHashMap<String, RemoteHeapKvStateInfo> kvStateInformation,
		RemoteHeapWriteBatchWrapper writeBatchWrapper,
		RemoteKVSyncClient syncRemClient, RemoteKVAsyncClient asyncRemClient) {
		super(
			kvStateRegistry,
			keySerializer,
			userCodeClassLoader,
			executionConfig,
			ttlTimeProvider,
			cancelStreamRegistry,
			keyContext);
		LOG.info("Initializing remote heap keyed state backend with stream factory.");
		this.sharedREMKeyBuilder = sharedREMKeyBuilder;
		this.keyGroupPrefixBytes = keyGroupPrefixBytes;
		this.writeBatchSize = writeBatchSize;
		this.syncRemClient = syncRemClient;
		this.asyncRemClient = asyncRemClient;
		this.kvStateInformation = kvStateInformation;
		this.registeredPQStates = registeredPQStates;
		this.priorityQueueSetFactory = priorityQueueSetFactory;
		this.snapshotStrategy = snapshotStrategy;
		this.writeBatchWrapper = writeBatchWrapper;

	}

	/**
	 * Returns the total number of state entries across all keys/namespaces.
	 */
	@Override
	public int numKeyValueStateEntries() {
		Long ret = this.syncRemClient.dbSize();
		if (ret > Integer.MAX_VALUE) {
			String message = String.format("DB size %d exceeds integer max value\n", ret);
			throw new FlinkRuntimeException(message);
		}
		return ret.intValue();
	}

	/**
	 * This method is called as a notification once a distributed checkpoint has been completed.
	 *
	 * <p>Note that any exception during this method will not cause the checkpoint to
	 * fail any more.
	 *
	 * @param checkpointId The ID of the checkpoint that has been completed.
	 *
	 * @throws Exception This method can propagate exceptions, which leads to a failure/recovery for
	 * 	the task. Not that this will NOT lead to the checkpoint being revoked.
	 */
	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {

	}

	/**
	 * @param state State variable for which existing keys will be returned.
	 * @param namespace Namespace for which existing keys will be returned.
	 *
	 * @return A stream of all keys for the given state and namespace. Modifications to the state during iterating
	 * 	over it keys are not supported.
	 */
	@Override
	public <N> Stream<K> getKeys(String state, N namespace) throws IOException {
		RemoteHeapKvStateInfo info = kvStateInformation.get(state);
		if (!(info.metaInfo instanceof RegisteredKeyValueStateBackendMetaInfo)) {
			return Stream.empty();
		}
		RegisteredKeyValueStateBackendMetaInfo<N, ?> registeredKeyValueStateBackendMetaInfo =
			(RegisteredKeyValueStateBackendMetaInfo<N, ?>) info.metaInfo;
		final TypeSerializer<N> namespaceSerializer = registeredKeyValueStateBackendMetaInfo.getNamespaceSerializer();

		String wildcard = "*";
		Collection<String> keys = this.syncRemClient.keys(wildcard);
		if (keys == null || keys.isEmpty()) {
			return Stream.empty();
		}
		final DataOutputSerializer namespaceOutputView = new DataOutputSerializer(8);
		boolean ambiguousKeyPossible = RemoteHeapKeySerializationUtils.isAmbiguousKeyPossible(
			getKeySerializer(),
			namespaceSerializer);
		RemoteHeapKeySerializationUtils.writeNameSpace(
			namespace,
			namespaceSerializer,
			namespaceOutputView,
			ambiguousKeyPossible);
		byte[] nameSpaceBytes = namespaceOutputView.getCopyOfBuffer();
		DataInputDeserializer byteArrayDataInputView = new DataInputDeserializer();
		Stream<K> ret = keys.stream().map(key ->
		{
			try {
				LOG.trace(
					"RemoteKeyedStateBackend: getKeys deserialize key {} {} {} {}",
					key,
					key.getBytes(),
					getKeySerializer(),
					namespaceSerializer);
				final byte[] keyBytes = key.getBytes();
				final K currentKey = deserializeKey(
					keyBytes,
					byteArrayDataInputView,
					byteArrayDataInputView,
					ambiguousKeyPossible);
				final int namespaceByteStartPos = byteArrayDataInputView.getPosition();
				if (isMatchingNameSpace(keyBytes, namespaceByteStartPos, nameSpaceBytes)) {
					return new Tuple2<>(currentKey, true);
				}
				return new Tuple2<>(currentKey, false);
			} catch (IOException e) {
				e.printStackTrace();
			}
			return null;
		}).filter(x -> (x != null && x.f1)).map(x -> x.f0);
		return ret;
	}

	private K deserializeKey(
		byte[] keyBytes, DataInputDeserializer readView,
		DataInputDeserializer byteArrayDataInputView,
		boolean ambiguousKeyPossible) throws IOException {
		readView.setBuffer(keyBytes, keyGroupPrefixBytes, keyBytes.length - keyGroupPrefixBytes);
		return RemoteHeapKeySerializationUtils.readKey(
			keySerializer,
			byteArrayDataInputView,
			ambiguousKeyPossible);
	}

	private boolean isMatchingNameSpace(@Nonnull byte[] key, int beginPos, byte[] namespaceBytes) {
		final int namespaceBytesLength = namespaceBytes.length;
		final int basicLength = namespaceBytesLength + beginPos;
		if (key.length >= basicLength) {
			for (int i = 0; i < namespaceBytesLength; ++i) {
				if (key[beginPos + i] != namespaceBytes[i]) {
					return false;
				}
			}
			return true;
		}
		return false;
	}

	/**
	 * Creates and returns a new Internal State.
	 *
	 * @param namespaceSerializer TypeSerializer for the state namespace.
	 * @param stateDesc The {@code StateDescriptor} that contains the name of the state.
	 * @param snapshotTransformFactory factory of state snapshot transformer.
	 */
	@Nonnull
	@Override
	public <N, SV, SEV, S extends State, IS extends S> IS createInternalState(
		@Nonnull TypeSerializer<N> namespaceSerializer,
		@Nonnull StateDescriptor<S, SV> stateDesc,
		@Nonnull StateSnapshotTransformer.StateSnapshotTransformFactory<SEV> snapshotTransformFactory) throws Exception {
		StateFactory stateFactory = STATE_FACTORIES.get(stateDesc.getClass());
		if (stateFactory == null) {
			String message = String.format("State %s is not supported by %s",
				stateDesc.getClass(), this.getClass());
			throw new FlinkRuntimeException(message);
		}
		RegisteredKeyValueStateBackendMetaInfo<N, SV> registerResult = tryRegisterKvStateInformation(
			stateDesc, namespaceSerializer, snapshotTransformFactory);
		LOG.trace("RemoteKeyedStateBackend: createInternalState {} default {} queryable state name {}",
			stateDesc.getName(),
			stateDesc.getDefaultValue(),
			stateDesc.getQueryableStateName());
		return stateFactory.createState(
			stateDesc,
			registerResult,
			keySerializer,
			RemoteHeapKeyedStateBackend.this);
	}

	/**
	 * Creates a {@link KeyGroupedInternalPriorityQueue}.
	 *
	 * @param stateName unique name for associated with this queue.
	 * @param byteOrderedElementSerializer a serializer that with a format that is lexicographically ordered in
	 * 	alignment with elementPriorityComparator.
	 *
	 * @return the queue with the specified unique name.
	 */
	@Nonnull
	@Override
	public <T extends HeapPriorityQueueElement & PriorityComparable & Keyed> KeyGroupedInternalPriorityQueue<T> create(
		@Nonnull String stateName,
		@Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
		final HeapPriorityQueueSnapshotRestoreWrapper existingState = registeredPQStates.get(
			stateName);

		if (existingState != null) {
			// TODO we implement the simple way of supporting the current functionality, mimicking keyed state
			// because this should be reworked in FLINK-9376 and then we should have a common algorithm over
			// StateMetaInfoSnapshot that avoids this code duplication.

			TypeSerializerSchemaCompatibility<T> compatibilityResult =
				existingState.getMetaInfo().updateElementSerializer(byteOrderedElementSerializer);

			if (compatibilityResult.isIncompatible()) {
				throw new FlinkRuntimeException(new StateMigrationException(
					"For heap backends, the new priority queue serializer must not be incompatible."));
			} else {
				registeredPQStates.put(
					stateName,
					existingState.forUpdatedSerializer(byteOrderedElementSerializer));
			}

			return existingState.getPriorityQueue();
		} else {
			final RegisteredPriorityQueueStateBackendMetaInfo<T> metaInfo =
				new RegisteredPriorityQueueStateBackendMetaInfo<>(
					stateName,
					byteOrderedElementSerializer);
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

	/**
	 * Operation that writes a snapshot into a stream that is provided by the given {@link CheckpointStreamFactory} and
	 * returns a @{@link RunnableFuture} that gives a state handle to the snapshot. It is up to the implementation if
	 * the operation is performed synchronous or asynchronous. In the later case, the returned Runnable must be executed
	 * first before obtaining the handle.
	 *
	 * @param checkpointId The ID of the checkpoint.
	 * @param timestamp The timestamp of the checkpoint.
	 * @param streamFactory The factory that we can use for writing our state to streams.
	 * @param checkpointOptions Options for how to perform this checkpoint.
	 *
	 * @return A runnable future that will yield a {@link StateObject}.
	 */
	@Nonnull
	@Override
	public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
		long checkpointId,
		long timestamp,
		@Nonnull CheckpointStreamFactory streamFactory,
		@Nonnull CheckpointOptions checkpointOptions) throws Exception {
		long startTime = System.currentTimeMillis();
		writeBatchWrapper.flush();
		final RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotRunner =
			snapshotStrategy.snapshot(checkpointId, timestamp, streamFactory, checkpointOptions);

		snapshotStrategy.logSyncCompleted(streamFactory, startTime);
		return snapshotRunner;
	}

	@Override
	public void dispose() {

		super.dispose();
		kvStateInformation.clear();
		if (syncRemClient != null) {
			IOUtils.closeQuietly(writeBatchWrapper);
		}
	}


	private interface StateFactory {
		<K, N, SV, S extends State, IS extends S> IS createState(
			StateDescriptor<S, SV> stateDesc,
			RegisteredKeyValueStateBackendMetaInfo<N, SV> result,
			TypeSerializer<K> keySerializer,
			RemoteHeapKeyedStateBackend backend) throws Exception;
	}

	RemoteHeapSerializedCompositeKeyBuilder<K> getSharedREMKeyBuilder() {
		return sharedREMKeyBuilder;
	}

	public int getKeyGroupPrefixBytes() {
		return keyGroupPrefixBytes;
	}

	@Nonnegative
	long getWriteBatchSize() {
		return writeBatchSize;
	}

	/** Rocks DB specific information about the k/v states. */
	public static class RemoteHeapKvStateInfo implements AutoCloseable {
		public final RegisteredStateMetaInfoBase metaInfo;
		public final byte[] nameBytes;

		public RemoteHeapKvStateInfo(
			RegisteredStateMetaInfoBase metaInfo,
			byte[] nameBytes) {
			this.metaInfo = metaInfo;
			this.nameBytes = nameBytes;
		}

		@Override
		public void close() throws Exception {

		}
	}

	/**
	 * Registers a k/v state information, which includes its state id, type, RocksDB column family handle, and serializers.
	 *
	 * <p>When restoring from a snapshot, we don’t restore the individual k/v states, just the global RocksDB database and
	 * the list of k/v state information. When a k/v state is first requested we check here whether we
	 * already have a registered entry for that and return it (after some necessary state compatibility checks)
	 * or create a new one if it does not exist.
	 */
	private <N, S extends State, SV, SEV> RegisteredKeyValueStateBackendMetaInfo<N, SV> tryRegisterKvStateInformation(
		StateDescriptor<S, SV> stateDesc,
		TypeSerializer<N> namespaceSerializer,
		@Nonnull StateSnapshotTransformer.StateSnapshotTransformFactory<SEV> snapshotTransformFactory) throws Exception {

		RemoteHeapKvStateInfo oldStateInfo = kvStateInformation.get(stateDesc.getName());

		TypeSerializer<SV> stateSerializer = stateDesc.getSerializer();

		RemoteHeapKvStateInfo newRedisStateInfo;
		RegisteredKeyValueStateBackendMetaInfo<N, SV> newMetaInfo;
		if (oldStateInfo != null) {
			@SuppressWarnings("unchecked")
			String message = String.format("oldStateInfo is not null for RemoteHeapState state Desc %s by %s",
				stateDesc.getClass(),
				this.getClass());
			throw new FlinkRuntimeException(message);
		} else {
			newMetaInfo = new RegisteredKeyValueStateBackendMetaInfo<>(
				stateDesc.getType(),
				stateDesc.getName(),
				namespaceSerializer,
				stateSerializer,
				StateSnapshotTransformer.StateSnapshotTransformFactory.noTransform());

			newRedisStateInfo = RedisOperationUtils.createStateInfo(newMetaInfo);
			RedisOperationUtils.registerKvStateInformation(
				this.kvStateInformation,
				stateDesc.getName(),
				newRedisStateInfo);
		}
		return newMetaInfo;
	}

	public RemoteHeapKvStateInfo getRemoteHeapKvStateInfo(String stateDescName) {
		return this.kvStateInformation.get(stateDescName);
	}

	@Override
	public void setCurrentKey(K newKey) {
		super.setCurrentKey(newKey);
		sharedREMKeyBuilder.setKeyAndKeyGroup(getCurrentKey(), getCurrentKeyGroupIndex());
	}
}
