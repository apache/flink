/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

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
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializerSnapshot;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.iterator.RocksStateKeysIterator;
import org.apache.flink.contrib.streaming.state.snapshot.RocksDBSnapshotStrategyBase;
import org.apache.flink.contrib.streaming.state.ttl.RocksDbTtlCompactFiltersManager;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.PriorityQueueSetFactory;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ResourceGuard;
import org.apache.flink.util.StateMigrationException;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Snapshot;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.RunnableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.flink.contrib.streaming.state.RocksDBSnapshotTransformFactoryAdaptor.wrapStateSnapshotTransformFactory;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An {@link AbstractKeyedStateBackend} that stores its state in {@code RocksDB} and serializes state to
 * streams provided by a {@link org.apache.flink.runtime.state.CheckpointStreamFactory} upon
 * checkpointing. This state backend can store very large state that exceeds memory and spills
 * to disk. Except for the snapshotting, this class should be accessed as if it is not threadsafe.
 *
 * <p>This class follows the rules for closing/releasing native RocksDB resources as described in
 + <a href="https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#opening-a-database-with-column-families">
 * this document</a>.
 */
public class RocksDBKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {

	private static final Logger LOG = LoggerFactory.getLogger(RocksDBKeyedStateBackend.class);

	/** The name of the merge operator in RocksDB. Do not change except you know exactly what you do. */
	public static final String MERGE_OPERATOR_NAME = "stringappendtest";

	@SuppressWarnings("deprecation")
	private static final Map<Class<? extends StateDescriptor>, StateFactory> STATE_FACTORIES =
		Stream.of(
			Tuple2.of(ValueStateDescriptor.class, (StateFactory) RocksDBValueState::create),
			Tuple2.of(ListStateDescriptor.class, (StateFactory) RocksDBListState::create),
			Tuple2.of(MapStateDescriptor.class, (StateFactory) RocksDBMapState::create),
			Tuple2.of(AggregatingStateDescriptor.class, (StateFactory) RocksDBAggregatingState::create),
			Tuple2.of(ReducingStateDescriptor.class, (StateFactory) RocksDBReducingState::create),
			Tuple2.of(FoldingStateDescriptor.class, (StateFactory) RocksDBFoldingState::create)
		).collect(Collectors.toMap(t -> t.f0, t -> t.f1));

	private interface StateFactory {
		<K, N, SV, S extends State, IS extends S> IS createState(
			StateDescriptor<S, SV> stateDesc,
			Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>> registerResult,
			RocksDBKeyedStateBackend<K> backend) throws Exception;
	}

	/** Factory function to create column family options from state name. */
	private final Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory;

	/** The container of RocksDB option factory and predefined options. */
	private final RocksDBResourceContainer optionsContainer;

	/** Path where this configured instance stores its data directory. */
	private final File instanceBasePath;

	/**
	 * Protects access to RocksDB in other threads, like the checkpointing thread from parallel call that disposes the
	 * RocksDb object.
	 */
	private final ResourceGuard rocksDBResourceGuard;

	/**
	 * The write options to use in the states. We disable write ahead logging.
	 */
	private final WriteOptions writeOptions;

	/**
	 * The max memory size for one batch in {@link RocksDBWriteBatchWrapper}.
	 */
	private final long writeBatchSize;

	/**
	 * Information about the k/v states, maintained in the order as we create them. This is used to retrieve the
	 * column family that is used for a state and also for sanity checks when restoring.
	 */
	private final LinkedHashMap<String, RocksDbKvStateInfo> kvStateInformation;

	/** Number of bytes required to prefix the key groups. */
	private final int keyGroupPrefixBytes;

	/**
	 * We are not using the default column family for Flink state ops, but we still need to remember this handle so that
	 * we can close it properly when the backend is closed. Note that the one returned by {@link RocksDB#open(String)}
	 * is different from that by {@link RocksDB#getDefaultColumnFamily()}, probably it's a bug of RocksDB java API.
	 */
	private final ColumnFamilyHandle defaultColumnFamily;

	/** Shared wrapper for batch writes to the RocksDB instance. */
	private final RocksDBWriteBatchWrapper writeBatchWrapper;

	/** The checkpoint snapshot strategy, e.g., if we use full or incremental checkpoints, local state, and so on. */
	private final RocksDBSnapshotStrategyBase<K> checkpointSnapshotStrategy;

	/** The savepoint snapshot strategy. */
	private final RocksDBSnapshotStrategyBase<K> savepointSnapshotStrategy;

	/** The native metrics monitor. */
	private final RocksDBNativeMetricMonitor nativeMetricMonitor;

	/** Factory for priority queue state. */
	private final PriorityQueueSetFactory priorityQueueFactory;

	/**
	 * Helper to build the byte arrays of composite keys to address data in RocksDB. Shared across all states.
	 */
	private final RocksDBSerializedCompositeKeyBuilder<K> sharedRocksKeyBuilder;

	/**
	 * Our RocksDB database, this is used by the actual subclasses of {@link AbstractRocksDBState}
	 * to store state. The different k/v states that we have don't each have their own RocksDB
	 * instance. They all write to this instance but to their own column family.
	 */
	protected final RocksDB db;

	// mark whether this backend is already disposed and prevent duplicate disposing
	private boolean disposed = false;

	private final RocksDbTtlCompactFiltersManager ttlCompactFiltersManager;

	public RocksDBKeyedStateBackend(
		ClassLoader userCodeClassLoader,
		File instanceBasePath,
		RocksDBResourceContainer optionsContainer,
		Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
		TaskKvStateRegistry kvStateRegistry,
		TypeSerializer<K> keySerializer,
		ExecutionConfig executionConfig,
		TtlTimeProvider ttlTimeProvider,
		RocksDB db,
		LinkedHashMap<String, RocksDbKvStateInfo> kvStateInformation,
		int keyGroupPrefixBytes,
		CloseableRegistry cancelStreamRegistry,
		StreamCompressionDecorator keyGroupCompressionDecorator,
		ResourceGuard rocksDBResourceGuard,
		RocksDBSnapshotStrategyBase<K> checkpointSnapshotStrategy,
		RocksDBSnapshotStrategyBase<K> savepointSnapshotStrategy,
		RocksDBWriteBatchWrapper writeBatchWrapper,
		ColumnFamilyHandle defaultColumnFamilyHandle,
		RocksDBNativeMetricMonitor nativeMetricMonitor,
		RocksDBSerializedCompositeKeyBuilder<K> sharedRocksKeyBuilder,
		PriorityQueueSetFactory priorityQueueFactory,
		RocksDbTtlCompactFiltersManager ttlCompactFiltersManager,
		InternalKeyContext<K> keyContext,
		@Nonnegative long writeBatchSize) {

		super(
			kvStateRegistry,
			keySerializer,
			userCodeClassLoader,
			executionConfig,
			ttlTimeProvider,
			cancelStreamRegistry,
			keyGroupCompressionDecorator,
			keyContext);

		this.ttlCompactFiltersManager = ttlCompactFiltersManager;

		// ensure that we use the right merge operator, because other code relies on this
		this.columnFamilyOptionsFactory = Preconditions.checkNotNull(columnFamilyOptionsFactory);

		this.optionsContainer = Preconditions.checkNotNull(optionsContainer);

		this.instanceBasePath = Preconditions.checkNotNull(instanceBasePath);

		this.keyGroupPrefixBytes = keyGroupPrefixBytes;
		this.kvStateInformation = kvStateInformation;

		this.writeOptions = new WriteOptions().setDisableWAL(true);
		checkArgument(writeBatchSize >= 0, "Write batch size have to be no negative value.");
		this.writeBatchSize = writeBatchSize;
		this.db = db;
		this.rocksDBResourceGuard = rocksDBResourceGuard;
		this.checkpointSnapshotStrategy = checkpointSnapshotStrategy;
		this.savepointSnapshotStrategy = savepointSnapshotStrategy;
		this.writeBatchWrapper = writeBatchWrapper;
		this.defaultColumnFamily = defaultColumnFamilyHandle;
		this.nativeMetricMonitor = nativeMetricMonitor;
		this.sharedRocksKeyBuilder = sharedRocksKeyBuilder;
		this.priorityQueueFactory = priorityQueueFactory;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <N> Stream<K> getKeys(String state, N namespace) {
		RocksDbKvStateInfo columnInfo = kvStateInformation.get(state);
		if (columnInfo == null || !(columnInfo.metaInfo instanceof RegisteredKeyValueStateBackendMetaInfo)) {
			return Stream.empty();
		}

		RegisteredKeyValueStateBackendMetaInfo<N, ?> registeredKeyValueStateBackendMetaInfo =
			(RegisteredKeyValueStateBackendMetaInfo<N, ?>) columnInfo.metaInfo;

		final TypeSerializer<N> namespaceSerializer = registeredKeyValueStateBackendMetaInfo.getNamespaceSerializer();
		final DataOutputSerializer namespaceOutputView = new DataOutputSerializer(8);
		boolean ambiguousKeyPossible = RocksDBKeySerializationUtils.isAmbiguousKeyPossible(getKeySerializer(), namespaceSerializer);
		final byte[] nameSpaceBytes;
		try {
			RocksDBKeySerializationUtils.writeNameSpace(
				namespace,
				namespaceSerializer,
				namespaceOutputView,
				ambiguousKeyPossible);
			nameSpaceBytes = namespaceOutputView.getCopyOfBuffer();
		} catch (IOException ex) {
			throw new FlinkRuntimeException("Failed to get keys from RocksDB state backend.", ex);
		}

		RocksIteratorWrapper iterator = RocksDBOperationUtils.getRocksIterator(db, columnInfo.columnFamilyHandle);
		iterator.seekToFirst();

		final RocksStateKeysIterator<K> iteratorWrapper = new RocksStateKeysIterator<>(iterator, state, getKeySerializer(), keyGroupPrefixBytes,
			ambiguousKeyPossible, nameSpaceBytes);

		Stream<K> targetStream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(iteratorWrapper, Spliterator.ORDERED), false);
		return targetStream.onClose(iteratorWrapper::close);
	}

	@VisibleForTesting
	ColumnFamilyHandle getColumnFamilyHandle(String state) {
				RocksDbKvStateInfo columnInfo = kvStateInformation.get(state);
				return columnInfo != null ? columnInfo.columnFamilyHandle : null;
	}

	@Override
	public void setCurrentKey(K newKey) {
		super.setCurrentKey(newKey);
		sharedRocksKeyBuilder.setKeyAndKeyGroup(getCurrentKey(), getCurrentKeyGroupIndex());
	}

	/**
	 * Should only be called by one thread, and only after all accesses to the DB happened.
	 */
	@Override
	public void dispose() {
		if (this.disposed) {
			return;
		}
		super.dispose();

		// This call will block until all clients that still acquire access to the RocksDB instance have released it,
		// so that we cannot release the native resources while clients are still working with it in parallel.
		rocksDBResourceGuard.close();

		// IMPORTANT: null reference to signal potential async checkpoint workers that the db was disposed, as
		// working on the disposed object results in SEGFAULTS.
		if (db != null) {

			IOUtils.closeQuietly(writeBatchWrapper);

			// Metric collection occurs on a background thread. When this method returns
			// it is guaranteed that thr RocksDB reference has been invalidated
			// and no more metric collection will be attempted against the database.
			if (nativeMetricMonitor != null) {
				nativeMetricMonitor.close();
			}

			List<ColumnFamilyOptions> columnFamilyOptions = new ArrayList<>(kvStateInformation.values().size());

			// RocksDB's native memory management requires that *all* CFs (including default) are closed before the
			// DB is closed. See:
			// https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#opening-a-database-with-column-families
			// Start with default CF ...
			RocksDBOperationUtils.addColumnFamilyOptionsToCloseLater(columnFamilyOptions, defaultColumnFamily);
			IOUtils.closeQuietly(defaultColumnFamily);

			// ... continue with the ones created by Flink...
			for (RocksDbKvStateInfo kvStateInfo : kvStateInformation.values()) {
				RocksDBOperationUtils.addColumnFamilyOptionsToCloseLater(columnFamilyOptions, kvStateInfo.columnFamilyHandle);
				IOUtils.closeQuietly(kvStateInfo.columnFamilyHandle);
			}

			// ... and finally close the DB instance ...
			IOUtils.closeQuietly(db);

			columnFamilyOptions.forEach(IOUtils::closeQuietly);

			IOUtils.closeQuietly(optionsContainer);
			IOUtils.closeQuietly(writeOptions);

			ttlCompactFiltersManager.disposeAndClearRegisteredCompactionFactories();

			kvStateInformation.clear();

			cleanInstanceBasePath();
		}
		this.disposed = true;
	}

	@Nonnull
	@Override
	public <T extends HeapPriorityQueueElement & PriorityComparable & Keyed> KeyGroupedInternalPriorityQueue<T>
	create(
		@Nonnull String stateName,
		@Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
		return priorityQueueFactory.create(stateName, byteOrderedElementSerializer);
	}

	private void cleanInstanceBasePath() {
		LOG.info("Closed RocksDB State Backend. Cleaning up RocksDB working directory {}.", instanceBasePath);

		try {
			FileUtils.deleteDirectory(instanceBasePath);
		} catch (IOException ex) {
			LOG.warn("Could not delete RocksDB working directory: {}", instanceBasePath, ex);
		}
	}

	// ------------------------------------------------------------------------
	//  Getters and Setters
	// ------------------------------------------------------------------------

	public int getKeyGroupPrefixBytes() {
		return keyGroupPrefixBytes;
	}

	@VisibleForTesting
	PriorityQueueSetFactory getPriorityQueueFactory() {
		return priorityQueueFactory;
	}

	public WriteOptions getWriteOptions() {
		return writeOptions;
	}

	RocksDBSerializedCompositeKeyBuilder<K> getSharedRocksKeyBuilder() {
		return sharedRocksKeyBuilder;
	}

	@VisibleForTesting
	boolean isDisposed() {
		return this.disposed;
	}

	/**
	 * Triggers an asynchronous snapshot of the keyed state backend from RocksDB. This snapshot can be canceled and
	 * is also stopped when the backend is closed through {@link #dispose()}. For each backend, this method must always
	 * be called by the same thread.
	 *
	 * @param checkpointId  The Id of the checkpoint.
	 * @param timestamp     The timestamp of the checkpoint.
	 * @param streamFactory The factory that we can use for writing our state to streams.
	 * @param checkpointOptions Options for how to perform this checkpoint.
	 * @return Future to the state handle of the snapshot data.
	 * @throws Exception indicating a problem in the synchronous part of the checkpoint.
	 */
	@Nonnull
	@Override
	public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
		final long checkpointId,
		final long timestamp,
		@Nonnull final CheckpointStreamFactory streamFactory,
		@Nonnull CheckpointOptions checkpointOptions) throws Exception {

		long startTime = System.currentTimeMillis();

		// flush everything into db before taking a snapshot
		writeBatchWrapper.flush();

		RocksDBSnapshotStrategyBase<K> chosenSnapshotStrategy =
				checkpointOptions.getCheckpointType().isSavepoint() ? savepointSnapshotStrategy : checkpointSnapshotStrategy;

		RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotRunner =
			chosenSnapshotStrategy.snapshot(checkpointId, timestamp, streamFactory, checkpointOptions);

		chosenSnapshotStrategy.logSyncCompleted(streamFactory, startTime);

		return snapshotRunner;
	}

	@Override
	public void notifyCheckpointComplete(long completedCheckpointId) throws Exception {

		if (checkpointSnapshotStrategy != null) {
			checkpointSnapshotStrategy.notifyCheckpointComplete(completedCheckpointId);
		}

		if (savepointSnapshotStrategy != null) {
			savepointSnapshotStrategy.notifyCheckpointComplete(completedCheckpointId);
		}
	}

	@Override
	public void notifyCheckpointAborted(long checkpointId) throws Exception {
		checkpointSnapshotStrategy.notifyCheckpointAborted(checkpointId);

		savepointSnapshotStrategy.notifyCheckpointAborted(checkpointId);
	}

	/**
	 * Registers a k/v state information, which includes its state id, type, RocksDB column family handle, and serializers.
	 *
	 * <p>When restoring from a snapshot, we don’t restore the individual k/v states, just the global RocksDB database and
	 * the list of k/v state information. When a k/v state is first requested we check here whether we
	 * already have a registered entry for that and return it (after some necessary state compatibility checks)
	 * or create a new one if it does not exist.
	 */
	private <N, S extends State, SV, SEV> Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>> tryRegisterKvStateInformation(
		StateDescriptor<S, SV> stateDesc,
		TypeSerializer<N> namespaceSerializer,
		@Nonnull StateSnapshotTransformFactory<SEV> snapshotTransformFactory) throws Exception {

		RocksDbKvStateInfo oldStateInfo = kvStateInformation.get(stateDesc.getName());

		TypeSerializer<SV> stateSerializer = stateDesc.getSerializer();

		RocksDbKvStateInfo newRocksStateInfo;
		RegisteredKeyValueStateBackendMetaInfo<N, SV> newMetaInfo;
		if (oldStateInfo != null) {
			@SuppressWarnings("unchecked")
			RegisteredKeyValueStateBackendMetaInfo<N, SV> castedMetaInfo =
				(RegisteredKeyValueStateBackendMetaInfo<N, SV>) oldStateInfo.metaInfo;

			newMetaInfo = updateRestoredStateMetaInfo(
				Tuple2.of(oldStateInfo.columnFamilyHandle, castedMetaInfo),
				stateDesc,
				namespaceSerializer,
				stateSerializer);

			newRocksStateInfo = new RocksDbKvStateInfo(oldStateInfo.columnFamilyHandle, newMetaInfo);
			kvStateInformation.put(stateDesc.getName(), newRocksStateInfo);
		} else {
			newMetaInfo = new RegisteredKeyValueStateBackendMetaInfo<>(
				stateDesc.getType(),
				stateDesc.getName(),
				namespaceSerializer,
				stateSerializer,
				StateSnapshotTransformFactory.noTransform());

			newRocksStateInfo = RocksDBOperationUtils.createStateInfo(
				newMetaInfo, db, columnFamilyOptionsFactory, ttlCompactFiltersManager);
			RocksDBOperationUtils.registerKvStateInformation(this.kvStateInformation, this.nativeMetricMonitor,
				stateDesc.getName(), newRocksStateInfo);
		}

		StateSnapshotTransformFactory<SV> wrappedSnapshotTransformFactory = wrapStateSnapshotTransformFactory(
			stateDesc, snapshotTransformFactory, newMetaInfo.getStateSerializer());
		newMetaInfo.updateSnapshotTransformFactory(wrappedSnapshotTransformFactory);

		ttlCompactFiltersManager.configCompactFilter(stateDesc, newMetaInfo.getStateSerializer());

		return Tuple2.of(newRocksStateInfo.columnFamilyHandle, newMetaInfo);
	}

	private <N, S extends State, SV> RegisteredKeyValueStateBackendMetaInfo<N, SV> updateRestoredStateMetaInfo(
		Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>> oldStateInfo,
		StateDescriptor<S, SV> stateDesc,
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<SV> stateSerializer) throws Exception {

		@SuppressWarnings("unchecked")
		RegisteredKeyValueStateBackendMetaInfo<N, SV> restoredKvStateMetaInfo = oldStateInfo.f1;

		TypeSerializerSchemaCompatibility<N> s = restoredKvStateMetaInfo.updateNamespaceSerializer(namespaceSerializer);
		if (s.isCompatibleAfterMigration() || s.isIncompatible()) {
			throw new StateMigrationException("The new namespace serializer must be compatible.");
		}

		restoredKvStateMetaInfo.checkStateMetaInfo(stateDesc);

		TypeSerializerSchemaCompatibility<SV> newStateSerializerCompatibility =
			restoredKvStateMetaInfo.updateStateSerializer(stateSerializer);
		if (newStateSerializerCompatibility.isCompatibleAfterMigration()) {
			migrateStateValues(stateDesc, oldStateInfo);
		} else if (newStateSerializerCompatibility.isIncompatible()) {
			throw new StateMigrationException("The new state serializer cannot be incompatible.");
		}

		return restoredKvStateMetaInfo;
	}

	/**
	 * Migrate only the state value, that is the "value" that is stored in RocksDB. We don't migrate
	 * the key here, which is made up of key group, key, namespace and map key
	 * (in case of MapState).
	 */
	@SuppressWarnings("unchecked")
	private <N, S extends State, SV> void migrateStateValues(
		StateDescriptor<S, SV> stateDesc,
		Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>> stateMetaInfo) throws Exception {

		if (stateDesc.getType() == StateDescriptor.Type.MAP) {
			TypeSerializerSnapshot<SV> previousSerializerSnapshot = stateMetaInfo.f1.getPreviousStateSerializerSnapshot();
			checkState(previousSerializerSnapshot != null, "the previous serializer snapshot should exist.");
			checkState(previousSerializerSnapshot instanceof MapSerializerSnapshot, "previous serializer snapshot should be a MapSerializerSnapshot.");

			TypeSerializer<SV> newSerializer = stateMetaInfo.f1.getStateSerializer();
			checkState(newSerializer instanceof MapSerializer, "new serializer should be a MapSerializer.");

			MapSerializer<?, ?> mapSerializer = (MapSerializer<?, ?>) newSerializer;
			MapSerializerSnapshot<?, ?> mapSerializerSnapshot = (MapSerializerSnapshot<?, ?>) previousSerializerSnapshot;
			if (!checkMapStateKeySchemaCompatibility(mapSerializerSnapshot, mapSerializer)) {
				throw new StateMigrationException(
					"The new serializer for a MapState requires state migration in order for the job to proceed, since the key schema has changed. However, migration for MapState currently only allows value schema evolutions.");
			}
		}

		LOG.info(
			"Performing state migration for state {} because the state serializer's schema, i.e. serialization format, has changed.",
			stateDesc);

		// we need to get an actual state instance because migration is different
		// for different state types. For example, ListState needs to deal with
		// individual elements
		StateFactory stateFactory = STATE_FACTORIES.get(stateDesc.getClass());
		if (stateFactory == null) {
			String message = String.format("State %s is not supported by %s",
				stateDesc.getClass(), this.getClass());
			throw new FlinkRuntimeException(message);
		}
		State state = stateFactory.createState(
			stateDesc,
			stateMetaInfo,
			RocksDBKeyedStateBackend.this);
		if (!(state instanceof AbstractRocksDBState)) {
			throw new FlinkRuntimeException(
				"State should be an AbstractRocksDBState but is " + state);
		}

		@SuppressWarnings("unchecked")
		AbstractRocksDBState<?, ?, SV> rocksDBState = (AbstractRocksDBState<?, ?, SV>) state;

		Snapshot rocksDBSnapshot = db.getSnapshot();
		try (
			RocksIteratorWrapper iterator = RocksDBOperationUtils.getRocksIterator(db, stateMetaInfo.f0);
			RocksDBWriteBatchWrapper batchWriter = new RocksDBWriteBatchWrapper(db, getWriteOptions(), getWriteBatchSize())
		) {
			iterator.seekToFirst();

			DataInputDeserializer serializedValueInput = new DataInputDeserializer();
			DataOutputSerializer migratedSerializedValueOutput = new DataOutputSerializer(512);
			while (iterator.isValid()) {
				serializedValueInput.setBuffer(iterator.value());

				rocksDBState.migrateSerializedValue(
					serializedValueInput,
					migratedSerializedValueOutput,
					stateMetaInfo.f1.getPreviousStateSerializer(),
					stateMetaInfo.f1.getStateSerializer());

				batchWriter.put(stateMetaInfo.f0, iterator.key(), migratedSerializedValueOutput.getCopyOfBuffer());

				migratedSerializedValueOutput.clear();
				iterator.next();
			}
		} finally {
			db.releaseSnapshot(rocksDBSnapshot);
			rocksDBSnapshot.close();
		}
	}

	@SuppressWarnings("unchecked")
	private static <UK> boolean checkMapStateKeySchemaCompatibility(
		MapSerializerSnapshot<?, ?> mapStateSerializerSnapshot,
		MapSerializer<?, ?> newMapStateSerializer) {
		TypeSerializerSnapshot<UK> previousKeySerializerSnapshot = (TypeSerializerSnapshot<UK>) mapStateSerializerSnapshot.getKeySerializerSnapshot();
		TypeSerializer<UK> newUserKeySerializer = (TypeSerializer<UK>) newMapStateSerializer.getKeySerializer();

		TypeSerializerSchemaCompatibility<UK> keyCompatibility = previousKeySerializerSnapshot.resolveSchemaCompatibility(newUserKeySerializer);
		return keyCompatibility.isCompatibleAsIs();
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
		Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>> registerResult = tryRegisterKvStateInformation(
			stateDesc, namespaceSerializer, snapshotTransformFactory);
		return stateFactory.createState(stateDesc, registerResult, RocksDBKeyedStateBackend.this);
	}

	/**
	 * Only visible for testing, DO NOT USE.
	 */
	File getInstanceBasePath() {
		return instanceBasePath;
	}

	@Override
	public boolean supportsAsynchronousSnapshots() {
		return true;
	}

	@VisibleForTesting
	@SuppressWarnings("unchecked")
	@Override
	public int numKeyValueStateEntries() {
		int count = 0;

		for (RocksDbKvStateInfo metaInfo : kvStateInformation.values()) {
			//TODO maybe filterOrTransform only for k/v states
			try (RocksIteratorWrapper rocksIterator = RocksDBOperationUtils.getRocksIterator(db, metaInfo.columnFamilyHandle)) {
				rocksIterator.seekToFirst();

				while (rocksIterator.isValid()) {
					count++;
					rocksIterator.next();
				}
			}
		}

		return count;
	}

	@Override
	public boolean requiresLegacySynchronousTimerSnapshots() {
		return priorityQueueFactory instanceof HeapPriorityQueueSetFactory;
	}

	/** Rocks DB specific information about the k/v states. */
	public static class RocksDbKvStateInfo implements AutoCloseable{
		public final ColumnFamilyHandle columnFamilyHandle;
		public final RegisteredStateMetaInfoBase metaInfo;

		public RocksDbKvStateInfo(
			ColumnFamilyHandle columnFamilyHandle,
			RegisteredStateMetaInfoBase metaInfo) {
			this.columnFamilyHandle = columnFamilyHandle;
			this.metaInfo = metaInfo;
		}

		@Override
		public void close() throws Exception {
			this.columnFamilyHandle.close();
		}
	}

	@VisibleForTesting
	public void compactState(StateDescriptor<?, ?> stateDesc) throws RocksDBException {
		RocksDbKvStateInfo kvStateInfo = kvStateInformation.get(stateDesc.getName());
		db.compactRange(kvStateInfo.columnFamilyHandle);
	}

	@Nonnegative
	long getWriteBatchSize() {
		return writeBatchSize;
	}
}
