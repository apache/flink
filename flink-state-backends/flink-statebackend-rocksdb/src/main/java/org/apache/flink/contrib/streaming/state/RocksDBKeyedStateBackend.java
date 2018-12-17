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
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.contrib.streaming.state.iterator.RocksStateKeysIterator;
import org.apache.flink.contrib.streaming.state.snapshot.RocksDBSnapshotStrategyBase;
import org.apache.flink.contrib.streaming.state.snapshot.RocksFullSnapshotStrategy;
import org.apache.flink.contrib.streaming.state.snapshot.RocksIncrementalSnapshotStrategy;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.DirectoryStateHandle;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.KeyExtractorFunction;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.PriorityComparator;
import org.apache.flink.runtime.state.PriorityQueueSetFactory;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredPriorityQueueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.SnappyStreamCompressionDecorator;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.heap.KeyGroupPartitionedPriorityQueue;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ResourceGuard;
import org.apache.flink.util.StateMigrationException;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Snapshot;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.END_OF_KEY_GROUP_MARK;
import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.SST_FILE_SUFFIX;
import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.clearMetaDataFollowsFlag;
import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.hasMetaDataFollowsFlag;

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

	/** String that identifies the operator that owns this backend. */
	private final String operatorIdentifier;

	/** The column family options from the options factory. */
	private final ColumnFamilyOptions columnOptions;

	/** The DB options from the options factory. */
	private final DBOptions dbOptions;

	/** Path where this configured instance stores its data directory. */
	private final File instanceBasePath;

	/** Path where this configured instance stores its RocksDB database. */
	private final File instanceRocksDBPath;

	/**
	 * Protects access to RocksDB in other threads, like the checkpointing thread from parallel call that disposes the
	 * RocksDb object.
	 */
	private final ResourceGuard rocksDBResourceGuard;

	/**
	 * Our RocksDB database, this is used by the actual subclasses of {@link AbstractRocksDBState}
	 * to store state. The different k/v states that we have don't each have their own RocksDB
	 * instance. They all write to this instance but to their own column family.
	 */
	protected RocksDB db;

	/**
	 * We are not using the default column family for Flink state ops, but we still need to remember this handle so that
	 * we can close it properly when the backend is closed. This is required by RocksDB's native memory management.
	 */
	private ColumnFamilyHandle defaultColumnFamily;

	/**
	 * The write options to use in the states. We disable write ahead logging.
	 */
	private final WriteOptions writeOptions;

	/**
	 * Information about the k/v states, maintained in the order as we create them. This is used to retrieve the
	 * column family that is used for a state and also for sanity checks when restoring.
	 */
	private final LinkedHashMap<String, Tuple2<ColumnFamilyHandle, RegisteredStateMetaInfoBase>> kvStateInformation;

	/** Number of bytes required to prefix the key groups. */
	private final int keyGroupPrefixBytes;

	/** True if incremental checkpointing is enabled. */
	private final boolean enableIncrementalCheckpointing;

	/** The configuration of local recovery. */
	private final LocalRecoveryConfig localRecoveryConfig;

	/** The checkpoint snapshot strategy, e.g., if we use full or incremental checkpoints, local state, and so on. */
	private RocksDBSnapshotStrategyBase<K> checkpointSnapshotStrategy;

	/** The savepoint snapshot strategy. */
	private RocksDBSnapshotStrategyBase<K> savepointSnapshotStrategy;

	/** Factory for priority queue state. */
	private final PriorityQueueSetFactory priorityQueueFactory;

	/** Shared wrapper for batch writes to the RocksDB instance. */
	private RocksDBWriteBatchWrapper writeBatchWrapper;

	private final RocksDBNativeMetricOptions metricOptions;

	private final MetricGroup metricGroup;

	/** The native metrics monitor. */
	private RocksDBNativeMetricMonitor nativeMetricMonitor;

	public RocksDBKeyedStateBackend(
		String operatorIdentifier,
		ClassLoader userCodeClassLoader,
		File instanceBasePath,
		DBOptions dbOptions,
		ColumnFamilyOptions columnFamilyOptions,
		TaskKvStateRegistry kvStateRegistry,
		TypeSerializer<K> keySerializer,
		int numberOfKeyGroups,
		KeyGroupRange keyGroupRange,
		ExecutionConfig executionConfig,
		boolean enableIncrementalCheckpointing,
		LocalRecoveryConfig localRecoveryConfig,
		RocksDBStateBackend.PriorityQueueStateType priorityQueueStateType,
		TtlTimeProvider ttlTimeProvider,
		RocksDBNativeMetricOptions metricOptions,
		MetricGroup metricGroup
	) throws IOException {

		super(kvStateRegistry, keySerializer, userCodeClassLoader,
			numberOfKeyGroups, keyGroupRange, executionConfig, ttlTimeProvider);

		this.operatorIdentifier = Preconditions.checkNotNull(operatorIdentifier);

		this.enableIncrementalCheckpointing = enableIncrementalCheckpointing;
		this.rocksDBResourceGuard = new ResourceGuard();

		// ensure that we use the right merge operator, because other code relies on this
		this.columnOptions = Preconditions.checkNotNull(columnFamilyOptions)
			.setMergeOperatorName(MERGE_OPERATOR_NAME);

		this.dbOptions = Preconditions.checkNotNull(dbOptions);

		this.instanceBasePath = Preconditions.checkNotNull(instanceBasePath);
		this.instanceRocksDBPath = new File(instanceBasePath, "db");

		checkAndCreateDirectory(instanceBasePath);

		if (instanceRocksDBPath.exists()) {
			// Clear the base directory when the backend is created
			// in case something crashed and the backend never reached dispose()
			cleanInstanceBasePath();
		}

		this.localRecoveryConfig = Preconditions.checkNotNull(localRecoveryConfig);
		this.keyGroupPrefixBytes =
			RocksDBKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(getNumberOfKeyGroups());
		this.kvStateInformation = new LinkedHashMap<>();

		this.writeOptions = new WriteOptions().setDisableWAL(true);

		this.metricOptions = metricOptions;
		this.metricGroup = metricGroup;

		switch (priorityQueueStateType) {
			case HEAP:
				this.priorityQueueFactory = new HeapPriorityQueueSetFactory(keyGroupRange, numberOfKeyGroups, 128);
				break;
			case ROCKSDB:
				this.priorityQueueFactory = new RocksDBPriorityQueueSetFactory();
				break;
			default:
				throw new IllegalArgumentException("Unknown priority queue state type: " + priorityQueueStateType);
		}
	}

	private static void checkAndCreateDirectory(File directory) throws IOException {
		if (directory.exists()) {
			if (!directory.isDirectory()) {
				throw new IOException("Not a directory: " + directory);
			}
		} else {
			if (!directory.mkdirs()) {
				throw new IOException(
					String.format("Could not create RocksDB data directory at %s.", directory));
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <N> Stream<K> getKeys(String state, N namespace) {
		Tuple2<ColumnFamilyHandle, RegisteredStateMetaInfoBase> columnInfo = kvStateInformation.get(state);
		if (columnInfo == null || !(columnInfo.f1 instanceof RegisteredKeyValueStateBackendMetaInfo)) {
			return Stream.empty();
		}

		RegisteredKeyValueStateBackendMetaInfo<N, ?> registeredKeyValueStateBackendMetaInfo =
			(RegisteredKeyValueStateBackendMetaInfo<N, ?>) columnInfo.f1;

		final TypeSerializer<N> namespaceSerializer = registeredKeyValueStateBackendMetaInfo.getNamespaceSerializer();
		final DataOutputSerializer namespaceOutputView = new DataOutputSerializer(8);
		boolean ambiguousKeyPossible = RocksDBKeySerializationUtils.isAmbiguousKeyPossible(keySerializer, namespaceSerializer);
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

		RocksIteratorWrapper iterator = getRocksIterator(db, columnInfo.f0);
		iterator.seekToFirst();

		final RocksStateKeysIterator<K> iteratorWrapper = new RocksStateKeysIterator<>(iterator, state, keySerializer, keyGroupPrefixBytes,
			ambiguousKeyPossible, nameSpaceBytes);

		Stream<K> targetStream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(iteratorWrapper, Spliterator.ORDERED), false);
		return targetStream.onClose(iteratorWrapper::close);
	}

	@VisibleForTesting
	public ColumnFamilyHandle getColumnFamilyHandle(String state) {
		Tuple2<ColumnFamilyHandle, ?> columnInfo = kvStateInformation.get(state);
		return columnInfo != null ? columnInfo.f0 : null;
	}

	private void registerKvStateInformation(String columnFamilyName, Tuple2<ColumnFamilyHandle, RegisteredStateMetaInfoBase> registeredColumn) {
		kvStateInformation.put(columnFamilyName, registeredColumn);

		if (nativeMetricMonitor != null) {
			nativeMetricMonitor.registerColumnFamily(columnFamilyName, registeredColumn.f0);
		}
	}

	/**
	 * Should only be called by one thread, and only after all accesses to the DB happened.
	 */
	@Override
	public void dispose() {
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

			// RocksDB's native memory management requires that *all* CFs (including default) are closed before the
			// DB is closed. See:
			// https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#opening-a-database-with-column-families
			// Start with default CF ...
			IOUtils.closeQuietly(defaultColumnFamily);

			// ... continue with the ones created by Flink...
			for (Tuple2<ColumnFamilyHandle, RegisteredStateMetaInfoBase> columnMetaData :
				kvStateInformation.values()) {
				IOUtils.closeQuietly(columnMetaData.f0);
			}

			// ... and finally close the DB instance ...
			IOUtils.closeQuietly(db);

			// invalidate the reference
			db = null;

			IOUtils.closeQuietly(columnOptions);
			IOUtils.closeQuietly(dbOptions);
			IOUtils.closeQuietly(writeOptions);
			kvStateInformation.clear();

			cleanInstanceBasePath();
		}
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
		LOG.info("Deleting existing instance base directory {}.", instanceBasePath);

		try {
			FileUtils.deleteDirectory(instanceBasePath);
		} catch (IOException ex) {
			LOG.warn("Could not delete instance base path for RocksDB: " + instanceBasePath, ex);
		}
	}

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
			CheckpointType.SAVEPOINT == checkpointOptions.getCheckpointType() ?
				savepointSnapshotStrategy : checkpointSnapshotStrategy;

		RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotRunner =
			chosenSnapshotStrategy.snapshot(checkpointId, timestamp, streamFactory, checkpointOptions);

		chosenSnapshotStrategy.logSyncCompleted(streamFactory, startTime);

		return snapshotRunner;
	}

	@Override
	public void restore(Collection<KeyedStateHandle> restoreState) throws Exception {

		LOG.info("Initializing RocksDB keyed state backend.");

		if (LOG.isDebugEnabled()) {
			LOG.debug("Restoring snapshot from state handles: {}.", restoreState);
		}

		// clear all meta data
		kvStateInformation.clear();

		try {
			RocksDBIncrementalRestoreOperation<K> incrementalRestoreOperation = null;
			if (restoreState == null || restoreState.isEmpty()) {
				createDB();
			} else {
				KeyedStateHandle firstStateHandle = restoreState.iterator().next();
				if (firstStateHandle instanceof IncrementalKeyedStateHandle
					|| firstStateHandle instanceof IncrementalLocalKeyedStateHandle) {
					incrementalRestoreOperation = new RocksDBIncrementalRestoreOperation<>(this);
					incrementalRestoreOperation.restore(restoreState);
				} else {
					RocksDBFullRestoreOperation<K> fullRestoreOperation = new RocksDBFullRestoreOperation<>(this);
					fullRestoreOperation.doRestore(restoreState);
				}
			}

			initializeSnapshotStrategy(incrementalRestoreOperation);
		} catch (Exception ex) {
			dispose();
			throw ex;
		}
	}

	@VisibleForTesting
	void initializeSnapshotStrategy(
		@Nullable RocksDBIncrementalRestoreOperation<K> incrementalRestoreOperation) {

		this.savepointSnapshotStrategy =
			new RocksFullSnapshotStrategy<>(
				db,
				rocksDBResourceGuard,
				keySerializer,
				kvStateInformation,
				keyGroupRange,
				keyGroupPrefixBytes,
				localRecoveryConfig,
				cancelStreamRegistry,
				keyGroupCompressionDecorator);

		if (enableIncrementalCheckpointing) {
			final UUID backendUID;
			final SortedMap<Long, Set<StateHandleID>> materializedSstFiles;
			final long lastCompletedCheckpointId;

			if (incrementalRestoreOperation == null) {
				backendUID = UUID.randomUUID();
				materializedSstFiles = new TreeMap<>();
				lastCompletedCheckpointId = -1L;
			} else {
				backendUID = Preconditions.checkNotNull(incrementalRestoreOperation.getRestoredBackendUID());
				materializedSstFiles = Preconditions.checkNotNull(incrementalRestoreOperation.getRestoredSstFiles());
				lastCompletedCheckpointId = incrementalRestoreOperation.getLastCompletedCheckpointId();
				Preconditions.checkState(lastCompletedCheckpointId >= 0L);
			}
			// TODO eventually we might want to separate savepoint and snapshot strategy, i.e. having 2 strategies.
			this.checkpointSnapshotStrategy = new RocksIncrementalSnapshotStrategy<>(
				db,
				rocksDBResourceGuard,
				keySerializer,
				kvStateInformation,
				keyGroupRange,
				keyGroupPrefixBytes,
				localRecoveryConfig,
				cancelStreamRegistry,
				instanceBasePath,
				backendUID,
				materializedSstFiles,
				lastCompletedCheckpointId);
		} else {
			this.checkpointSnapshotStrategy = savepointSnapshotStrategy;
		}
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

	private void createDB() throws IOException {
		List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(1);
		this.db = openDB(instanceRocksDBPath.getAbsolutePath(), Collections.emptyList(), columnFamilyHandles);
		this.writeBatchWrapper = new RocksDBWriteBatchWrapper(db, writeOptions);
		this.defaultColumnFamily = columnFamilyHandles.get(0);
	}

	private RocksDB openDB(
		String path,
		List<ColumnFamilyDescriptor> stateColumnFamilyDescriptors,
		List<ColumnFamilyHandle> stateColumnFamilyHandles) throws IOException {

		List<ColumnFamilyDescriptor> columnFamilyDescriptors =
			new ArrayList<>(1 + stateColumnFamilyDescriptors.size());

		// we add the required descriptor for the default CF in FIRST position, see
		// https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#opening-a-database-with-column-families
		columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnOptions));
		columnFamilyDescriptors.addAll(stateColumnFamilyDescriptors);

		RocksDB dbRef;

		try {
			dbRef = RocksDB.open(
				Preconditions.checkNotNull(dbOptions),
				Preconditions.checkNotNull(path),
				columnFamilyDescriptors,
				stateColumnFamilyHandles);
		} catch (RocksDBException e) {
			throw new IOException("Error while opening RocksDB instance.", e);
		}

		// requested + default CF
		Preconditions.checkState(1 + stateColumnFamilyDescriptors.size() == stateColumnFamilyHandles.size(),
			"Not all requested column family handles have been created");

		if (this.metricOptions.isEnabled()) {
			this.nativeMetricMonitor = new RocksDBNativeMetricMonitor(
				dbRef,
				metricOptions,
				metricGroup
			);
		}

		return dbRef;
	}

	/**
	 * Encapsulates the process of restoring a RocksDBKeyedStateBackend from a full snapshot.
	 */
	private static final class RocksDBFullRestoreOperation<K> {

		private final RocksDBKeyedStateBackend<K> rocksDBKeyedStateBackend;

		/** Current key-groups state handle from which we restore key-groups. */
		private KeyGroupsStateHandle currentKeyGroupsStateHandle;
		/** Current input stream we obtained from currentKeyGroupsStateHandle. */
		private FSDataInputStream currentStateHandleInStream;
		/** Current data input view that wraps currentStateHandleInStream. */
		private DataInputView currentStateHandleInView;
		/** Current list of ColumnFamilyHandles for all column families we restore from currentKeyGroupsStateHandle. */
		private List<ColumnFamilyHandle> currentStateHandleKVStateColumnFamilies;
		/** The compression decorator that was used for writing the state, as determined by the meta data. */
		private StreamCompressionDecorator keygroupStreamCompressionDecorator;

		/**
		 * Creates a restore operation object for the given state backend instance.
		 *
		 * @param rocksDBKeyedStateBackend the state backend into which we restore
		 */
		public RocksDBFullRestoreOperation(RocksDBKeyedStateBackend<K> rocksDBKeyedStateBackend) {
			this.rocksDBKeyedStateBackend = Preconditions.checkNotNull(rocksDBKeyedStateBackend);
		}

		/**
		 * Restores all key-groups data that is referenced by the passed state handles.
		 *
		 * @param keyedStateHandles List of all key groups state handles that shall be restored.
		 */
		public void doRestore(Collection<KeyedStateHandle> keyedStateHandles)
			throws IOException, StateMigrationException, RocksDBException {

			rocksDBKeyedStateBackend.createDB();

			for (KeyedStateHandle keyedStateHandle : keyedStateHandles) {
				if (keyedStateHandle != null) {

					if (!(keyedStateHandle instanceof KeyGroupsStateHandle)) {
						throw new IllegalStateException("Unexpected state handle type, " +
							"expected: " + KeyGroupsStateHandle.class +
							", but found: " + keyedStateHandle.getClass());
					}
					this.currentKeyGroupsStateHandle = (KeyGroupsStateHandle) keyedStateHandle;
					restoreKeyGroupsInStateHandle();
				}
			}
		}

		/**
		 * Restore one key groups state handle.
		 */
		private void restoreKeyGroupsInStateHandle()
			throws IOException, StateMigrationException, RocksDBException {
			try {
				currentStateHandleInStream = currentKeyGroupsStateHandle.openInputStream();
				rocksDBKeyedStateBackend.cancelStreamRegistry.registerCloseable(currentStateHandleInStream);
				currentStateHandleInView = new DataInputViewStreamWrapper(currentStateHandleInStream);
				restoreKVStateMetaData();
				restoreKVStateData();
			} finally {
				if (rocksDBKeyedStateBackend.cancelStreamRegistry.unregisterCloseable(currentStateHandleInStream)) {
					IOUtils.closeQuietly(currentStateHandleInStream);
				}
			}
		}

		/**
		 * Restore the KV-state / ColumnFamily meta data for all key-groups referenced by the current state handle.
		 */
		private void restoreKVStateMetaData() throws IOException, StateMigrationException, RocksDBException {

			// isSerializerPresenceRequired flag is set to false, since for the RocksDB state backend,
			// deserialization of state happens lazily during runtime; we depend on the fact
			// that the new serializer for states could be compatible, and therefore the restore can continue
			// without old serializers required to be present.
			KeyedBackendSerializationProxy<K> serializationProxy =
				new KeyedBackendSerializationProxy<>(rocksDBKeyedStateBackend.userCodeClassLoader);

			serializationProxy.read(currentStateHandleInView);

			// check for key serializer compatibility; this also reconfigures the
			// key serializer to be compatible, if it is required and is possible
			if (!serializationProxy.getKeySerializerConfigSnapshot()
					.resolveSchemaCompatibility(rocksDBKeyedStateBackend.keySerializer).isCompatibleAsIs()) {
				throw new StateMigrationException("The new key serializer must be compatible.");
			}

			this.keygroupStreamCompressionDecorator = serializationProxy.isUsingKeyGroupCompression() ?
				SnappyStreamCompressionDecorator.INSTANCE : UncompressedStreamCompressionDecorator.INSTANCE;

			List<StateMetaInfoSnapshot> restoredMetaInfos =
				serializationProxy.getStateMetaInfoSnapshots();
			currentStateHandleKVStateColumnFamilies = new ArrayList<>(restoredMetaInfos.size());

			for (StateMetaInfoSnapshot restoredMetaInfo : restoredMetaInfos) {

				Tuple2<ColumnFamilyHandle, RegisteredStateMetaInfoBase> registeredColumn =
					rocksDBKeyedStateBackend.kvStateInformation.get(restoredMetaInfo.getName());

				if (registeredColumn == null) {
					byte[] nameBytes = restoredMetaInfo.getName().getBytes(ConfigConstants.DEFAULT_CHARSET);

					ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(
						nameBytes,
						rocksDBKeyedStateBackend.columnOptions);

					ColumnFamilyHandle columnFamily = rocksDBKeyedStateBackend.db.createColumnFamily(columnFamilyDescriptor);

					// create a meta info for the state on restore;
					// this allows us to retain the state in future snapshots even if it wasn't accessed
					RegisteredStateMetaInfoBase stateMetaInfo =
						RegisteredStateMetaInfoBase.fromMetaInfoSnapshot(restoredMetaInfo);
					registeredColumn = new Tuple2<>(columnFamily, stateMetaInfo);
					rocksDBKeyedStateBackend.kvStateInformation.put(restoredMetaInfo.getName(), registeredColumn);

				} else {
					// TODO with eager state registration in place, check here for serializer migration strategies
				}
				currentStateHandleKVStateColumnFamilies.add(registeredColumn.f0);
			}
		}

		/**
		 * Restore the KV-state / ColumnFamily data for all key-groups referenced by the current state handle.
		 */
		private void restoreKVStateData() throws IOException, RocksDBException {
			//for all key-groups in the current state handle...
			try (RocksDBWriteBatchWrapper writeBatchWrapper = new RocksDBWriteBatchWrapper(rocksDBKeyedStateBackend.db)) {
				for (Tuple2<Integer, Long> keyGroupOffset : currentKeyGroupsStateHandle.getGroupRangeOffsets()) {
					int keyGroup = keyGroupOffset.f0;

					// Check that restored key groups all belong to the backend
					Preconditions.checkState(rocksDBKeyedStateBackend.getKeyGroupRange().contains(keyGroup),
						"The key group must belong to the backend");

					long offset = keyGroupOffset.f1;
					//not empty key-group?
					if (0L != offset) {
						currentStateHandleInStream.seek(offset);
						try (InputStream compressedKgIn = keygroupStreamCompressionDecorator.decorateWithCompression(currentStateHandleInStream)) {
							DataInputViewStreamWrapper compressedKgInputView = new DataInputViewStreamWrapper(compressedKgIn);
							//TODO this could be aware of keyGroupPrefixBytes and write only one byte if possible
							int kvStateId = compressedKgInputView.readShort();
							ColumnFamilyHandle handle = currentStateHandleKVStateColumnFamilies.get(kvStateId);
							//insert all k/v pairs into DB
							boolean keyGroupHasMoreKeys = true;
							while (keyGroupHasMoreKeys) {
								byte[] key = BytePrimitiveArraySerializer.INSTANCE.deserialize(compressedKgInputView);
								byte[] value = BytePrimitiveArraySerializer.INSTANCE.deserialize(compressedKgInputView);
								if (hasMetaDataFollowsFlag(key)) {
									//clear the signal bit in the key to make it ready for insertion again
									clearMetaDataFollowsFlag(key);
									writeBatchWrapper.put(handle, key, value);
									//TODO this could be aware of keyGroupPrefixBytes and write only one byte if possible
									kvStateId = END_OF_KEY_GROUP_MARK
										& compressedKgInputView.readShort();
									if (END_OF_KEY_GROUP_MARK == kvStateId) {
										keyGroupHasMoreKeys = false;
									} else {
										handle = currentStateHandleKVStateColumnFamilies.get(kvStateId);
									}
								} else {
									writeBatchWrapper.put(handle, key, value);
								}
							}
						}
					}
				}
			}
		}
	}

	/**
	 * Encapsulates the process of restoring a RocksDBKeyedStateBackend from an incremental snapshot.
	 */
	private static class RocksDBIncrementalRestoreOperation<T> {

		private final RocksDBKeyedStateBackend<T> stateBackend;
		private final SortedMap<Long, Set<StateHandleID>> restoredSstFiles;
		private UUID restoredBackendUID;
		private long lastCompletedCheckpointId;

		private RocksDBIncrementalRestoreOperation(RocksDBKeyedStateBackend<T> stateBackend) {

			this.stateBackend = stateBackend;
			this.restoredSstFiles = new TreeMap<>();
		}

		SortedMap<Long, Set<StateHandleID>> getRestoredSstFiles() {
			return restoredSstFiles;
		}

		UUID getRestoredBackendUID() {
			return restoredBackendUID;
		}

		long getLastCompletedCheckpointId() {
			return lastCompletedCheckpointId;
		}

		/**
		 * Root method that branches for different implementations of {@link KeyedStateHandle}.
		 */
		void restore(Collection<KeyedStateHandle> restoreStateHandles) throws Exception {

			if (restoreStateHandles.isEmpty()) {
				return;
			}

			final KeyedStateHandle theFirstStateHandle = restoreStateHandles.iterator().next();

			boolean isRescaling = (restoreStateHandles.size() > 1 ||
				!Objects.equals(theFirstStateHandle.getKeyGroupRange(), stateBackend.keyGroupRange));

			if (!isRescaling) {
				restoreWithoutRescaling(theFirstStateHandle);
			} else {
				restoreWithRescaling(restoreStateHandles);
			}
		}

		/**
		 * Recovery from a single remote incremental state without rescaling.
		 */
		void restoreWithoutRescaling(KeyedStateHandle rawStateHandle) throws Exception {

			IncrementalLocalKeyedStateHandle localKeyedStateHandle;
			List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;
			List<ColumnFamilyDescriptor> columnFamilyDescriptors;

			// Recovery from remote incremental state.
			Path temporaryRestoreInstancePath = new Path(
				stateBackend.instanceBasePath.getAbsolutePath(),
				UUID.randomUUID().toString());

			try {
				if (rawStateHandle instanceof IncrementalKeyedStateHandle) {

					IncrementalKeyedStateHandle restoreStateHandle = (IncrementalKeyedStateHandle) rawStateHandle;

					// read state data.
					transferAllStateDataToDirectory(restoreStateHandle, temporaryRestoreInstancePath);

					stateMetaInfoSnapshots = readMetaData(restoreStateHandle.getMetaStateHandle());
					columnFamilyDescriptors = createAndRegisterColumnFamilyDescriptors(stateMetaInfoSnapshots);

					// since we transferred all remote state to a local directory, we can use the same code as for
					// local recovery.
					localKeyedStateHandle = new IncrementalLocalKeyedStateHandle(
						restoreStateHandle.getBackendIdentifier(),
						restoreStateHandle.getCheckpointId(),
						new DirectoryStateHandle(temporaryRestoreInstancePath),
						restoreStateHandle.getKeyGroupRange(),
						restoreStateHandle.getMetaStateHandle(),
						restoreStateHandle.getSharedState().keySet());
				} else if (rawStateHandle instanceof IncrementalLocalKeyedStateHandle) {

					// Recovery from local incremental state.
					localKeyedStateHandle = (IncrementalLocalKeyedStateHandle) rawStateHandle;
					stateMetaInfoSnapshots = readMetaData(localKeyedStateHandle.getMetaDataState());
					columnFamilyDescriptors = createAndRegisterColumnFamilyDescriptors(stateMetaInfoSnapshots);
				} else {
					throw new IllegalStateException("Unexpected state handle type, " +
						"expected " + IncrementalKeyedStateHandle.class + " or " + IncrementalLocalKeyedStateHandle.class +
						", but found " + rawStateHandle.getClass());
				}

				restoreLocalStateIntoFullInstance(
					localKeyedStateHandle,
					columnFamilyDescriptors,
					stateMetaInfoSnapshots);
			} finally {
				FileSystem restoreFileSystem = temporaryRestoreInstancePath.getFileSystem();
				if (restoreFileSystem.exists(temporaryRestoreInstancePath)) {
					restoreFileSystem.delete(temporaryRestoreInstancePath, true);
				}
			}
		}

		/**
		 * Recovery from multi incremental states with rescaling. For rescaling, this method creates a temporary
		 * RocksDB instance for a key-groups shard. All contents from the temporary instance are copied into the
		 * real restore instance and then the temporary instance is discarded.
		 */
		void restoreWithRescaling(Collection<KeyedStateHandle> restoreStateHandles) throws Exception {

			this.restoredBackendUID = UUID.randomUUID();

			initTargetDB(restoreStateHandles, stateBackend.keyGroupRange);

			byte[] startKeyGroupPrefixBytes = new byte[stateBackend.keyGroupPrefixBytes];
			RocksDBKeySerializationUtils.serializeKeyGroup(stateBackend.getKeyGroupRange().getStartKeyGroup(), startKeyGroupPrefixBytes);

			byte[] stopKeyGroupPrefixBytes = new byte[stateBackend.keyGroupPrefixBytes];
			RocksDBKeySerializationUtils.serializeKeyGroup(stateBackend.getKeyGroupRange().getEndKeyGroup() + 1, stopKeyGroupPrefixBytes);

			for (KeyedStateHandle rawStateHandle : restoreStateHandles) {

				if (!(rawStateHandle instanceof IncrementalKeyedStateHandle)) {
					throw new IllegalStateException("Unexpected state handle type, " +
						"expected " + IncrementalKeyedStateHandle.class +
						", but found " + rawStateHandle.getClass());
				}

				Path temporaryRestoreInstancePath = new Path(stateBackend.instanceBasePath.getAbsolutePath() + UUID.randomUUID().toString());
				try (RestoredDBInstance tmpRestoreDBInfo = restoreDBInstanceFromStateHandle(
						(IncrementalKeyedStateHandle) rawStateHandle,
						temporaryRestoreInstancePath);
					RocksDBWriteBatchWrapper writeBatchWrapper = new RocksDBWriteBatchWrapper(stateBackend.db)) {

					List<ColumnFamilyDescriptor> tmpColumnFamilyDescriptors = tmpRestoreDBInfo.columnFamilyDescriptors;
					List<ColumnFamilyHandle> tmpColumnFamilyHandles = tmpRestoreDBInfo.columnFamilyHandles;

					// iterating only the requested descriptors automatically skips the default column family handle
					for (int i = 0; i < tmpColumnFamilyDescriptors.size(); ++i) {
						ColumnFamilyHandle tmpColumnFamilyHandle = tmpColumnFamilyHandles.get(i);
						ColumnFamilyDescriptor tmpColumnFamilyDescriptor = tmpColumnFamilyDescriptors.get(i);

						ColumnFamilyHandle targetColumnFamilyHandle = getOrRegisterColumnFamilyHandle(
							tmpColumnFamilyDescriptor, null, tmpRestoreDBInfo.stateMetaInfoSnapshots.get(i));

						try (RocksIteratorWrapper iterator = getRocksIterator(tmpRestoreDBInfo.db, tmpColumnFamilyHandle)) {

							iterator.seek(startKeyGroupPrefixBytes);

							while (iterator.isValid()) {

								if (RocksDBIncrementalCheckpointUtils.beforeThePrefixBytes(iterator.key(), stopKeyGroupPrefixBytes)) {
									writeBatchWrapper.put(targetColumnFamilyHandle, iterator.key(), iterator.value());
								} else {
									// Since the iterator will visit the record according to the sorted order,
									// we can just break here.
									break;
								}

								iterator.next();
							}
						} // releases native iterator resources
					}
				} finally {
					FileSystem restoreFileSystem = temporaryRestoreInstancePath.getFileSystem();
					if (restoreFileSystem.exists(temporaryRestoreInstancePath)) {
						restoreFileSystem.delete(temporaryRestoreInstancePath, true);
					}
				}
			}
		}

		private class RestoredDBInstance implements AutoCloseable {

			@Nonnull
			private final RocksDB db;

			@Nonnull
			private final ColumnFamilyHandle defaultColumnFamilyHandle;

			@Nonnull
			private final List<ColumnFamilyHandle> columnFamilyHandles;

			@Nonnull
			private final List<ColumnFamilyDescriptor> columnFamilyDescriptors;

			@Nonnull
			private final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;

			private RestoredDBInstance(
				@Nonnull RocksDB db,
				@Nonnull List<ColumnFamilyHandle> columnFamilyHandles,
				@Nonnull List<ColumnFamilyDescriptor> columnFamilyDescriptors,
				@Nonnull List<StateMetaInfoSnapshot> stateMetaInfoSnapshots) {
				this.db = db;
				this.columnFamilyHandles = columnFamilyHandles;
				this.defaultColumnFamilyHandle = this.columnFamilyHandles.remove(0);
				this.columnFamilyDescriptors = columnFamilyDescriptors;
				this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
			}

			@Override
			public void close() {

				IOUtils.closeQuietly(defaultColumnFamilyHandle);

				for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
					IOUtils.closeQuietly(columnFamilyHandle);
				}

				IOUtils.closeQuietly(db);
			}
		}

		private RestoredDBInstance restoreDBInstanceFromStateHandle(
			IncrementalKeyedStateHandle restoreStateHandle,
			Path temporaryRestoreInstancePath) throws Exception {

			transferAllStateDataToDirectory(restoreStateHandle, temporaryRestoreInstancePath);

			// read meta data
			List<StateMetaInfoSnapshot> stateMetaInfoSnapshots =
				readMetaData(restoreStateHandle.getMetaStateHandle());

			List<ColumnFamilyDescriptor> columnFamilyDescriptors =
				createAndRegisterColumnFamilyDescriptors(stateMetaInfoSnapshots);

			List<ColumnFamilyHandle> columnFamilyHandles =
				new ArrayList<>(stateMetaInfoSnapshots.size() + 1);

			RocksDB restoreDb = stateBackend.openDB(
				temporaryRestoreInstancePath.getPath(),
				columnFamilyDescriptors,
				columnFamilyHandles);

			return new RestoredDBInstance(restoreDb, columnFamilyHandles, columnFamilyDescriptors, stateMetaInfoSnapshots);
		}

		private ColumnFamilyHandle getOrRegisterColumnFamilyHandle(
			ColumnFamilyDescriptor columnFamilyDescriptor,
			ColumnFamilyHandle columnFamilyHandle,
			StateMetaInfoSnapshot stateMetaInfoSnapshot) throws RocksDBException {

			Tuple2<ColumnFamilyHandle, RegisteredStateMetaInfoBase> registeredStateMetaInfoEntry =
				stateBackend.kvStateInformation.get(stateMetaInfoSnapshot.getName());

			if (null == registeredStateMetaInfoEntry) {
				// create a meta info for the state on restore;
				// this allows us to retain the state in future snapshots even if it wasn't accessed
				RegisteredStateMetaInfoBase stateMetaInfo =
					RegisteredStateMetaInfoBase.fromMetaInfoSnapshot(stateMetaInfoSnapshot);
				registeredStateMetaInfoEntry =
					new Tuple2<>(
						columnFamilyHandle != null ? columnFamilyHandle : stateBackend.db.createColumnFamily(columnFamilyDescriptor),
						stateMetaInfo);

				stateBackend.registerKvStateInformation(
					stateMetaInfoSnapshot.getName(),
					registeredStateMetaInfoEntry);
			}

			return registeredStateMetaInfoEntry.f0;
		}

		/**
		 * This method first try to find a initial handle to init the target db, if the initial handle
		 * is not null, we just init the target db with the handle and clip it with the target key-group
		 * range. If the initial handle is null we create a empty db as the target db.
		 */
		private void initTargetDB(
			Collection<KeyedStateHandle> restoreStateHandles,
			KeyGroupRange targetKeyGroupRange) throws Exception {

			IncrementalKeyedStateHandle initialHandle = (IncrementalKeyedStateHandle) RocksDBIncrementalCheckpointUtils.chooseTheBestStateHandleForInitial(
				restoreStateHandles, targetKeyGroupRange);

			if (initialHandle != null) {
				restoreStateHandles.remove(initialHandle);
				RestoredDBInstance restoreDBInfo = null;
				Path instancePath = new Path(stateBackend.instanceRocksDBPath.getAbsolutePath());
				try {
					restoreDBInfo = restoreDBInstanceFromStateHandle(
						initialHandle,
						instancePath);

					RocksDBIncrementalCheckpointUtils.clipDBWithKeyGroupRange(
						restoreDBInfo.db,
						restoreDBInfo.columnFamilyHandles,
						targetKeyGroupRange,
						initialHandle.getKeyGroupRange(),
						stateBackend.keyGroupPrefixBytes);

					stateBackend.db = restoreDBInfo.db;
					stateBackend.defaultColumnFamily = restoreDBInfo.defaultColumnFamilyHandle;
					stateBackend.writeBatchWrapper =
						new RocksDBWriteBatchWrapper(stateBackend.db, stateBackend.writeOptions);

					for (int i = 0; i < restoreDBInfo.stateMetaInfoSnapshots.size(); ++i) {
						getOrRegisterColumnFamilyHandle(
							restoreDBInfo.columnFamilyDescriptors.get(i),
							restoreDBInfo.columnFamilyHandles.get(i),
							restoreDBInfo.stateMetaInfoSnapshots.get(i));
					}
				} catch (Exception e) {
					if (restoreDBInfo != null) {
						restoreDBInfo.close();
					}
					FileSystem restoreFileSystem = instancePath.getFileSystem();
					if (restoreFileSystem.exists(instancePath)) {
						restoreFileSystem.delete(instancePath, true);
					}
					throw e;
				}
			} else {
				List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(1);
				stateBackend.db = stateBackend.openDB(
					stateBackend.instanceRocksDBPath.getAbsolutePath(),
					Collections.emptyList(),
					columnFamilyHandles);
				stateBackend.defaultColumnFamily = columnFamilyHandles.get(0);
				stateBackend.writeBatchWrapper =
					new RocksDBWriteBatchWrapper(stateBackend.db, stateBackend.writeOptions);
			}
		}

		/**
		 * This method recreates and registers all {@link ColumnFamilyDescriptor} from Flink's state meta data snapshot.
		 */
		private List<ColumnFamilyDescriptor> createAndRegisterColumnFamilyDescriptors(
			List<StateMetaInfoSnapshot> stateMetaInfoSnapshots) {

			List<ColumnFamilyDescriptor> columnFamilyDescriptors =
				new ArrayList<>(stateMetaInfoSnapshots.size());

			for (StateMetaInfoSnapshot stateMetaInfoSnapshot : stateMetaInfoSnapshots) {

				ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(
					stateMetaInfoSnapshot.getName().getBytes(ConfigConstants.DEFAULT_CHARSET),
					stateBackend.columnOptions);

				columnFamilyDescriptors.add(columnFamilyDescriptor);
			}
			return columnFamilyDescriptors;
		}

		/**
		 * This method implements the core of the restore logic that unifies how local and remote state are recovered.
		 */
		private void restoreLocalStateIntoFullInstance(
			IncrementalLocalKeyedStateHandle restoreStateHandle,
			List<ColumnFamilyDescriptor> columnFamilyDescriptors,
			List<StateMetaInfoSnapshot> stateMetaInfoSnapshots) throws Exception {
			// pick up again the old backend id, so the we can reference existing state
			this.restoredBackendUID = restoreStateHandle.getBackendIdentifier();

			LOG.debug("Restoring keyed backend uid in operator {} from incremental snapshot to {}.",
				stateBackend.operatorIdentifier, this.restoredBackendUID);

			// create hard links in the instance directory
			if (!stateBackend.instanceRocksDBPath.mkdirs()) {
				throw new IOException("Could not create RocksDB data directory.");
			}

			Path restoreSourcePath = restoreStateHandle.getDirectoryStateHandle().getDirectory();
			restoreInstanceDirectoryFromPath(restoreSourcePath);

			List<ColumnFamilyHandle> columnFamilyHandles =
				new ArrayList<>(1 + columnFamilyDescriptors.size());

			stateBackend.db = stateBackend.openDB(
				stateBackend.instanceRocksDBPath.getAbsolutePath(),
				columnFamilyDescriptors, columnFamilyHandles);

			// extract and store the default column family which is located at the first index
			stateBackend.defaultColumnFamily = columnFamilyHandles.remove(0);
			stateBackend.writeBatchWrapper = new RocksDBWriteBatchWrapper(stateBackend.db, stateBackend.writeOptions);

			for (int i = 0; i < columnFamilyDescriptors.size(); ++i) {
				StateMetaInfoSnapshot stateMetaInfoSnapshot = stateMetaInfoSnapshots.get(i);

				ColumnFamilyHandle columnFamilyHandle = columnFamilyHandles.get(i);

				// create a meta info for the state on restore;
				// this allows us to retain the state in future snapshots even if it wasn't accessed
				RegisteredStateMetaInfoBase stateMetaInfo =
					RegisteredStateMetaInfoBase.fromMetaInfoSnapshot(stateMetaInfoSnapshot);
				stateBackend.registerKvStateInformation(
					stateMetaInfoSnapshot.getName(),
					new Tuple2<>(columnFamilyHandle, stateMetaInfo));
			}

			// use the restore sst files as the base for succeeding checkpoints
				restoredSstFiles.put(
					restoreStateHandle.getCheckpointId(),
					restoreStateHandle.getSharedStateHandleIDs());

			lastCompletedCheckpointId = restoreStateHandle.getCheckpointId();
		}

		/**
		 * This recreates the new working directory of the recovered RocksDB instance and links/copies the contents from
		 * a local state.
		 */
		private void restoreInstanceDirectoryFromPath(Path source) throws IOException {

			FileSystem fileSystem = source.getFileSystem();

			final FileStatus[] fileStatuses = fileSystem.listStatus(source);

			if (fileStatuses == null) {
				throw new IOException("Cannot list file statues. Directory " + source + " does not exist.");
			}

			for (FileStatus fileStatus : fileStatuses) {
				final Path filePath = fileStatus.getPath();
				final String fileName = filePath.getName();
				File restoreFile = new File(source.getPath(), fileName);
				File targetFile = new File(stateBackend.instanceRocksDBPath.getPath(), fileName);
				if (fileName.endsWith(SST_FILE_SUFFIX)) {
					// hardlink'ing the immutable sst-files.
					Files.createLink(targetFile.toPath(), restoreFile.toPath());
				} else {
					// true copy for all other files.
					Files.copy(restoreFile.toPath(), targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
				}
			}
		}

		/**
		 * Reads Flink's state meta data file from the state handle.
		 */
		private List<StateMetaInfoSnapshot> readMetaData(
			StreamStateHandle metaStateHandle) throws Exception {

			FSDataInputStream inputStream = null;

			try {
				inputStream = metaStateHandle.openInputStream();
				stateBackend.cancelStreamRegistry.registerCloseable(inputStream);

				// isSerializerPresenceRequired flag is set to false, since for the RocksDB state backend,
				// deserialization of state happens lazily during runtime; we depend on the fact
				// that the new serializer for states could be compatible, and therefore the restore can continue
				// without old serializers required to be present.
				KeyedBackendSerializationProxy<T> serializationProxy =
					new KeyedBackendSerializationProxy<>(stateBackend.userCodeClassLoader);
				DataInputView in = new DataInputViewStreamWrapper(inputStream);
				serializationProxy.read(in);

				// check for key serializer compatibility; this also reconfigures the
				// key serializer to be compatible, if it is required and is possible
				if (!serializationProxy.getKeySerializerConfigSnapshot()
						.resolveSchemaCompatibility(stateBackend.keySerializer).isCompatibleAsIs()) {
					throw new StateMigrationException("The new key serializer must be compatible.");
				}

				return serializationProxy.getStateMetaInfoSnapshots();
			} finally {
				if (stateBackend.cancelStreamRegistry.unregisterCloseable(inputStream)) {
					inputStream.close();
				}
			}
		}

		private void transferAllStateDataToDirectory(
			IncrementalKeyedStateHandle restoreStateHandle,
			Path dest) throws IOException {

			final Map<StateHandleID, StreamStateHandle> sstFiles =
				restoreStateHandle.getSharedState();
			final Map<StateHandleID, StreamStateHandle> miscFiles =
				restoreStateHandle.getPrivateState();

			transferAllDataFromStateHandles(sstFiles, dest);
			transferAllDataFromStateHandles(miscFiles, dest);
		}

		/**
		 * Copies all the files from the given stream state handles to the given path, renaming the files w.r.t. their
		 * {@link StateHandleID}.
		 */
		private void transferAllDataFromStateHandles(
			Map<StateHandleID, StreamStateHandle> stateHandleMap,
			Path restoreInstancePath) throws IOException {

			for (Map.Entry<StateHandleID, StreamStateHandle> entry : stateHandleMap.entrySet()) {
				StateHandleID stateHandleID = entry.getKey();
				StreamStateHandle remoteFileHandle = entry.getValue();
				copyStateDataHandleData(new Path(restoreInstancePath, stateHandleID.toString()), remoteFileHandle);
			}

		}

		/**
		 * Copies the file from a single state handle to the given path.
		 */
		private void copyStateDataHandleData(
			Path restoreFilePath,
			StreamStateHandle remoteFileHandle) throws IOException {

			FileSystem restoreFileSystem = restoreFilePath.getFileSystem();

			FSDataInputStream inputStream = null;
			FSDataOutputStream outputStream = null;

			try {
				inputStream = remoteFileHandle.openInputStream();
				stateBackend.cancelStreamRegistry.registerCloseable(inputStream);

				outputStream = restoreFileSystem.create(restoreFilePath, FileSystem.WriteMode.OVERWRITE);
				stateBackend.cancelStreamRegistry.registerCloseable(outputStream);

				byte[] buffer = new byte[8 * 1024];
				while (true) {
					int numBytes = inputStream.read(buffer);
					if (numBytes == -1) {
						break;
					}

					outputStream.write(buffer, 0, numBytes);
				}
			} finally {
				if (stateBackend.cancelStreamRegistry.unregisterCloseable(inputStream)) {
					inputStream.close();
				}

				if (stateBackend.cancelStreamRegistry.unregisterCloseable(outputStream)) {
					outputStream.close();
				}
			}
		}
	}

	// ------------------------------------------------------------------------
	//  State factories
	// ------------------------------------------------------------------------

	/**
	 * Registers a k/v state information, which includes its state id, type, RocksDB column family handle, and serializers.
	 *
	 * <p>When restoring from a snapshot, we don’t restore the individual k/v states, just the global RocksDB database and
	 * the list of k/v state information. When a k/v state is first requested we check here whether we
	 * already have a registered entry for that and return it (after some necessary state compatibility checks)
	 * or create a new one if it does not exist.
	 */
	private <N, S extends State, SV> Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>> tryRegisterKvStateInformation(
			StateDescriptor<S, SV> stateDesc,
			TypeSerializer<N> namespaceSerializer,
			@Nullable StateSnapshotTransformer<SV> snapshotTransformer) throws Exception {

		Tuple2<ColumnFamilyHandle, RegisteredStateMetaInfoBase> oldStateInfo =
			kvStateInformation.get(stateDesc.getName());

		TypeSerializer<SV> stateSerializer = stateDesc.getSerializer();

		ColumnFamilyHandle newColumnFamily;
		RegisteredKeyValueStateBackendMetaInfo<N, SV> newMetaInfo;
		if (oldStateInfo != null) {
			@SuppressWarnings("unchecked")
			RegisteredKeyValueStateBackendMetaInfo<N, SV> castedMetaInfo = (RegisteredKeyValueStateBackendMetaInfo<N, SV>) oldStateInfo.f1;

			newMetaInfo = updateRestoredStateMetaInfo(
				Tuple2.of(oldStateInfo.f0, castedMetaInfo),
				stateDesc,
				namespaceSerializer,
				stateSerializer,
				snapshotTransformer);

			oldStateInfo.f1 = newMetaInfo;
			newColumnFamily = oldStateInfo.f0;
		} else {
			newMetaInfo = new RegisteredKeyValueStateBackendMetaInfo<>(
				stateDesc.getType(),
				stateDesc.getName(),
				namespaceSerializer,
				stateSerializer,
				snapshotTransformer);

			newColumnFamily = createColumnFamily(stateDesc.getName());
			registerKvStateInformation(stateDesc.getName(), Tuple2.of(newColumnFamily, newMetaInfo));
		}

		return Tuple2.of(newColumnFamily, newMetaInfo);
	}

	private <N, S extends State, SV> RegisteredKeyValueStateBackendMetaInfo<N, SV> updateRestoredStateMetaInfo(
			Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>> oldStateInfo,
			StateDescriptor<S, SV> stateDesc,
			TypeSerializer<N> namespaceSerializer,
			TypeSerializer<SV> stateSerializer,
			@Nullable StateSnapshotTransformer<SV> snapshotTransformer) throws Exception {

		@SuppressWarnings("unchecked")
		RegisteredKeyValueStateBackendMetaInfo<N, SV> restoredKvStateMetaInfo = oldStateInfo.f1;

		restoredKvStateMetaInfo.updateSnapshotTransformer(snapshotTransformer);

		TypeSerializerSchemaCompatibility<N> s = restoredKvStateMetaInfo.updateNamespaceSerializer(namespaceSerializer);
		if (!s.isCompatibleAsIs()) {
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
	private <N, S extends State, SV> void migrateStateValues(
		StateDescriptor<S, SV> stateDesc,
		Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>> stateMetaInfo) throws Exception {

		if (stateDesc.getType() == StateDescriptor.Type.MAP) {
			throw new StateMigrationException("The new serializer for a MapState requires state migration in order for the job to proceed." +
				" However, migration for MapState currently isn't supported.");
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
		AbstractRocksDBState<?, ?, SV, S> rocksDBState = (AbstractRocksDBState<?, ?, SV, S>) state;

		Snapshot rocksDBSnapshot = db.getSnapshot();
		try (
			RocksIteratorWrapper iterator = getRocksIterator(db, stateMetaInfo.f0);
			RocksDBWriteBatchWrapper batchWriter = new RocksDBWriteBatchWrapper(db, getWriteOptions())
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

	/**
	 * Creates a column family handle for use with a k/v state.
	 */
	private ColumnFamilyHandle createColumnFamily(String stateName) {
		byte[] nameBytes = stateName.getBytes(ConfigConstants.DEFAULT_CHARSET);
		Preconditions.checkState(!Arrays.equals(RocksDB.DEFAULT_COLUMN_FAMILY, nameBytes),
			"The chosen state name 'default' collides with the name of the default column family!");

		ColumnFamilyDescriptor columnDescriptor = new ColumnFamilyDescriptor(nameBytes, columnOptions);

		try {
			return db.createColumnFamily(columnDescriptor);
		} catch (RocksDBException e) {
			throw new FlinkRuntimeException("Error creating ColumnFamilyHandle.", e);
		}
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
			stateDesc, namespaceSerializer, getStateSnapshotTransformer(stateDesc, snapshotTransformFactory));
		return stateFactory.createState(stateDesc, registerResult, RocksDBKeyedStateBackend.this);
	}

	@SuppressWarnings("unchecked")
	private <SV, SEV> StateSnapshotTransformer<SV> getStateSnapshotTransformer(
		StateDescriptor<?, SV> stateDesc,
		StateSnapshotTransformFactory<SEV> snapshotTransformFactory) {
		if (stateDesc instanceof ListStateDescriptor) {
			Optional<StateSnapshotTransformer<SEV>> original = snapshotTransformFactory.createForDeserializedState();
			return original.map(est -> createRocksDBListStateTransformer(stateDesc, est)).orElse(null);
		} else if (stateDesc instanceof MapStateDescriptor) {
			Optional<StateSnapshotTransformer<byte[]>> original = snapshotTransformFactory.createForSerializedState();
			return (StateSnapshotTransformer<SV>) original
				.map(RocksDBMapState.StateSnapshotTransformerWrapper::new).orElse(null);
		} else {
			Optional<StateSnapshotTransformer<byte[]>> original = snapshotTransformFactory.createForSerializedState();
			return (StateSnapshotTransformer<SV>) original.orElse(null);
		}
	}

	@SuppressWarnings("unchecked")
	private <SV, SEV> StateSnapshotTransformer<SV> createRocksDBListStateTransformer(
		StateDescriptor<?, SV> stateDesc,
		StateSnapshotTransformer<SEV> elementTransformer) {
		return (StateSnapshotTransformer<SV>) new RocksDBListState.StateSnapshotTransformerWrapper<>(
			elementTransformer, ((ListStateDescriptor<SEV>) stateDesc).getElementSerializer());
	}

	/**
	 * Only visible for testing, DO NOT USE.
	 */
	public File getInstanceBasePath() {
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

		for (Tuple2<ColumnFamilyHandle, RegisteredStateMetaInfoBase> column : kvStateInformation.values()) {
			//TODO maybe filterOrTransform only for k/v states
			try (RocksIteratorWrapper rocksIterator = getRocksIterator(db, column.f0)) {
				rocksIterator.seekToFirst();

				while (rocksIterator.isValid()) {
					count++;
					rocksIterator.next();
				}
			}
		}

		return count;
	}

	public static RocksIteratorWrapper getRocksIterator(RocksDB db) {
		return new RocksIteratorWrapper(db.newIterator());
	}

	public static RocksIteratorWrapper getRocksIterator(
		RocksDB db,
		ColumnFamilyHandle columnFamilyHandle) {
		return new RocksIteratorWrapper(db.newIterator(columnFamilyHandle));
	}

	/**
	 * Encapsulates the logic and resources in connection with creating priority queue state structures.
	 */
	class RocksDBPriorityQueueSetFactory implements PriorityQueueSetFactory {

		/** Default cache size per key-group. */
		private static final int DEFAULT_CACHES_SIZE = 128; //TODO make this configurable

		/** A shared buffer to serialize elements for the priority queue. */
		@Nonnull
		private final DataOutputSerializer sharedElementOutView;

		/** A shared buffer to de-serialize elements for the priority queue. */
		@Nonnull
		private final DataInputDeserializer sharedElementInView;

		RocksDBPriorityQueueSetFactory() {
			this.sharedElementOutView = new DataOutputSerializer(128);
			this.sharedElementInView = new DataInputDeserializer();
		}

		@Nonnull
		@Override
		public <T extends HeapPriorityQueueElement & PriorityComparable & Keyed> KeyGroupedInternalPriorityQueue<T>
		create(@Nonnull String stateName, @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {

			final Tuple2<ColumnFamilyHandle, RegisteredStateMetaInfoBase> metaInfoTuple =
				tryRegisterPriorityQueueMetaInfo(stateName, byteOrderedElementSerializer);

			final ColumnFamilyHandle columnFamilyHandle = metaInfoTuple.f0;

			return new KeyGroupPartitionedPriorityQueue<>(
				KeyExtractorFunction.forKeyedObjects(),
				PriorityComparator.forPriorityComparableObjects(),
				new KeyGroupPartitionedPriorityQueue.PartitionQueueSetFactory<T, RocksDBCachingPriorityQueueSet<T>>() {
					@Nonnull
					@Override
					public RocksDBCachingPriorityQueueSet<T> create(
						int keyGroupId,
						int numKeyGroups,
						@Nonnull KeyExtractorFunction<T> keyExtractor,
						@Nonnull PriorityComparator<T> elementPriorityComparator) {
						TreeOrderedSetCache orderedSetCache = new TreeOrderedSetCache(DEFAULT_CACHES_SIZE);
						return new RocksDBCachingPriorityQueueSet<>(
							keyGroupId,
							keyGroupPrefixBytes,
							db,
							columnFamilyHandle,
							byteOrderedElementSerializer,
							sharedElementOutView,
							sharedElementInView,
							writeBatchWrapper,
							orderedSetCache
						);
					}
				},
				keyGroupRange,
				numberOfKeyGroups);
		}
	}

	@Nonnull
	private <T> Tuple2<ColumnFamilyHandle, RegisteredStateMetaInfoBase> tryRegisterPriorityQueueMetaInfo(
		@Nonnull String stateName,
		@Nonnull TypeSerializer<T> byteOrderedElementSerializer) {

		Tuple2<ColumnFamilyHandle, RegisteredStateMetaInfoBase> metaInfoTuple =
			kvStateInformation.get(stateName);

		if (metaInfoTuple == null) {
			final ColumnFamilyHandle columnFamilyHandle = createColumnFamily(stateName);

			RegisteredPriorityQueueStateBackendMetaInfo<T> metaInfo =
				new RegisteredPriorityQueueStateBackendMetaInfo<>(stateName, byteOrderedElementSerializer);

			metaInfoTuple = new Tuple2<>(columnFamilyHandle, metaInfo);
			registerKvStateInformation(stateName, metaInfoTuple);
		} else {
			// TODO we implement the simple way of supporting the current functionality, mimicking keyed state
			// because this should be reworked in FLINK-9376 and then we should have a common algorithm over
			// StateMetaInfoSnapshot that avoids this code duplication.

			@SuppressWarnings("unchecked")
			RegisteredPriorityQueueStateBackendMetaInfo<T> castedMetaInfo =
				(RegisteredPriorityQueueStateBackendMetaInfo<T>) metaInfoTuple.f1;

			TypeSerializer<T> previousElementSerializer = castedMetaInfo.getPreviousElementSerializer();

			if (previousElementSerializer != byteOrderedElementSerializer) {
				TypeSerializerSchemaCompatibility<T> compatibilityResult =
					castedMetaInfo.updateElementSerializer(byteOrderedElementSerializer);

				// Since priority queue elements are written into RocksDB
				// as keys prefixed with the key group and namespace, we do not support
				// migrating them. Therefore, here we only check for incompatibility.
				if (compatibilityResult.isIncompatible()) {
					throw new FlinkRuntimeException(
						new StateMigrationException("The new priority queue serializer must not be incompatible."));
				}

				// update meta info with new serializer
				metaInfoTuple.f1 =
					new RegisteredPriorityQueueStateBackendMetaInfo<>(stateName, byteOrderedElementSerializer);
			}
		}

		return metaInfoTuple;
	}

	@Override
	public boolean requiresLegacySynchronousTimerSnapshots() {
		return priorityQueueFactory instanceof HeapPriorityQueueSetFactory;
	}
}
