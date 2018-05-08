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
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.CompatibilityUtil;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.io.async.AbstractAsyncCallableWithResources;
import org.apache.flink.runtime.io.async.AsyncStoppableTaskWithCallback;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.DirectoryStateHandle;
import org.apache.flink.runtime.state.DoneFuture;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.LocalRecoveryDirectoryProvider;
import org.apache.flink.runtime.state.PlaceholderStreamStateHandle;
import org.apache.flink.runtime.state.RegisteredKeyedBackendStateMetaInfo;
import org.apache.flink.runtime.state.SnappyStreamCompressionDecorator;
import org.apache.flink.runtime.state.SnapshotDirectory;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategy;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.internal.InternalFoldingState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalReducingState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ResourceGuard;
import org.apache.flink.util.StateMigrationException;
import org.apache.flink.util.function.SupplierWithException;

import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedMap;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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

	/** File suffix of sstable files. */
	private static final String SST_FILE_SUFFIX = ".sst";

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
	 * Information about the k/v states as we create them. This is used to retrieve the
	 * column family that is used for a state and also for sanity checks when restoring.
	 */
	private final Map<String, Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<?, ?>>> kvStateInformation;

	/**
	 * Map of state names to their corresponding restored state meta info.
	 *
	 * <p>TODO this map can be removed when eager-state registration is in place.
	 * TODO we currently need this cached to check state migration strategies when new serializers are registered.
	 */
	private final Map<String, RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> restoredKvStateMetaInfos;

	/** Number of bytes required to prefix the key groups. */
	private final int keyGroupPrefixBytes;

	/** True if incremental checkpointing is enabled. */
	private final boolean enableIncrementalCheckpointing;

	/** The state handle ids of all sst files materialized in snapshots for previous checkpoints. */
	private final SortedMap<Long, Set<StateHandleID>> materializedSstFiles;

	/** The identifier of the last completed checkpoint. */
	private long lastCompletedCheckpointId = -1L;

	/** Unique ID of this backend. */
	private UUID backendUID;

	/** The configuration of local recovery. */
	private final LocalRecoveryConfig localRecoveryConfig;

	/** The snapshot strategy, e.g., if we use full or incremental checkpoints, local state, and so on. */
	private final SnapshotStrategy<SnapshotResult<KeyedStateHandle>> snapshotStrategy;

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
		LocalRecoveryConfig localRecoveryConfig
	) throws IOException {

		super(kvStateRegistry, keySerializer, userCodeClassLoader, numberOfKeyGroups, keyGroupRange, executionConfig);

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
		this.keyGroupPrefixBytes = getNumberOfKeyGroups() > (Byte.MAX_VALUE + 1) ? 2 : 1;
		this.kvStateInformation = new LinkedHashMap<>();
		this.restoredKvStateMetaInfos = new HashMap<>();
		this.materializedSstFiles = new TreeMap<>();
		this.backendUID = UUID.randomUUID();

		this.snapshotStrategy = enableIncrementalCheckpointing ?
			new IncrementalSnapshotStrategy() :
			new FullSnapshotStrategy();

		this.writeOptions = new WriteOptions().setDisableWAL(true);

		LOG.debug("Setting initial keyed backend uid for operator {} to {}.", this.operatorIdentifier, this.backendUID);
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

	@Override
	public <N> Stream<K> getKeys(String state, N namespace) {
		Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<?, ?>> columnInfo = kvStateInformation.get(state);
		if (columnInfo == null) {
			return Stream.empty();
		}

		final TypeSerializer<N> namespaceSerializer = (TypeSerializer<N>) columnInfo.f1.getNamespaceSerializer();
		final ByteArrayOutputStreamWithPos namespaceOutputStream = new ByteArrayOutputStreamWithPos(8);
		boolean ambiguousKeyPossible = RocksDBKeySerializationUtils.isAmbiguousKeyPossible(keySerializer, namespaceSerializer);
		final byte[] nameSpaceBytes;
		try {
			RocksDBKeySerializationUtils.writeNameSpace(
				namespace,
				namespaceSerializer,
				namespaceOutputStream,
				new DataOutputViewStreamWrapper(namespaceOutputStream),
				ambiguousKeyPossible);
			nameSpaceBytes = namespaceOutputStream.toByteArray();
		} catch (IOException ex) {
			throw new FlinkRuntimeException("Failed to get keys from RocksDB state backend.", ex);
		}

		RocksIterator iterator = db.newIterator(columnInfo.f0);
		iterator.seekToFirst();

		final RocksIteratorForKeysWrapper<K> iteratorWrapper = new RocksIteratorForKeysWrapper<>(iterator, state, keySerializer, keyGroupPrefixBytes,
			ambiguousKeyPossible, nameSpaceBytes);

		Stream<K> targetStream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(iteratorWrapper, Spliterator.ORDERED), false);
		return targetStream.onClose(iteratorWrapper::close);
	}

	@VisibleForTesting
	ColumnFamilyHandle getColumnFamilyHandle(String state) {
		Tuple2<ColumnFamilyHandle, ?> columnInfo = kvStateInformation.get(state);
		return columnInfo != null ? columnInfo.f0 : null;
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

			// RocksDB's native memory management requires that *all* CFs (including default) are closed before the
			// DB is closed. See:
			// https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#opening-a-database-with-column-families
			// Start with default CF ...
			IOUtils.closeQuietly(defaultColumnFamily);

			// ... continue with the ones created by Flink...
			for (Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<?, ?>> columnMetaData :
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
			restoredKvStateMetaInfos.clear();

			cleanInstanceBasePath();
		}
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
	@Override
	public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
		final long checkpointId,
		final long timestamp,
		final CheckpointStreamFactory streamFactory,
		CheckpointOptions checkpointOptions) throws Exception {

		return snapshotStrategy.performSnapshot(checkpointId, timestamp, streamFactory, checkpointOptions);
	}

	@Override
	public void restore(Collection<KeyedStateHandle> restoreState) throws Exception {
		LOG.info("Initializing RocksDB keyed state backend.");

		if (LOG.isDebugEnabled()) {
			LOG.debug("Restoring snapshot from state handles: {}.", restoreState);
		}

		// clear all meta data
		kvStateInformation.clear();
		restoredKvStateMetaInfos.clear();

		try {
			if (restoreState == null || restoreState.isEmpty()) {
				createDB();
			} else {
				KeyedStateHandle firstStateHandle = restoreState.iterator().next();
				if (firstStateHandle instanceof IncrementalKeyedStateHandle
					|| firstStateHandle instanceof IncrementalLocalKeyedStateHandle) {
					RocksDBIncrementalRestoreOperation<K> restoreOperation = new RocksDBIncrementalRestoreOperation<>(this);
					restoreOperation.restore(restoreState);
				} else {
					RocksDBFullRestoreOperation<K> restoreOperation = new RocksDBFullRestoreOperation<>(this);
					restoreOperation.doRestore(restoreState);
				}
			}
		} catch (Exception ex) {
			dispose();
			throw ex;
		}
	}

	@Override
	public void notifyCheckpointComplete(long completedCheckpointId) {

		if (!enableIncrementalCheckpointing) {
			return;
		}

		synchronized (materializedSstFiles) {

			if (completedCheckpointId < lastCompletedCheckpointId) {
				return;
			}

			materializedSstFiles.keySet().removeIf(checkpointId -> checkpointId < completedCheckpointId);

			lastCompletedCheckpointId = completedCheckpointId;
		}
	}

	private void createDB() throws IOException {
		List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(1);
		this.db = openDB(instanceRocksDBPath.getAbsolutePath(), Collections.emptyList(), columnFamilyHandles);
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
		 *
		 * @throws IOException
		 * @throws ClassNotFoundException
		 * @throws RocksDBException
		 */
		private void restoreKVStateMetaData() throws IOException, StateMigrationException, RocksDBException {

			// isSerializerPresenceRequired flag is set to false, since for the RocksDB state backend,
			// deserialization of state happens lazily during runtime; we depend on the fact
			// that the new serializer for states could be compatible, and therefore the restore can continue
			// without old serializers required to be present.
			KeyedBackendSerializationProxy<K> serializationProxy =
				new KeyedBackendSerializationProxy<>(rocksDBKeyedStateBackend.userCodeClassLoader, false);

			serializationProxy.read(currentStateHandleInView);

			// check for key serializer compatibility; this also reconfigures the
			// key serializer to be compatible, if it is required and is possible
			if (CompatibilityUtil.resolveCompatibilityResult(
				serializationProxy.getKeySerializer(),
				UnloadableDummyTypeSerializer.class,
				serializationProxy.getKeySerializerConfigSnapshot(),
				rocksDBKeyedStateBackend.keySerializer)
				.isRequiresMigration()) {

				// TODO replace with state migration; note that key hash codes need to remain the same after migration
				throw new StateMigrationException("The new key serializer is not compatible to read previous keys. " +
					"Aborting now since state migration is currently not available");
			}

			this.keygroupStreamCompressionDecorator = serializationProxy.isUsingKeyGroupCompression() ?
				SnappyStreamCompressionDecorator.INSTANCE : UncompressedStreamCompressionDecorator.INSTANCE;

			List<RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> restoredMetaInfos =
				serializationProxy.getStateMetaInfoSnapshots();
			currentStateHandleKVStateColumnFamilies = new ArrayList<>(restoredMetaInfos.size());

			for (RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?> restoredMetaInfo : restoredMetaInfos) {

				Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<?, ?>> registeredColumn =
					rocksDBKeyedStateBackend.kvStateInformation.get(restoredMetaInfo.getName());

				if (registeredColumn == null) {
					byte[] nameBytes = restoredMetaInfo.getName().getBytes(ConfigConstants.DEFAULT_CHARSET);

					ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(
						nameBytes,
						rocksDBKeyedStateBackend.columnOptions);

					RegisteredKeyedBackendStateMetaInfo<?, ?> stateMetaInfo =
						new RegisteredKeyedBackendStateMetaInfo<>(
							restoredMetaInfo.getStateType(),
							restoredMetaInfo.getName(),
							restoredMetaInfo.getNamespaceSerializer(),
							restoredMetaInfo.getStateSerializer());

					rocksDBKeyedStateBackend.restoredKvStateMetaInfos.put(restoredMetaInfo.getName(), restoredMetaInfo);

					ColumnFamilyHandle columnFamily = rocksDBKeyedStateBackend.db.createColumnFamily(columnFamilyDescriptor);

					registeredColumn = new Tuple2<>(columnFamily, stateMetaInfo);
					rocksDBKeyedStateBackend.kvStateInformation.put(stateMetaInfo.getName(), registeredColumn);

				} else {
					// TODO with eager state registration in place, check here for serializer migration strategies
				}
				currentStateHandleKVStateColumnFamilies.add(registeredColumn.f0);
			}
		}

		/**
		 * Restore the KV-state / ColumnFamily data for all key-groups referenced by the current state handle.
		 *
		 * @throws IOException
		 * @throws RocksDBException
		 */
		private void restoreKVStateData() throws IOException, RocksDBException {
			//for all key-groups in the current state handle...
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
							if (RocksDBFullSnapshotOperation.hasMetaDataFollowsFlag(key)) {
								//clear the signal bit in the key to make it ready for insertion again
								RocksDBFullSnapshotOperation.clearMetaDataFollowsFlag(key);
								rocksDBKeyedStateBackend.db.put(handle, key, value);
								//TODO this could be aware of keyGroupPrefixBytes and write only one byte if possible
								kvStateId = RocksDBFullSnapshotOperation.END_OF_KEY_GROUP_MARK
									& compressedKgInputView.readShort();
								if (RocksDBFullSnapshotOperation.END_OF_KEY_GROUP_MARK == kvStateId) {
									keyGroupHasMoreKeys = false;
								} else {
									handle = currentStateHandleKVStateColumnFamilies.get(kvStateId);
								}
							} else {
								rocksDBKeyedStateBackend.db.put(handle, key, value);
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

		private RocksDBIncrementalRestoreOperation(RocksDBKeyedStateBackend<T> stateBackend) {
			this.stateBackend = stateBackend;
		}

		/**
		 * Root method that branches for different implementations of {@link KeyedStateHandle}.
		 */
		void restore(Collection<KeyedStateHandle> restoreStateHandles) throws Exception {

			boolean hasExtraKeys = (restoreStateHandles.size() > 1 ||
				!Objects.equals(restoreStateHandles.iterator().next().getKeyGroupRange(), stateBackend.keyGroupRange));

			if (hasExtraKeys) {
				stateBackend.createDB();
			}

			for (KeyedStateHandle rawStateHandle : restoreStateHandles) {

				if (rawStateHandle instanceof IncrementalKeyedStateHandle) {
					restoreInstance((IncrementalKeyedStateHandle) rawStateHandle, hasExtraKeys);
				} else if (rawStateHandle instanceof IncrementalLocalKeyedStateHandle) {
					Preconditions.checkState(!hasExtraKeys, "Cannot recover from local state after rescaling.");
					restoreInstance((IncrementalLocalKeyedStateHandle) rawStateHandle);
				} else {
					throw new IllegalStateException("Unexpected state handle type, " +
						"expected " + IncrementalKeyedStateHandle.class +
						", but found " + rawStateHandle.getClass());
				}
			}
		}

		/**
		 * Recovery from remote incremental state.
		 */
		private void restoreInstance(
			IncrementalKeyedStateHandle restoreStateHandle,
			boolean hasExtraKeys) throws Exception {

			// read state data
			Path temporaryRestoreInstancePath = new Path(
				stateBackend.instanceBasePath.getAbsolutePath(),
				UUID.randomUUID().toString());

			try {

				transferAllStateDataToDirectory(restoreStateHandle, temporaryRestoreInstancePath);

				// read meta data
				List<RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> stateMetaInfoSnapshots =
					readMetaData(restoreStateHandle.getMetaStateHandle());

				List<ColumnFamilyDescriptor> columnFamilyDescriptors =
					createAndRegisterColumnFamilyDescriptors(stateMetaInfoSnapshots);

				if (hasExtraKeys) {
					restoreKeyGroupsShardWithTemporaryHelperInstance(
						temporaryRestoreInstancePath,
						columnFamilyDescriptors,
						stateMetaInfoSnapshots);
				} else {

					// since we transferred all remote state to a local directory, we can use the same code as for
					// local recovery.
					IncrementalLocalKeyedStateHandle localKeyedStateHandle = new IncrementalLocalKeyedStateHandle(
						restoreStateHandle.getBackendIdentifier(),
						restoreStateHandle.getCheckpointId(),
						new DirectoryStateHandle(temporaryRestoreInstancePath),
						restoreStateHandle.getKeyGroupRange(),
						restoreStateHandle.getMetaStateHandle(),
						restoreStateHandle.getSharedState().keySet());

					restoreLocalStateIntoFullInstance(
						localKeyedStateHandle,
						columnFamilyDescriptors,
						stateMetaInfoSnapshots);
				}
			} finally {
				FileSystem restoreFileSystem = temporaryRestoreInstancePath.getFileSystem();
				if (restoreFileSystem.exists(temporaryRestoreInstancePath)) {
					restoreFileSystem.delete(temporaryRestoreInstancePath, true);
				}
			}
		}

		/**
		 * Recovery from local incremental state.
		 */
		private void restoreInstance(IncrementalLocalKeyedStateHandle localKeyedStateHandle) throws Exception {
			// read meta data
			List<RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> stateMetaInfoSnapshots =
				readMetaData(localKeyedStateHandle.getMetaDataState());

			List<ColumnFamilyDescriptor> columnFamilyDescriptors =
				createAndRegisterColumnFamilyDescriptors(stateMetaInfoSnapshots);

			restoreLocalStateIntoFullInstance(
				localKeyedStateHandle,
				columnFamilyDescriptors,
				stateMetaInfoSnapshots);
		}

		/**
		 * This method recreates and registers all {@link ColumnFamilyDescriptor} from Flink's state meta data snapshot.
		 */
		private List<ColumnFamilyDescriptor> createAndRegisterColumnFamilyDescriptors(
			List<RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> stateMetaInfoSnapshots) {

			List<ColumnFamilyDescriptor> columnFamilyDescriptors =
				new ArrayList<>(1 + stateMetaInfoSnapshots.size());

			for (RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?> stateMetaInfoSnapshot : stateMetaInfoSnapshots) {

				ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(
					stateMetaInfoSnapshot.getName().getBytes(ConfigConstants.DEFAULT_CHARSET),
					stateBackend.columnOptions);

				columnFamilyDescriptors.add(columnFamilyDescriptor);
				stateBackend.restoredKvStateMetaInfos.put(stateMetaInfoSnapshot.getName(), stateMetaInfoSnapshot);
			}
			return columnFamilyDescriptors;
		}

		/**
		 * This method implements the core of the restore logic that unifies how local and remote state are recovered.
		 */
		private void restoreLocalStateIntoFullInstance(
			IncrementalLocalKeyedStateHandle restoreStateHandle,
			List<ColumnFamilyDescriptor> columnFamilyDescriptors,
			List<RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> stateMetaInfoSnapshots) throws Exception {
			// pick up again the old backend id, so the we can reference existing state
			stateBackend.backendUID = restoreStateHandle.getBackendIdentifier();

			LOG.debug("Restoring keyed backend uid in operator {} from incremental snapshot to {}.",
				stateBackend.operatorIdentifier, stateBackend.backendUID);

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

			for (int i = 0; i < columnFamilyDescriptors.size(); ++i) {
				RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?> stateMetaInfoSnapshot = stateMetaInfoSnapshots.get(i);

				ColumnFamilyHandle columnFamilyHandle = columnFamilyHandles.get(i);
				RegisteredKeyedBackendStateMetaInfo<?, ?> stateMetaInfo =
					new RegisteredKeyedBackendStateMetaInfo<>(
						stateMetaInfoSnapshot.getStateType(),
						stateMetaInfoSnapshot.getName(),
						stateMetaInfoSnapshot.getNamespaceSerializer(),
						stateMetaInfoSnapshot.getStateSerializer());

				stateBackend.kvStateInformation.put(
					stateMetaInfoSnapshot.getName(),
					new Tuple2<>(columnFamilyHandle, stateMetaInfo));
			}

			// use the restore sst files as the base for succeeding checkpoints
			synchronized (stateBackend.materializedSstFiles) {
				stateBackend.materializedSstFiles.put(
					restoreStateHandle.getCheckpointId(),
					restoreStateHandle.getSharedStateHandleIDs());
			}

			stateBackend.lastCompletedCheckpointId = restoreStateHandle.getCheckpointId();
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
		private List<RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> readMetaData(
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
					new KeyedBackendSerializationProxy<>(stateBackend.userCodeClassLoader, false);
				DataInputView in = new DataInputViewStreamWrapper(inputStream);
				serializationProxy.read(in);

				// check for key serializer compatibility; this also reconfigures the
				// key serializer to be compatible, if it is required and is possible
				if (CompatibilityUtil.resolveCompatibilityResult(
					serializationProxy.getKeySerializer(),
					UnloadableDummyTypeSerializer.class,
					serializationProxy.getKeySerializerConfigSnapshot(),
					stateBackend.keySerializer)
					.isRequiresMigration()) {

					// TODO replace with state migration; note that key hash codes need to remain the same after migration
					throw new StateMigrationException("The new key serializer is not compatible to read previous keys. " +
						"Aborting now since state migration is currently not available");
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

		/**
		 * In case of rescaling, this method creates a temporary RocksDB instance for a key-groups shard. All contents
		 * from the temporary instance are copied into the real restore instance and then the temporary instance is
		 * discarded.
		 */
		private void restoreKeyGroupsShardWithTemporaryHelperInstance(
			Path restoreInstancePath,
			List<ColumnFamilyDescriptor> columnFamilyDescriptors,
			List<RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> stateMetaInfoSnapshots) throws Exception {

			List<ColumnFamilyHandle> columnFamilyHandles =
				new ArrayList<>(1 + columnFamilyDescriptors.size());

			try (RocksDB restoreDb = stateBackend.openDB(
				restoreInstancePath.getPath(),
				columnFamilyDescriptors,
				columnFamilyHandles)) {

				final ColumnFamilyHandle defaultColumnFamily = columnFamilyHandles.remove(0);

				Preconditions.checkState(columnFamilyHandles.size() == columnFamilyDescriptors.size());

				try {
					for (int i = 0; i < columnFamilyDescriptors.size(); ++i) {
						ColumnFamilyHandle columnFamilyHandle = columnFamilyHandles.get(i);
						ColumnFamilyDescriptor columnFamilyDescriptor = columnFamilyDescriptors.get(i);
						RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?> stateMetaInfoSnapshot = stateMetaInfoSnapshots.get(i);

						Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<?, ?>> registeredStateMetaInfoEntry =
							stateBackend.kvStateInformation.get(stateMetaInfoSnapshot.getName());

						if (null == registeredStateMetaInfoEntry) {

							RegisteredKeyedBackendStateMetaInfo<?, ?> stateMetaInfo =
								new RegisteredKeyedBackendStateMetaInfo<>(
									stateMetaInfoSnapshot.getStateType(),
									stateMetaInfoSnapshot.getName(),
									stateMetaInfoSnapshot.getNamespaceSerializer(),
									stateMetaInfoSnapshot.getStateSerializer());

							registeredStateMetaInfoEntry =
								new Tuple2<>(
									stateBackend.db.createColumnFamily(columnFamilyDescriptor),
									stateMetaInfo);

							stateBackend.kvStateInformation.put(
								stateMetaInfoSnapshot.getName(),
								registeredStateMetaInfoEntry);
						}

						ColumnFamilyHandle targetColumnFamilyHandle = registeredStateMetaInfoEntry.f0;

						try (RocksIterator iterator = restoreDb.newIterator(columnFamilyHandle)) {

							int startKeyGroup = stateBackend.getKeyGroupRange().getStartKeyGroup();
							byte[] startKeyGroupPrefixBytes = new byte[stateBackend.keyGroupPrefixBytes];
							for (int j = 0; j < stateBackend.keyGroupPrefixBytes; ++j) {
								startKeyGroupPrefixBytes[j] = (byte) (startKeyGroup >>> ((stateBackend.keyGroupPrefixBytes - j - 1) * Byte.SIZE));
							}

							iterator.seek(startKeyGroupPrefixBytes);

							while (iterator.isValid()) {

								int keyGroup = 0;
								for (int j = 0; j < stateBackend.keyGroupPrefixBytes; ++j) {
									keyGroup = (keyGroup << Byte.SIZE) + iterator.key()[j];
								}

								if (stateBackend.keyGroupRange.contains(keyGroup)) {
									stateBackend.db.put(targetColumnFamilyHandle,
										iterator.key(), iterator.value());
								}

								iterator.next();
							}
						} // releases native iterator resources
					}
				} finally {

					//release native tmp db column family resources
					IOUtils.closeQuietly(defaultColumnFamily);

					for (ColumnFamilyHandle flinkColumnFamilyHandle : columnFamilyHandles) {
						IOUtils.closeQuietly(flinkColumnFamilyHandle);
					}
				}
			} // releases native tmp db resources
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
	private <N, S> Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<N, S>> tryRegisterKvStateInformation(
			StateDescriptor<?, S> stateDesc,
			TypeSerializer<N> namespaceSerializer) throws StateMigrationException, IOException {

		Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<?, ?>> stateInfo =
			kvStateInformation.get(stateDesc.getName());

		RegisteredKeyedBackendStateMetaInfo<N, S> newMetaInfo;
		if (stateInfo != null) {

			@SuppressWarnings("unchecked")
			RegisteredKeyedBackendStateMetaInfo.Snapshot<N, S> restoredMetaInfoSnapshot =
				(RegisteredKeyedBackendStateMetaInfo.Snapshot<N, S>) restoredKvStateMetaInfos.get(stateDesc.getName());

			Preconditions.checkState(
				restoredMetaInfoSnapshot != null,
				"Requested to check compatibility of a restored RegisteredKeyedBackendStateMetaInfo," +
					" but its corresponding restored snapshot cannot be found.");

			newMetaInfo = RegisteredKeyedBackendStateMetaInfo.resolveKvStateCompatibility(
				restoredMetaInfoSnapshot,
				namespaceSerializer,
				stateDesc);

			stateInfo.f1 = newMetaInfo;
		} else {
			String stateName = stateDesc.getName();

			newMetaInfo = new RegisteredKeyedBackendStateMetaInfo<>(
				stateDesc.getType(),
				stateName,
				namespaceSerializer,
				stateDesc.getSerializer());

			ColumnFamilyHandle columnFamily = createColumnFamily(stateName);

			stateInfo = Tuple2.of(columnFamily, newMetaInfo);
			kvStateInformation.put(stateDesc.getName(), stateInfo);
		}

		return Tuple2.of(stateInfo.f0, newMetaInfo);
	}

	/**
	 * Creates a column family handle for use with a k/v state.
	 */
	private ColumnFamilyHandle createColumnFamily(String stateName) throws IOException {
		byte[] nameBytes = stateName.getBytes(ConfigConstants.DEFAULT_CHARSET);
		Preconditions.checkState(!Arrays.equals(RocksDB.DEFAULT_COLUMN_FAMILY, nameBytes),
			"The chosen state name 'default' collides with the name of the default column family!");

		ColumnFamilyDescriptor columnDescriptor = new ColumnFamilyDescriptor(nameBytes, columnOptions);

		try {
			return db.createColumnFamily(columnDescriptor);
		} catch (RocksDBException e) {
			throw new IOException("Error creating ColumnFamilyHandle.", e);
		}
	}

	@Override
	protected <N, T> InternalValueState<K, N, T> createValueState(
		TypeSerializer<N> namespaceSerializer,
		ValueStateDescriptor<T> stateDesc) throws Exception {

		Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<N, T>> registerResult =
				tryRegisterKvStateInformation(stateDesc, namespaceSerializer);

		return new RocksDBValueState<>(
				registerResult.f0,
				registerResult.f1.getNamespaceSerializer(),
				registerResult.f1.getStateSerializer(),
				stateDesc.getDefaultValue(),
				this);
	}

	@Override
	protected <N, T> InternalListState<K, N, T> createListState(
		TypeSerializer<N> namespaceSerializer,
		ListStateDescriptor<T> stateDesc) throws Exception {

		Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<N, List<T>>> registerResult =
				tryRegisterKvStateInformation(stateDesc, namespaceSerializer);

		return new RocksDBListState<>(
				registerResult.f0,
				registerResult.f1.getNamespaceSerializer(),
				registerResult.f1.getStateSerializer(),
				stateDesc.getDefaultValue(),
				stateDesc.getElementSerializer(),
				this);
	}

	@Override
	protected <N, T> InternalReducingState<K, N, T> createReducingState(
		TypeSerializer<N> namespaceSerializer,
		ReducingStateDescriptor<T> stateDesc) throws Exception {

		Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<N, T>> registerResult =
				tryRegisterKvStateInformation(stateDesc, namespaceSerializer);

		return new RocksDBReducingState<>(
				registerResult.f0,
				registerResult.f1.getNamespaceSerializer(),
				registerResult.f1.getStateSerializer(),
				stateDesc.getDefaultValue(),
				stateDesc.getReduceFunction(),
				this);
	}

	@Override
	protected <N, T, ACC, R> InternalAggregatingState<K, N, T, ACC, R> createAggregatingState(
		TypeSerializer<N> namespaceSerializer,
		AggregatingStateDescriptor<T, ACC, R> stateDesc) throws Exception {

		Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<N, ACC>> registerResult =
				tryRegisterKvStateInformation(stateDesc, namespaceSerializer);

		return new RocksDBAggregatingState<>(
				registerResult.f0,
				registerResult.f1.getNamespaceSerializer(),
				registerResult.f1.getStateSerializer(),
				stateDesc.getDefaultValue(),
				stateDesc.getAggregateFunction(),
				this);
	}

	@Override
	protected <N, T, ACC> InternalFoldingState<K, N, T, ACC> createFoldingState(
		TypeSerializer<N> namespaceSerializer,
		FoldingStateDescriptor<T, ACC> stateDesc) throws Exception {

		Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<N, ACC>> registerResult =
				tryRegisterKvStateInformation(stateDesc, namespaceSerializer);

		return new RocksDBFoldingState<>(
				registerResult.f0,
				registerResult.f1.getNamespaceSerializer(),
				registerResult.f1.getStateSerializer(),
				stateDesc.getDefaultValue(),
				stateDesc.getFoldFunction(),
				this);
	}

	@Override
	protected <N, UK, UV> InternalMapState<K, N, UK, UV> createMapState(
		TypeSerializer<N> namespaceSerializer,
		MapStateDescriptor<UK, UV> stateDesc) throws Exception {

		Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<N, Map<UK, UV>>> registerResult =
				tryRegisterKvStateInformation(stateDesc, namespaceSerializer);

		return new RocksDBMapState<>(
				registerResult.f0,
				registerResult.f1.getNamespaceSerializer(),
				registerResult.f1.getStateSerializer(),
				stateDesc.getDefaultValue(),
				this);
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
	public int numStateEntries() {
		int count = 0;

		for (Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<?, ?>> column : kvStateInformation.values()) {
			try (RocksIterator rocksIterator = db.newIterator(column.f0)) {
				rocksIterator.seekToFirst();

				while (rocksIterator.isValid()) {
					count++;
					rocksIterator.next();
				}
			}
		}

		return count;
	}



	/**
	 * Iterator that merges multiple RocksDB iterators to partition all states into contiguous key-groups.
	 * The resulting iteration sequence is ordered by (key-group, kv-state).
	 */
	@VisibleForTesting
	static final class RocksDBMergeIterator implements AutoCloseable {

		private final PriorityQueue<MergeIterator> heap;
		private final int keyGroupPrefixByteCount;
		private boolean newKeyGroup;
		private boolean newKVState;
		private boolean valid;

		private MergeIterator currentSubIterator;

		private static final List<Comparator<MergeIterator>> COMPARATORS;

		static {
			int maxBytes = 2;
			COMPARATORS = new ArrayList<>(maxBytes);
			for (int i = 0; i < maxBytes; ++i) {
				final int currentBytes = i + 1;
				COMPARATORS.add(new Comparator<MergeIterator>() {
					@Override
					public int compare(MergeIterator o1, MergeIterator o2) {
						int arrayCmpRes = compareKeyGroupsForByteArrays(
							o1.currentKey, o2.currentKey, currentBytes);
						return arrayCmpRes == 0 ? o1.getKvStateId() - o2.getKvStateId() : arrayCmpRes;
					}
				});
			}
		}

		RocksDBMergeIterator(List<Tuple2<RocksIterator, Integer>> kvStateIterators, final int keyGroupPrefixByteCount) {
			Preconditions.checkNotNull(kvStateIterators);
			Preconditions.checkArgument(keyGroupPrefixByteCount >= 1);

			this.keyGroupPrefixByteCount = keyGroupPrefixByteCount;

			Comparator<MergeIterator> iteratorComparator = COMPARATORS.get(keyGroupPrefixByteCount - 1);

			if (kvStateIterators.size() > 0) {
				PriorityQueue<MergeIterator> iteratorPriorityQueue =
					new PriorityQueue<>(kvStateIterators.size(), iteratorComparator);

				for (Tuple2<RocksIterator, Integer> rocksIteratorWithKVStateId : kvStateIterators) {
					final RocksIterator rocksIterator = rocksIteratorWithKVStateId.f0;
					rocksIterator.seekToFirst();
					if (rocksIterator.isValid()) {
						iteratorPriorityQueue.offer(new MergeIterator(rocksIterator, rocksIteratorWithKVStateId.f1));
					} else {
						IOUtils.closeQuietly(rocksIterator);
					}
				}

				kvStateIterators.clear();

				this.heap = iteratorPriorityQueue;
				this.valid = !heap.isEmpty();
				this.currentSubIterator = heap.poll();
			} else {
				// creating a PriorityQueue of size 0 results in an exception.
				this.heap = null;
				this.valid = false;
			}

			this.newKeyGroup = true;
			this.newKVState = true;
		}

		/**
		 * Advance the iterator. Should only be called if {@link #isValid()} returned true. Valid can only chance after
		 * calls to {@link #next()}.
		 */
		public void next() {
			newKeyGroup = false;
			newKVState = false;

			final RocksIterator rocksIterator = currentSubIterator.getIterator();
			rocksIterator.next();

			byte[] oldKey = currentSubIterator.getCurrentKey();
			if (rocksIterator.isValid()) {
				currentSubIterator.currentKey = rocksIterator.key();

				if (isDifferentKeyGroup(oldKey, currentSubIterator.getCurrentKey())) {
					heap.offer(currentSubIterator);
					currentSubIterator = heap.poll();
					newKVState = currentSubIterator.getIterator() != rocksIterator;
					detectNewKeyGroup(oldKey);
				}
			} else {
				IOUtils.closeQuietly(rocksIterator);

				if (heap.isEmpty()) {
					currentSubIterator = null;
					valid = false;
				} else {
					currentSubIterator = heap.poll();
					newKVState = true;
					detectNewKeyGroup(oldKey);
				}
			}
		}

		private boolean isDifferentKeyGroup(byte[] a, byte[] b) {
			return 0 != compareKeyGroupsForByteArrays(a, b, keyGroupPrefixByteCount);
		}

		private void detectNewKeyGroup(byte[] oldKey) {
			if (isDifferentKeyGroup(oldKey, currentSubIterator.currentKey)) {
				newKeyGroup = true;
			}
		}

		/**
		 * @return key-group for the current key
		 */
		public int keyGroup() {
			int result = 0;
			//big endian decode
			for (int i = 0; i < keyGroupPrefixByteCount; ++i) {
				result <<= 8;
				result |= (currentSubIterator.currentKey[i] & 0xFF);
			}
			return result;
		}

		public byte[] key() {
			return currentSubIterator.getCurrentKey();
		}

		public byte[] value() {
			return currentSubIterator.getIterator().value();
		}

		/**
		 * @return Id of K/V state to which the current key belongs.
		 */
		public int kvStateId() {
			return currentSubIterator.getKvStateId();
		}

		/**
		 * Indicates if current key starts a new k/v-state, i.e. belong to a different k/v-state than it's predecessor.
		 * @return true iff the current key belong to a different k/v-state than it's predecessor.
		 */
		public boolean isNewKeyValueState() {
			return newKVState;
		}

		/**
		 * Indicates if current key starts a new key-group, i.e. belong to a different key-group than it's predecessor.
		 * @return true iff the current key belong to a different key-group than it's predecessor.
		 */
		public boolean isNewKeyGroup() {
			return newKeyGroup;
		}

		/**
		 * Check if the iterator is still valid. Getters like {@link #key()}, {@link #value()}, etc. as well as
		 * {@link #next()} should only be called if valid returned true. Should be checked after each call to
		 * {@link #next()} before accessing iterator state.
		 * @return True iff this iterator is valid.
		 */
		public boolean isValid() {
			return valid;
		}

		private static int compareKeyGroupsForByteArrays(byte[] a, byte[] b, int len) {
			for (int i = 0; i < len; ++i) {
				int diff = (a[i] & 0xFF) - (b[i] & 0xFF);
				if (diff != 0) {
					return diff;
				}
			}
			return 0;
		}

		@Override
		public void close() {
			IOUtils.closeQuietly(currentSubIterator);
			currentSubIterator = null;

			IOUtils.closeAllQuietly(heap);
			heap.clear();
		}
	}

	/**
	 * Wraps a RocksDB iterator to cache it's current key and assigns an id for the key/value state to the iterator.
	 * Used by #MergeIterator.
	 */
	private static final class MergeIterator implements AutoCloseable {

		/**
		 * @param iterator  The #RocksIterator to wrap .
		 * @param kvStateId Id of the K/V state to which this iterator belongs.
		 */
		MergeIterator(RocksIterator iterator, int kvStateId) {
			this.iterator = Preconditions.checkNotNull(iterator);
			this.currentKey = iterator.key();
			this.kvStateId = kvStateId;
		}

		private final RocksIterator iterator;
		private byte[] currentKey;
		private final int kvStateId;

		public byte[] getCurrentKey() {
			return currentKey;
		}

		public void setCurrentKey(byte[] currentKey) {
			this.currentKey = currentKey;
		}

		public RocksIterator getIterator() {
			return iterator;
		}

		public int getKvStateId() {
			return kvStateId;
		}

		@Override
		public void close() {
			IOUtils.closeQuietly(iterator);
		}
	}

	/**
	 * Adapter class to bridge between {@link RocksIterator} and {@link Iterator} to iterate over the keys. This class
	 * is not thread safe.
	 *
	 * @param <K> the type of the iterated objects, which are keys in RocksDB.
	 */
	static class RocksIteratorForKeysWrapper<K> implements Iterator<K>, AutoCloseable {
		private final RocksIterator iterator;
		private final String state;
		private final TypeSerializer<K> keySerializer;
		private final int keyGroupPrefixBytes;
		private final byte[] namespaceBytes;
		private final boolean ambiguousKeyPossible;
		private K nextKey;

		RocksIteratorForKeysWrapper(
			RocksIterator iterator,
			String state,
			TypeSerializer<K> keySerializer,
			int keyGroupPrefixBytes,
			boolean ambiguousKeyPossible,
			byte[] namespaceBytes) {
			this.iterator = Preconditions.checkNotNull(iterator);
			this.state = Preconditions.checkNotNull(state);
			this.keySerializer = Preconditions.checkNotNull(keySerializer);
			this.keyGroupPrefixBytes = Preconditions.checkNotNull(keyGroupPrefixBytes);
			this.namespaceBytes = Preconditions.checkNotNull(namespaceBytes);
			this.nextKey = null;
			this.ambiguousKeyPossible = ambiguousKeyPossible;
		}

		@Override
		public boolean hasNext() {
			while (nextKey == null && iterator.isValid()) {
				try {
					byte[] key = iterator.key();
					if (isMatchingNameSpace(key)) {
						ByteArrayInputStreamWithPos inputStream =
							new ByteArrayInputStreamWithPos(key, keyGroupPrefixBytes, key.length - keyGroupPrefixBytes);
						DataInputViewStreamWrapper dataInput = new DataInputViewStreamWrapper(inputStream);
						K value = RocksDBKeySerializationUtils.readKey(
							keySerializer,
							inputStream,
							dataInput,
							ambiguousKeyPossible);
						nextKey = value;
					}
					iterator.next();
				} catch (IOException e) {
					throw new FlinkRuntimeException("Failed to access state [" + state + "]", e);
				}
			}
			return nextKey != null;
		}

		@Override
		public K next() {
			if (!hasNext()) {
				throw new NoSuchElementException("Failed to access state [" + state + "]");
			}

			K tmpKey = nextKey;
			nextKey = null;
			return tmpKey;
		}

		private boolean isMatchingNameSpace(@Nonnull byte[] key) {
			final int namespaceBytesLength = namespaceBytes.length;
			final int basicLength = namespaceBytesLength + keyGroupPrefixBytes;
			if (key.length >= basicLength) {
				for (int i = 1; i <= namespaceBytesLength; ++i) {
					if (key[key.length - i] != namespaceBytes[namespaceBytesLength - i]) {
						return false;
					}
				}
				return true;
			}
			return false;
		}

		@Override
		public void close() {
			iterator.close();
		}
	}

	private class FullSnapshotStrategy implements SnapshotStrategy<SnapshotResult<KeyedStateHandle>> {

		@Override
		public RunnableFuture<SnapshotResult<KeyedStateHandle>> performSnapshot(
			long checkpointId,
			long timestamp,
			CheckpointStreamFactory primaryStreamFactory,
			CheckpointOptions checkpointOptions) throws Exception {

			long startTime = System.currentTimeMillis();

			if (kvStateInformation.isEmpty()) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Asynchronous RocksDB snapshot performed on empty keyed state at {}. Returning null.",
						timestamp);
				}

				return DoneFuture.of(SnapshotResult.empty());
			}

			final SupplierWithException<CheckpointStreamWithResultProvider, Exception> supplier =

				isWithLocalRecovery(
					checkpointOptions.getCheckpointType(),
					localRecoveryConfig.getLocalRecoveryMode()) ?

					() -> CheckpointStreamWithResultProvider.createDuplicatingStream(
						checkpointId,
						CheckpointedStateScope.EXCLUSIVE,
						primaryStreamFactory,
						localRecoveryConfig.getLocalStateDirectoryProvider()) :

					() -> CheckpointStreamWithResultProvider.createSimpleStream(
						CheckpointedStateScope.EXCLUSIVE,
						primaryStreamFactory);

			final CloseableRegistry snapshotCloseableRegistry = new CloseableRegistry();

			final RocksDBFullSnapshotOperation<K> snapshotOperation =
				new RocksDBFullSnapshotOperation<>(
					RocksDBKeyedStateBackend.this,
					supplier,
					snapshotCloseableRegistry);

			snapshotOperation.takeDBSnapShot();

			// implementation of the async IO operation, based on FutureTask
			AbstractAsyncCallableWithResources<SnapshotResult<KeyedStateHandle>> ioCallable =
				new AbstractAsyncCallableWithResources<SnapshotResult<KeyedStateHandle>>() {

					@Override
					protected void acquireResources() throws Exception {
						cancelStreamRegistry.registerCloseable(snapshotCloseableRegistry);
						snapshotOperation.openCheckpointStream();
					}

					@Override
					protected void releaseResources() throws Exception {
						closeLocalRegistry();
						releaseSnapshotOperationResources();
					}

					private void releaseSnapshotOperationResources() {
						// hold the db lock while operation on the db to guard us against async db disposal
						snapshotOperation.releaseSnapshotResources();
					}

					@Override
					protected void stopOperation() throws Exception {
						closeLocalRegistry();
					}

					private void closeLocalRegistry() {
						if (cancelStreamRegistry.unregisterCloseable(snapshotCloseableRegistry)) {
							try {
								snapshotCloseableRegistry.close();
							} catch (Exception ex) {
								LOG.warn("Error closing local registry", ex);
							}
						}
					}

					@Nonnull
					@Override
					public SnapshotResult<KeyedStateHandle> performOperation() throws Exception {
						long startTime = System.currentTimeMillis();

						if (isStopped()) {
							throw new IOException("RocksDB closed.");
						}

						snapshotOperation.writeDBSnapshot();

						LOG.info("Asynchronous RocksDB snapshot ({}, asynchronous part) in thread {} took {} ms.",
							primaryStreamFactory, Thread.currentThread(), (System.currentTimeMillis() - startTime));

						return snapshotOperation.getSnapshotResultStateHandle();
					}
				};

			LOG.info("Asynchronous RocksDB snapshot ({}, synchronous part) in thread {} took {} ms.",
				primaryStreamFactory, Thread.currentThread(), (System.currentTimeMillis() - startTime));
			return AsyncStoppableTaskWithCallback.from(ioCallable);
		}

		private boolean isWithLocalRecovery(
			CheckpointType checkpointType,
			LocalRecoveryConfig.LocalRecoveryMode recoveryMode) {
			// we use local recovery when it is activated and we are not taking a savepoint.
			return LocalRecoveryConfig.LocalRecoveryMode.ENABLE_FILE_BASED == recoveryMode
				&& CheckpointType.SAVEPOINT != checkpointType;
		}
	}

	private class IncrementalSnapshotStrategy implements SnapshotStrategy<SnapshotResult<KeyedStateHandle>> {

		private final SnapshotStrategy<SnapshotResult<KeyedStateHandle>> savepointDelegate;

		public IncrementalSnapshotStrategy() {
			this.savepointDelegate = new FullSnapshotStrategy();
		}

		@Override
		public RunnableFuture<SnapshotResult<KeyedStateHandle>> performSnapshot(
			long checkpointId,
			long checkpointTimestamp,
			CheckpointStreamFactory checkpointStreamFactory,
			CheckpointOptions checkpointOptions) throws Exception {

			// for savepoints, we delegate to the full snapshot strategy because savepoints are always self-contained.
			if (CheckpointType.SAVEPOINT == checkpointOptions.getCheckpointType()) {
				return savepointDelegate.performSnapshot(
					checkpointId,
					checkpointTimestamp,
					checkpointStreamFactory,
					checkpointOptions);
			}

			if (db == null) {
				throw new IOException("RocksDB closed.");
			}

			if (kvStateInformation.isEmpty()) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Asynchronous RocksDB snapshot performed on empty keyed state at {}. Returning null.", checkpointTimestamp);
				}
				return DoneFuture.of(SnapshotResult.empty());
			}

			SnapshotDirectory snapshotDirectory;

			if (LocalRecoveryConfig.LocalRecoveryMode.ENABLE_FILE_BASED == localRecoveryConfig.getLocalRecoveryMode()) {
				// create a "permanent" snapshot directory for local recovery.
				LocalRecoveryDirectoryProvider directoryProvider = localRecoveryConfig.getLocalStateDirectoryProvider();
				File directory = directoryProvider.subtaskSpecificCheckpointDirectory(checkpointId);

				if (directory.exists()) {
					FileUtils.deleteDirectory(directory);
				}

				if (!directory.mkdirs()) {
					throw new IOException("Local state base directory for checkpoint " + checkpointId +
						" already exists: " + directory);
				}

				// introduces an extra directory because RocksDB wants a non-existing directory for native checkpoints.
				File rdbSnapshotDir = new File(directory, "rocks_db");
				Path path = new Path(rdbSnapshotDir.toURI());
				// create a "permanent" snapshot directory because local recovery is active.
				snapshotDirectory = SnapshotDirectory.permanent(path);
			} else {
				// create a "temporary" snapshot directory because local recovery is inactive.
				Path path = new Path(instanceBasePath.getAbsolutePath(), "chk-" + checkpointId);
				snapshotDirectory = SnapshotDirectory.temporary(path);
			}

			final RocksDBIncrementalSnapshotOperation<K> snapshotOperation =
				new RocksDBIncrementalSnapshotOperation<>(
					RocksDBKeyedStateBackend.this,
					checkpointStreamFactory,
					snapshotDirectory,
					checkpointId);

			try {
				snapshotOperation.takeSnapshot();
			} catch (Exception e) {
				snapshotOperation.stop();
				snapshotOperation.releaseResources(true);
				throw e;
			}

			return new FutureTask<SnapshotResult<KeyedStateHandle>>(
				snapshotOperation::runSnapshot
			) {
				@Override
				public boolean cancel(boolean mayInterruptIfRunning) {
					snapshotOperation.stop();
					return super.cancel(mayInterruptIfRunning);
				}

				@Override
				protected void done() {
					snapshotOperation.releaseResources(isCancelled());
				}
			};
		}
	}

	/**
	 * Encapsulates the process to perform a full snapshot of a RocksDBKeyedStateBackend.
	 */
	@VisibleForTesting
	static class RocksDBFullSnapshotOperation<K>
		extends AbstractAsyncCallableWithResources<SnapshotResult<KeyedStateHandle>> {

		static final int FIRST_BIT_IN_BYTE_MASK = 0x80;
		static final int END_OF_KEY_GROUP_MARK = 0xFFFF;

		private final RocksDBKeyedStateBackend<K> stateBackend;
		private final KeyGroupRangeOffsets keyGroupRangeOffsets;
		private final SupplierWithException<CheckpointStreamWithResultProvider, Exception> checkpointStreamSupplier;
		private final CloseableRegistry snapshotCloseableRegistry;
		private final ResourceGuard.Lease dbLease;

		private Snapshot snapshot;
		private ReadOptions readOptions;

		/**
		 * The state meta data.
		 */
		private List<RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> stateMetaInfoSnapshots;

		/**
		 * The copied column handle.
		 */
		private List<ColumnFamilyHandle> copiedColumnFamilyHandles;

		private List<Tuple2<RocksIterator, Integer>> kvStateIterators;

		private CheckpointStreamWithResultProvider checkpointStreamWithResultProvider;
		private DataOutputView outputView;

		RocksDBFullSnapshotOperation(
			RocksDBKeyedStateBackend<K> stateBackend,
			SupplierWithException<CheckpointStreamWithResultProvider, Exception> checkpointStreamSupplier,
			CloseableRegistry registry) throws IOException {

			this.stateBackend = stateBackend;
			this.checkpointStreamSupplier = checkpointStreamSupplier;
			this.keyGroupRangeOffsets = new KeyGroupRangeOffsets(stateBackend.keyGroupRange);
			this.snapshotCloseableRegistry = registry;
			this.dbLease = this.stateBackend.rocksDBResourceGuard.acquireResource();
		}

		/**
		 * 1) Create a snapshot object from RocksDB.
		 *
		 */
		public void takeDBSnapShot() {
			Preconditions.checkArgument(snapshot == null, "Only one ongoing snapshot allowed!");

			this.stateMetaInfoSnapshots = new ArrayList<>(stateBackend.kvStateInformation.size());

			this.copiedColumnFamilyHandles = new ArrayList<>(stateBackend.kvStateInformation.size());

			for (Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<?, ?>> tuple2 :
				stateBackend.kvStateInformation.values()) {
				// snapshot meta info
				this.stateMetaInfoSnapshots.add(tuple2.f1.snapshot());

				// copy column family handle
				this.copiedColumnFamilyHandles.add(tuple2.f0);
			}
			this.snapshot = stateBackend.db.getSnapshot();
		}

		/**
		 * 2) Open CheckpointStateOutputStream through the checkpointStreamFactory into which we will write.
		 *
		 * @throws Exception
		 */
		public void openCheckpointStream() throws Exception {
			Preconditions.checkArgument(checkpointStreamWithResultProvider == null,
				"Output stream for snapshot is already set.");

			checkpointStreamWithResultProvider = checkpointStreamSupplier.get();
			snapshotCloseableRegistry.registerCloseable(checkpointStreamWithResultProvider);
			outputView = new DataOutputViewStreamWrapper(
				checkpointStreamWithResultProvider.getCheckpointOutputStream());
		}

		/**
		 * 3) Write the actual data from RocksDB from the time we took the snapshot object in (1).
		 *
		 * @throws IOException
		 */
		public void writeDBSnapshot() throws IOException, InterruptedException {

			if (null == snapshot) {
				throw new IOException("No snapshot available. Might be released due to cancellation.");
			}

			Preconditions.checkNotNull(checkpointStreamWithResultProvider, "No output stream to write snapshot.");
			writeKVStateMetaData();
			writeKVStateData();
		}

		/**
		 * 4) Returns a snapshot result for the completed snapshot.
		 *
		 * @return snapshot result for the completed snapshot.
		 */
		@Nonnull
		public SnapshotResult<KeyedStateHandle> getSnapshotResultStateHandle() throws IOException {

			if (snapshotCloseableRegistry.unregisterCloseable(checkpointStreamWithResultProvider)) {

				SnapshotResult<StreamStateHandle> res =
					checkpointStreamWithResultProvider.closeAndFinalizeCheckpointStreamResult();
				checkpointStreamWithResultProvider = null;
				return CheckpointStreamWithResultProvider.toKeyedStateHandleSnapshotResult(res, keyGroupRangeOffsets);
			}

			return SnapshotResult.empty();
		}

		/**
		 * 5) Release the snapshot object for RocksDB and clean up.
		 */
		public void releaseSnapshotResources() {

			checkpointStreamWithResultProvider = null;

			if (null != kvStateIterators) {
				for (Tuple2<RocksIterator, Integer> kvStateIterator : kvStateIterators) {
					IOUtils.closeQuietly(kvStateIterator.f0);
				}
				kvStateIterators = null;
			}

			if (null != snapshot) {
				if (null != stateBackend.db) {
					stateBackend.db.releaseSnapshot(snapshot);
				}
				IOUtils.closeQuietly(snapshot);
				snapshot = null;
			}

			if (null != readOptions) {
				IOUtils.closeQuietly(readOptions);
				readOptions = null;
			}

			this.dbLease.close();
		}

		private void writeKVStateMetaData() throws IOException {

			this.kvStateIterators = new ArrayList<>(copiedColumnFamilyHandles.size());

			int kvStateId = 0;

			//retrieve iterator for this k/v states
			readOptions = new ReadOptions();
			readOptions.setSnapshot(snapshot);

			for (ColumnFamilyHandle columnFamilyHandle : copiedColumnFamilyHandles) {

				kvStateIterators.add(
					new Tuple2<>(stateBackend.db.newIterator(columnFamilyHandle, readOptions), kvStateId));

				++kvStateId;
			}

			KeyedBackendSerializationProxy<K> serializationProxy =
				new KeyedBackendSerializationProxy<>(
					// TODO: this code assumes that writing a serializer is threadsafe, we should support to
					// get a serialized form already at state registration time in the future
					stateBackend.getKeySerializer(),
					stateMetaInfoSnapshots,
					!Objects.equals(
						UncompressedStreamCompressionDecorator.INSTANCE,
						stateBackend.keyGroupCompressionDecorator));

			serializationProxy.write(outputView);
		}

		private void writeKVStateData() throws IOException, InterruptedException {

			byte[] previousKey = null;
			byte[] previousValue = null;
			DataOutputView kgOutView = null;
			OutputStream kgOutStream = null;
			CheckpointStreamFactory.CheckpointStateOutputStream checkpointOutputStream =
				checkpointStreamWithResultProvider.getCheckpointOutputStream();

			try {
				// Here we transfer ownership of RocksIterators to the RocksDBMergeIterator
				try (RocksDBMergeIterator mergeIterator = new RocksDBMergeIterator(
					kvStateIterators, stateBackend.keyGroupPrefixBytes)) {

					// handover complete, null out to prevent double close
					kvStateIterators = null;

					//preamble: setup with first key-group as our lookahead
					if (mergeIterator.isValid()) {
						//begin first key-group by recording the offset
						keyGroupRangeOffsets.setKeyGroupOffset(
							mergeIterator.keyGroup(),
							checkpointOutputStream.getPos());
						//write the k/v-state id as metadata
						kgOutStream = stateBackend.keyGroupCompressionDecorator.
							decorateWithCompression(checkpointOutputStream);
						kgOutView = new DataOutputViewStreamWrapper(kgOutStream);
						//TODO this could be aware of keyGroupPrefixBytes and write only one byte if possible
						kgOutView.writeShort(mergeIterator.kvStateId());
						previousKey = mergeIterator.key();
						previousValue = mergeIterator.value();
						mergeIterator.next();
					}

					//main loop: write k/v pairs ordered by (key-group, kv-state), thereby tracking key-group offsets.
					while (mergeIterator.isValid()) {

						assert (!hasMetaDataFollowsFlag(previousKey));

						//set signal in first key byte that meta data will follow in the stream after this k/v pair
						if (mergeIterator.isNewKeyGroup() || mergeIterator.isNewKeyValueState()) {

							//be cooperative and check for interruption from time to time in the hot loop
							checkInterrupted();

							setMetaDataFollowsFlagInKey(previousKey);
						}

						writeKeyValuePair(previousKey, previousValue, kgOutView);

						//write meta data if we have to
						if (mergeIterator.isNewKeyGroup()) {
							//TODO this could be aware of keyGroupPrefixBytes and write only one byte if possible
							kgOutView.writeShort(END_OF_KEY_GROUP_MARK);
							// this will just close the outer stream
							kgOutStream.close();
							//begin new key-group
							keyGroupRangeOffsets.setKeyGroupOffset(
								mergeIterator.keyGroup(),
								checkpointOutputStream.getPos());
							//write the kev-state
							//TODO this could be aware of keyGroupPrefixBytes and write only one byte if possible
							kgOutStream = stateBackend.keyGroupCompressionDecorator.
								decorateWithCompression(checkpointOutputStream);
							kgOutView = new DataOutputViewStreamWrapper(kgOutStream);
							kgOutView.writeShort(mergeIterator.kvStateId());
						} else if (mergeIterator.isNewKeyValueState()) {
							//write the k/v-state
							//TODO this could be aware of keyGroupPrefixBytes and write only one byte if possible
							kgOutView.writeShort(mergeIterator.kvStateId());
						}

						//request next k/v pair
						previousKey = mergeIterator.key();
						previousValue = mergeIterator.value();
						mergeIterator.next();
					}
				}

				//epilogue: write last key-group
				if (previousKey != null) {
					assert (!hasMetaDataFollowsFlag(previousKey));
					setMetaDataFollowsFlagInKey(previousKey);
					writeKeyValuePair(previousKey, previousValue, kgOutView);
					//TODO this could be aware of keyGroupPrefixBytes and write only one byte if possible
					kgOutView.writeShort(END_OF_KEY_GROUP_MARK);
					// this will just close the outer stream
					kgOutStream.close();
					kgOutStream = null;
				}

			} finally {
				// this will just close the outer stream
				IOUtils.closeQuietly(kgOutStream);
			}
		}

		private void writeKeyValuePair(byte[] key, byte[] value, DataOutputView out) throws IOException {
			BytePrimitiveArraySerializer.INSTANCE.serialize(key, out);
			BytePrimitiveArraySerializer.INSTANCE.serialize(value, out);
		}

		static void setMetaDataFollowsFlagInKey(byte[] key) {
			key[0] |= FIRST_BIT_IN_BYTE_MASK;
		}

		static void clearMetaDataFollowsFlag(byte[] key) {
			key[0] &= (~RocksDBFullSnapshotOperation.FIRST_BIT_IN_BYTE_MASK);
		}

		static boolean hasMetaDataFollowsFlag(byte[] key) {
			return 0 != (key[0] & RocksDBFullSnapshotOperation.FIRST_BIT_IN_BYTE_MASK);
		}

		private static void checkInterrupted() throws InterruptedException {
			if (Thread.currentThread().isInterrupted()) {
				throw new InterruptedException("RocksDB snapshot interrupted.");
			}
		}

		@Override
		protected void acquireResources() throws Exception {
			stateBackend.cancelStreamRegistry.registerCloseable(snapshotCloseableRegistry);
			openCheckpointStream();
		}

		@Override
		protected void releaseResources() {
			closeLocalRegistry();
			releaseSnapshotOperationResources();
		}

		private void releaseSnapshotOperationResources() {
			// hold the db lock while operation on the db to guard us against async db disposal
			releaseSnapshotResources();
		}

		@Override
		protected void stopOperation() {
			closeLocalRegistry();
		}

		private void closeLocalRegistry() {
			if (stateBackend.cancelStreamRegistry.unregisterCloseable(snapshotCloseableRegistry)) {
				try {
					snapshotCloseableRegistry.close();
				} catch (Exception ex) {
					LOG.warn("Error closing local registry", ex);
				}
			}
		}

		@Nonnull
		@Override
		public SnapshotResult<KeyedStateHandle> performOperation() throws Exception {
			long startTime = System.currentTimeMillis();

			if (isStopped()) {
				throw new IOException("RocksDB closed.");
			}

			writeDBSnapshot();

			LOG.info("Asynchronous RocksDB snapshot ({}, asynchronous part) in thread {} took {} ms.",
				checkpointStreamSupplier, Thread.currentThread(), (System.currentTimeMillis() - startTime));

			return getSnapshotResultStateHandle();
		}
	}

	/**
	 * Encapsulates the process to perform an incremental snapshot of a RocksDBKeyedStateBackend.
	 */
	private static final class RocksDBIncrementalSnapshotOperation<K> {

		/** The backend which we snapshot. */
		private final RocksDBKeyedStateBackend<K> stateBackend;

		/** Stream factory that creates the outpus streams to DFS. */
		private final CheckpointStreamFactory checkpointStreamFactory;

		/** Id for the current checkpoint. */
		private final long checkpointId;

		/** All sst files that were part of the last previously completed checkpoint. */
		private Set<StateHandleID> baseSstFiles;

		/** The state meta data. */
		private final List<RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> stateMetaInfoSnapshots = new ArrayList<>();

		/** Local directory for the RocksDB native backup. */
		private SnapshotDirectory localBackupDirectory;

		// Registry for all opened i/o streams
		private final CloseableRegistry closeableRegistry = new CloseableRegistry();

		// new sst files since the last completed checkpoint
		private final Map<StateHandleID, StreamStateHandle> sstFiles = new HashMap<>();

		// handles to the misc files in the current snapshot
		private final Map<StateHandleID, StreamStateHandle> miscFiles = new HashMap<>();

		// This lease protects from concurrent disposal of the native rocksdb instance.
		private final ResourceGuard.Lease dbLease;

		private SnapshotResult<StreamStateHandle> metaStateHandle = null;

		private RocksDBIncrementalSnapshotOperation(
			RocksDBKeyedStateBackend<K> stateBackend,
			CheckpointStreamFactory checkpointStreamFactory,
			SnapshotDirectory localBackupDirectory,
			long checkpointId) throws IOException {

			this.stateBackend = stateBackend;
			this.checkpointStreamFactory = checkpointStreamFactory;
			this.checkpointId = checkpointId;
			this.dbLease = this.stateBackend.rocksDBResourceGuard.acquireResource();
			this.localBackupDirectory = localBackupDirectory;
		}

		private StreamStateHandle materializeStateData(Path filePath) throws Exception {
			FSDataInputStream inputStream = null;
			CheckpointStreamFactory.CheckpointStateOutputStream outputStream = null;

			try {
				final byte[] buffer = new byte[8 * 1024];

				FileSystem backupFileSystem = localBackupDirectory.getFileSystem();
				inputStream = backupFileSystem.open(filePath);
				closeableRegistry.registerCloseable(inputStream);

				outputStream = checkpointStreamFactory
					.createCheckpointStateOutputStream(CheckpointedStateScope.SHARED);
				closeableRegistry.registerCloseable(outputStream);

				while (true) {
					int numBytes = inputStream.read(buffer);

					if (numBytes == -1) {
						break;
					}

					outputStream.write(buffer, 0, numBytes);
				}

				StreamStateHandle result = null;
				if (closeableRegistry.unregisterCloseable(outputStream)) {
					result = outputStream.closeAndGetHandle();
					outputStream = null;
				}
				return result;

			} finally {

				if (closeableRegistry.unregisterCloseable(inputStream)) {
					inputStream.close();
				}

				if (closeableRegistry.unregisterCloseable(outputStream)) {
					outputStream.close();
				}
			}
		}

		@Nonnull
		private SnapshotResult<StreamStateHandle> materializeMetaData() throws Exception {

			LocalRecoveryConfig localRecoveryConfig = stateBackend.localRecoveryConfig;

			CheckpointStreamWithResultProvider streamWithResultProvider =

				LocalRecoveryConfig.LocalRecoveryMode.ENABLE_FILE_BASED == localRecoveryConfig.getLocalRecoveryMode() ?

					CheckpointStreamWithResultProvider.createDuplicatingStream(
						checkpointId,
						CheckpointedStateScope.EXCLUSIVE,
						checkpointStreamFactory,
						localRecoveryConfig.getLocalStateDirectoryProvider()) :

					CheckpointStreamWithResultProvider.createSimpleStream(
						CheckpointedStateScope.EXCLUSIVE,
						checkpointStreamFactory);

			try {
				closeableRegistry.registerCloseable(streamWithResultProvider);

				//no need for compression scheme support because sst-files are already compressed
				KeyedBackendSerializationProxy<K> serializationProxy =
					new KeyedBackendSerializationProxy<>(
						stateBackend.keySerializer,
						stateMetaInfoSnapshots,
						false);

				DataOutputView out =
					new DataOutputViewStreamWrapper(streamWithResultProvider.getCheckpointOutputStream());

				serializationProxy.write(out);

				if (closeableRegistry.unregisterCloseable(streamWithResultProvider)) {
					SnapshotResult<StreamStateHandle> result =
						streamWithResultProvider.closeAndFinalizeCheckpointStreamResult();
					streamWithResultProvider = null;
					return result;
				} else {
					throw new IOException("Stream already closed and cannot return a handle.");
				}
			} finally {
				if (streamWithResultProvider != null) {
					if (closeableRegistry.unregisterCloseable(streamWithResultProvider)) {
						IOUtils.closeQuietly(streamWithResultProvider);
					}
				}
			}
		}

		void takeSnapshot() throws Exception {

			final long lastCompletedCheckpoint;

			// use the last completed checkpoint as the comparison base.
			synchronized (stateBackend.materializedSstFiles) {
				lastCompletedCheckpoint = stateBackend.lastCompletedCheckpointId;
				baseSstFiles = stateBackend.materializedSstFiles.get(lastCompletedCheckpoint);
			}

			LOG.trace("Taking incremental snapshot for checkpoint {}. Snapshot is based on last completed checkpoint {} " +
				"assuming the following (shared) files as base: {}.", checkpointId, lastCompletedCheckpoint, baseSstFiles);

			// save meta data
			for (Map.Entry<String, Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<?, ?>>> stateMetaInfoEntry
				: stateBackend.kvStateInformation.entrySet()) {
				stateMetaInfoSnapshots.add(stateMetaInfoEntry.getValue().f1.snapshot());
			}

			LOG.trace("Local RocksDB checkpoint goes to backup path {}.", localBackupDirectory);

			if (localBackupDirectory.exists()) {
				throw new IllegalStateException("Unexpected existence of the backup directory.");
			}

			// create hard links of living files in the snapshot path
			try (Checkpoint checkpoint = Checkpoint.create(stateBackend.db)) {
				checkpoint.createCheckpoint(localBackupDirectory.getDirectory().getPath());
			}
		}

		@Nonnull
		SnapshotResult<KeyedStateHandle> runSnapshot() throws Exception {

			stateBackend.cancelStreamRegistry.registerCloseable(closeableRegistry);

			// write meta data
			metaStateHandle = materializeMetaData();

			// sanity checks - they should never fail
			Preconditions.checkNotNull(metaStateHandle,
				"Metadata was not properly created.");
			Preconditions.checkNotNull(metaStateHandle.getJobManagerOwnedSnapshot(),
				"Metadata for job manager was not properly created.");

			// write state data
			Preconditions.checkState(localBackupDirectory.exists());

			FileStatus[] fileStatuses = localBackupDirectory.listStatus();
			if (fileStatuses != null) {
				for (FileStatus fileStatus : fileStatuses) {
					final Path filePath = fileStatus.getPath();
					final String fileName = filePath.getName();
					final StateHandleID stateHandleID = new StateHandleID(fileName);

					if (fileName.endsWith(SST_FILE_SUFFIX)) {
						final boolean existsAlready =
							baseSstFiles != null && baseSstFiles.contains(stateHandleID);

						if (existsAlready) {
							// we introduce a placeholder state handle, that is replaced with the
							// original from the shared state registry (created from a previous checkpoint)
							sstFiles.put(
								stateHandleID,
								new PlaceholderStreamStateHandle());
						} else {
							sstFiles.put(stateHandleID, materializeStateData(filePath));
						}
					} else {
						StreamStateHandle fileHandle = materializeStateData(filePath);
						miscFiles.put(stateHandleID, fileHandle);
					}
				}
			}

			synchronized (stateBackend.materializedSstFiles) {
				stateBackend.materializedSstFiles.put(checkpointId, sstFiles.keySet());
			}

			IncrementalKeyedStateHandle jmIncrementalKeyedStateHandle = new IncrementalKeyedStateHandle(
				stateBackend.backendUID,
				stateBackend.keyGroupRange,
				checkpointId,
				sstFiles,
				miscFiles,
				metaStateHandle.getJobManagerOwnedSnapshot());

			StreamStateHandle taskLocalSnapshotMetaDataStateHandle = metaStateHandle.getTaskLocalSnapshot();
			DirectoryStateHandle directoryStateHandle = null;

			try {

				directoryStateHandle = localBackupDirectory.completeSnapshotAndGetHandle();
			} catch (IOException ex) {

				Exception collector = ex;

				try {
					taskLocalSnapshotMetaDataStateHandle.discardState();
				} catch (Exception discardEx) {
					collector = ExceptionUtils.firstOrSuppressed(discardEx, collector);
				}

				LOG.warn("Problem with local state snapshot.", collector);
			}

			if (directoryStateHandle != null && taskLocalSnapshotMetaDataStateHandle != null) {

				IncrementalLocalKeyedStateHandle localDirKeyedStateHandle =
					new IncrementalLocalKeyedStateHandle(
						stateBackend.backendUID,
						checkpointId,
						directoryStateHandle,
						stateBackend.keyGroupRange,
						taskLocalSnapshotMetaDataStateHandle,
						sstFiles.keySet());
				return SnapshotResult.withLocalState(jmIncrementalKeyedStateHandle, localDirKeyedStateHandle);
			} else {
				return SnapshotResult.of(jmIncrementalKeyedStateHandle);
			}
		}

		void stop() {

			if (stateBackend.cancelStreamRegistry.unregisterCloseable(closeableRegistry)) {
				try {
					closeableRegistry.close();
				} catch (IOException e) {
					LOG.warn("Could not properly close io streams.", e);
				}
			}
		}

		void releaseResources(boolean canceled) {

			dbLease.close();

			if (stateBackend.cancelStreamRegistry.unregisterCloseable(closeableRegistry)) {
				try {
					closeableRegistry.close();
				} catch (IOException e) {
					LOG.warn("Exception on closing registry.", e);
				}
			}

			try {
				if (localBackupDirectory.exists()) {
					LOG.trace("Running cleanup for local RocksDB backup directory {}.", localBackupDirectory);
					boolean cleanupOk = localBackupDirectory.cleanup();

					if (!cleanupOk) {
						LOG.debug("Could not properly cleanup local RocksDB backup directory.");
					}
				}
			} catch (IOException e) {
				LOG.warn("Could not properly cleanup local RocksDB backup directory.", e);
			}

			if (canceled) {
				Collection<StateObject> statesToDiscard =
					new ArrayList<>(1 + miscFiles.size() + sstFiles.size());

				statesToDiscard.add(metaStateHandle);
				statesToDiscard.addAll(miscFiles.values());
				statesToDiscard.addAll(sstFiles.values());

				try {
					StateUtil.bestEffortDiscardAllStateObjects(statesToDiscard);
				} catch (Exception e) {
					LOG.warn("Could not properly discard states.", e);
				}

				if (localBackupDirectory.isSnapshotCompleted()) {
					try {
						DirectoryStateHandle directoryStateHandle = localBackupDirectory.completeSnapshotAndGetHandle();
						if (directoryStateHandle != null) {
							directoryStateHandle.discardState();
						}
					} catch (Exception e) {
						LOG.warn("Could not properly discard local state.", e);
					}
				}
			}
		}
	}
}
