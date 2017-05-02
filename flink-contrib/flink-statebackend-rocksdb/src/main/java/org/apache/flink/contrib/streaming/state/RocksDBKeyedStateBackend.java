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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.migration.MigrationNamespaceSerializerProxy;
import org.apache.flink.migration.MigrationUtil;
import org.apache.flink.migration.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.migration.state.MigrationKeyGroupStateHandle;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.io.async.AbstractAsyncIOCallable;
import org.apache.flink.runtime.io.async.AsyncStoppableTaskWithCallback;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.DoneFuture;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.RegisteredBackendStateMetaInfo;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.internal.InternalFoldingState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalReducingState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.runtime.util.SerializableObject;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.RunnableFuture;

/**
 * A {@link AbstractKeyedStateBackend} that stores its state in {@code RocksDB} and will serialize state to
 * streams provided by a {@link org.apache.flink.runtime.state.CheckpointStreamFactory} upon
 * checkpointing. This state backend can store very large state that exceeds memory and spills
 * to disk. Except for the snapshotting, this class should be accessed as if it is not threadsafe.
 */
public class RocksDBKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {

	private static final Logger LOG = LoggerFactory.getLogger(RocksDBKeyedStateBackend.class);

	/** The column family options from the options factory */
	private final ColumnFamilyOptions columnOptions;

	/** The DB options from the options factory */
	private final DBOptions dbOptions;

	/** Path where this configured instance stores its data directory */
	private final File instanceBasePath;

	/** Path where this configured instance stores its RocksDB data base */
	private final File instanceRocksDBPath;

	/**
	 * Lock for protecting cleanup of the RocksDB against the checkpointing thread. We acquire this when doing
	 * asynchronous checkpoints and when disposing the DB. Otherwise, the asynchronous snapshot might try
	 * iterating over a disposed DB. After aquriring the lock, always first check if (db == null).
	 */
	private final SerializableObject asyncSnapshotLock = new SerializableObject();

	/**
	 * Our RocksDB data base, this is used by the actual subclasses of {@link AbstractRocksDBState}
	 * to store state. The different k/v states that we have don't each have their own RocksDB
	 * instance. They all write to this instance but to their own column family.
	 */
	protected RocksDB db;

	/**
	 * Information about the k/v states as we create them. This is used to retrieve the
	 * column family that is used for a state and also for sanity checks when restoring.
	 */
	private Map<String, Tuple2<ColumnFamilyHandle, RegisteredBackendStateMetaInfo<?, ?>>> kvStateInformation;

	/** Number of bytes required to prefix the key groups. */
	private final int keyGroupPrefixBytes;

	public RocksDBKeyedStateBackend(
			JobID jobId,
			String operatorIdentifier,
			ClassLoader userCodeClassLoader,
			File instanceBasePath,
			DBOptions dbOptions,
			ColumnFamilyOptions columnFamilyOptions,
			TaskKvStateRegistry kvStateRegistry,
			TypeSerializer<K> keySerializer,
			int numberOfKeyGroups,
			KeyGroupRange keyGroupRange,
			ExecutionConfig executionConfig
	) throws IOException {

		super(kvStateRegistry, keySerializer, userCodeClassLoader, numberOfKeyGroups, keyGroupRange, executionConfig);
		this.columnOptions = Preconditions.checkNotNull(columnFamilyOptions);
		this.dbOptions = Preconditions.checkNotNull(dbOptions);

		this.instanceBasePath = Preconditions.checkNotNull(instanceBasePath);
		this.instanceRocksDBPath = new File(instanceBasePath, "db");

		if (!instanceBasePath.exists()) {
			if (!instanceBasePath.mkdirs()) {
				throw new IOException("Could not create RocksDB data directory.");
			}
		}

		// clean it, this will remove the last part of the path but RocksDB will recreate it
		try {
			if (instanceRocksDBPath.exists()) {
				LOG.warn("Deleting already existing db directory {}.", instanceRocksDBPath);
				FileUtils.deleteDirectory(instanceRocksDBPath);
			}
		} catch (IOException e) {
			throw new IOException("Error cleaning RocksDB data directory.", e);
		}

		List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>(1);
		// RocksDB seems to need this...
		columnFamilyDescriptors.add(new ColumnFamilyDescriptor("default".getBytes(ConfigConstants.DEFAULT_CHARSET)));
		List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(1);
		try {

			db = RocksDB.open(
					Preconditions.checkNotNull(dbOptions),
					instanceRocksDBPath.getAbsolutePath(),
					columnFamilyDescriptors,
					columnFamilyHandles);

		} catch (RocksDBException e) {
			throw new IOException("Error while opening RocksDB instance.", e);
		}
		keyGroupPrefixBytes = getNumberOfKeyGroups() > (Byte.MAX_VALUE + 1) ? 2 : 1;
		kvStateInformation = new HashMap<>();
	}

	/**
	 * Should only be called by one thread, and only after all accesses to the DB happened.
	 */
	@Override
	public void dispose() {
		super.dispose();

		// Acquire the lock, so that no ongoing snapshots access the db during cleanup
		synchronized (asyncSnapshotLock) {
			// IMPORTANT: null reference to signal potential async checkpoint workers that the db was disposed, as
			// working on the disposed object results in SEGFAULTS. Other code has to check field #db for null
			// and access it in a synchronized block that locks on #dbDisposeLock.
			if (db != null) {

				for (Tuple2<ColumnFamilyHandle, RegisteredBackendStateMetaInfo<?, ?>> column :
						kvStateInformation.values()) {
					try {
						column.f0.close();
					} catch (Exception ex) {
						LOG.info("Exception while closing ColumnFamilyHandle object.", ex);
					}
				}

				kvStateInformation.clear();

				try {
					db.close();
				} catch (Exception ex) {
					LOG.info("Exception while closing RocksDB object.", ex);
				}

				db = null;
			}
		}

		IOUtils.closeQuietly(columnOptions);
		IOUtils.closeQuietly(dbOptions);

		try {
			FileUtils.deleteDirectory(instanceBasePath);
		} catch (IOException ioex) {
			LOG.info("Could not delete instace base path for RocksDB: " + instanceBasePath, ioex);
		}
	}

	public int getKeyGroupPrefixBytes() {
		return keyGroupPrefixBytes;
	}

	private boolean hasRegisteredState() {
		return !kvStateInformation.isEmpty();
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
	 * @throws Exception
	 */
	@Override
	public RunnableFuture<KeyedStateHandle> snapshot(
			final long checkpointId,
			final long timestamp,
			final CheckpointStreamFactory streamFactory,
			CheckpointOptions checkpointOptions) throws Exception {

		long startTime = System.currentTimeMillis();

		final RocksDBSnapshotOperation snapshotOperation = new RocksDBSnapshotOperation(this, streamFactory);
		// hold the db lock while operation on the db to guard us against async db disposal
		synchronized (asyncSnapshotLock) {

			if (db != null) {

				if (!hasRegisteredState()) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Asynchronous RocksDB snapshot performed on empty keyed state at " + timestamp +
								" . Returning null.");
					}
					return DoneFuture.nullValue();
				}

				snapshotOperation.takeDBSnapShot(checkpointId, timestamp);
			} else {
				throw new IOException("RocksDB closed.");
			}
		}

		// implementation of the async IO operation, based on FutureTask
		AbstractAsyncIOCallable<KeyedStateHandle, CheckpointStreamFactory.CheckpointStateOutputStream> ioCallable =
				new AbstractAsyncIOCallable<KeyedStateHandle, CheckpointStreamFactory.CheckpointStateOutputStream>() {

					@Override
					public CheckpointStreamFactory.CheckpointStateOutputStream openIOHandle() throws Exception {
						snapshotOperation.openCheckpointStream();
						return snapshotOperation.getOutStream();
					}

					@Override
					public KeyGroupsStateHandle performOperation() throws Exception {
						long startTime = System.currentTimeMillis();
						synchronized (asyncSnapshotLock) {
							try {
								// hold the db lock while operation on the db to guard us against async db disposal
								if (db == null) {
									throw new IOException("RocksDB closed.");
								}

								snapshotOperation.writeDBSnapshot();

							} finally {
								snapshotOperation.closeCheckpointStream();
							}
						}

						LOG.info("Asynchronous RocksDB snapshot ({}, asynchronous part) in thread {} took {} ms.",
							streamFactory, Thread.currentThread(), (System.currentTimeMillis() - startTime));

						return snapshotOperation.getSnapshotResultStateHandle();
					}

					private void releaseSnapshotOperationResources(boolean canceled) {
						// hold the db lock while operation on the db to guard us against async db disposal
						synchronized (asyncSnapshotLock) {
							snapshotOperation.releaseSnapshotResources(canceled);
						}
					}

					@Override
					public void done(boolean canceled) {
						releaseSnapshotOperationResources(canceled);
					}
				};

		LOG.info("Asynchronous RocksDB snapshot (" + streamFactory + ", synchronous part) in thread " +
				Thread.currentThread() + " took " + (System.currentTimeMillis() - startTime) + " ms.");

		return AsyncStoppableTaskWithCallback.from(ioCallable);
	}

	/**
	 * Encapsulates the process to perform a snapshot of a RocksDBKeyedStateBackend.
	 */
	static final class RocksDBSnapshotOperation {

		static final int FIRST_BIT_IN_BYTE_MASK = 0x80;
		static final int END_OF_KEY_GROUP_MARK = 0xFFFF;

		private final RocksDBKeyedStateBackend<?> stateBackend;
		private final KeyGroupRangeOffsets keyGroupRangeOffsets;
		private final CheckpointStreamFactory checkpointStreamFactory;

		private long checkpointId;
		private long checkpointTimeStamp;

		private Snapshot snapshot;
		private ReadOptions readOptions;
		private List<Tuple2<RocksIterator, Integer>> kvStateIterators;

		private CheckpointStreamFactory.CheckpointStateOutputStream outStream;
		private DataOutputView outputView;
		private KeyGroupsStateHandle snapshotResultStateHandle;

		RocksDBSnapshotOperation(
				RocksDBKeyedStateBackend<?> stateBackend,
				CheckpointStreamFactory checkpointStreamFactory) {

			this.stateBackend = stateBackend;
			this.checkpointStreamFactory = checkpointStreamFactory;
			this.keyGroupRangeOffsets = new KeyGroupRangeOffsets(stateBackend.keyGroupRange);
		}

		/**
		 * 1) Create a snapshot object from RocksDB.
		 *
		 * @param checkpointId id of the checkpoint for which we take the snapshot
		 * @param checkpointTimeStamp timestamp of the checkpoint for which we take the snapshot
		 */
		public void takeDBSnapShot(long checkpointId, long checkpointTimeStamp) {
			Preconditions.checkArgument(snapshot == null, "Only one ongoing snapshot allowed!");
			this.kvStateIterators = new ArrayList<>(stateBackend.kvStateInformation.size());
			this.checkpointId = checkpointId;
			this.checkpointTimeStamp = checkpointTimeStamp;
			this.snapshot = stateBackend.db.getSnapshot();
		}

		/**
		 * 2) Open CheckpointStateOutputStream through the checkpointStreamFactory into which we will write.
		 *
		 * @throws Exception
		 */
		public void openCheckpointStream() throws Exception {
			Preconditions.checkArgument(outStream == null, "Output stream for snapshot is already set.");
			outStream = checkpointStreamFactory.
					createCheckpointStateOutputStream(checkpointId, checkpointTimeStamp);
			stateBackend.cancelStreamRegistry.registerClosable(outStream);
			outputView = new DataOutputViewStreamWrapper(outStream);
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

			Preconditions.checkNotNull(outStream, "No output stream to write snapshot.");
			writeKVStateMetaData();
			writeKVStateData();
		}

		/**
		 * 4) Close the CheckpointStateOutputStream after writing and receive a state handle.
		 *
		 * @throws IOException
		 */
		public void closeCheckpointStream() throws IOException {
			if (outStream != null) {
				snapshotResultStateHandle = closeSnapshotStreamAndGetHandle();
			} else {
				snapshotResultStateHandle = null;
			}
		}

		/**
		 * 5) Release the snapshot object for RocksDB and clean up.
		 */
		public void releaseSnapshotResources(boolean canceled) {

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

			if (canceled) {
				try {
					if (null != snapshotResultStateHandle) {
						snapshotResultStateHandle.discardState();
					}
				} catch (Exception e) {
					LOG.warn("Exception occurred during snapshot state handle cleanup.", e);
				}
			}
		}

		/**
		 * Returns the current CheckpointStateOutputStream (when it was opened and not yet closed) into which we write
		 * the state snapshot.
		 *
		 * @return the current CheckpointStateOutputStream
		 */
		public CheckpointStreamFactory.CheckpointStateOutputStream getOutStream() {
			return outStream;
		}

		/**
		 * Returns a state handle to the snapshot after the snapshot procedure is completed and null before.
		 *
		 * @return state handle to the completed snapshot
		 */
		public KeyGroupsStateHandle getSnapshotResultStateHandle() {
			return snapshotResultStateHandle;
		}

		private void writeKVStateMetaData() throws IOException {

			List<KeyedBackendSerializationProxy.StateMetaInfo<?, ?>> metaInfoList =
					new ArrayList<>(stateBackend.kvStateInformation.size());

			int kvStateId = 0;
			for (Map.Entry<String, Tuple2<ColumnFamilyHandle, RegisteredBackendStateMetaInfo<?, ?>>> column :
					stateBackend.kvStateInformation.entrySet()) {

				RegisteredBackendStateMetaInfo<?, ?> metaInfo = column.getValue().f1;

				KeyedBackendSerializationProxy.StateMetaInfo<?, ?> metaInfoProxy =
						new KeyedBackendSerializationProxy.StateMetaInfo<>(
								metaInfo.getStateType(),
								metaInfo.getName(),
								metaInfo.getNamespaceSerializer(),
								metaInfo.getStateSerializer());

				metaInfoList.add(metaInfoProxy);

				//retrieve iterator for this k/v states
				readOptions = new ReadOptions();
				readOptions.setSnapshot(snapshot);

				kvStateIterators.add(
						new Tuple2<>(stateBackend.db.newIterator(column.getValue().f0, readOptions), kvStateId));

				++kvStateId;
			}

			KeyedBackendSerializationProxy serializationProxy =
					new KeyedBackendSerializationProxy(stateBackend.getKeySerializer(), metaInfoList);

			serializationProxy.write(outputView);
		}

		private void writeKVStateData() throws IOException, InterruptedException {

			byte[] previousKey = null;
			byte[] previousValue = null;

			// Here we transfer ownership of RocksIterators to the RocksDBMergeIterator
			try (RocksDBMergeIterator mergeIterator = new RocksDBMergeIterator(
					kvStateIterators, stateBackend.keyGroupPrefixBytes)) {

				// handover complete, null out to prevent double close
				kvStateIterators = null;

				//preamble: setup with first key-group as our lookahead
				if (mergeIterator.isValid()) {
					//begin first key-group by recording the offset
					keyGroupRangeOffsets.setKeyGroupOffset(mergeIterator.keyGroup(), outStream.getPos());
					//write the k/v-state id as metadata
					//TODO this could be aware of keyGroupPrefixBytes and write only one byte if possible
					outputView.writeShort(mergeIterator.kvStateId());
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

					writeKeyValuePair(previousKey, previousValue);

					//write meta data if we have to
					if (mergeIterator.isNewKeyGroup()) {
						//TODO this could be aware of keyGroupPrefixBytes and write only one byte if possible
						outputView.writeShort(END_OF_KEY_GROUP_MARK);
						//begin new key-group
						keyGroupRangeOffsets.setKeyGroupOffset(mergeIterator.keyGroup(), outStream.getPos());
						//write the kev-state
						//TODO this could be aware of keyGroupPrefixBytes and write only one byte if possible
						outputView.writeShort(mergeIterator.kvStateId());
					} else if (mergeIterator.isNewKeyValueState()) {
						//write the k/v-state
						//TODO this could be aware of keyGroupPrefixBytes and write only one byte if possible
						outputView.writeShort(mergeIterator.kvStateId());
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
				writeKeyValuePair(previousKey, previousValue);
				//TODO this could be aware of keyGroupPrefixBytes and write only one byte if possible
				outputView.writeShort(END_OF_KEY_GROUP_MARK);
			}
		}

		private KeyGroupsStateHandle closeSnapshotStreamAndGetHandle() throws IOException {
			stateBackend.cancelStreamRegistry.unregisterClosable(outStream);
			StreamStateHandle stateHandle = outStream.closeAndGetHandle();
			outStream = null;
			return stateHandle != null ? new KeyGroupsStateHandle(keyGroupRangeOffsets, stateHandle) : null;
		}

		private void writeKeyValuePair(byte[] key, byte[] value) throws IOException {
			BytePrimitiveArraySerializer.INSTANCE.serialize(key, outputView);
			BytePrimitiveArraySerializer.INSTANCE.serialize(value, outputView);
		}

		static void setMetaDataFollowsFlagInKey(byte[] key) {
			key[0] |= FIRST_BIT_IN_BYTE_MASK;
		}

		static void clearMetaDataFollowsFlag(byte[] key) {
			key[0] &= (~RocksDBSnapshotOperation.FIRST_BIT_IN_BYTE_MASK);
		}

		static boolean hasMetaDataFollowsFlag(byte[] key) {
			return 0 != (key[0] & RocksDBSnapshotOperation.FIRST_BIT_IN_BYTE_MASK);
		}

		private static void checkInterrupted() throws InterruptedException {
			if (Thread.currentThread().isInterrupted()) {
				throw new InterruptedException("RocksDB snapshot interrupted.");
			}
		}
	}

	@Override
	public void restore(Collection<KeyedStateHandle> restoreState) throws Exception {
		LOG.info("Initializing RocksDB keyed state backend from snapshot.");

		if (LOG.isDebugEnabled()) {
			LOG.debug("Restoring snapshot from state handles: {}.", restoreState);
		}

		try {
			if (MigrationUtil.isOldSavepointKeyedState(restoreState)) {
				LOG.info("Converting RocksDB state from old savepoint.");
				restoreOldSavepointKeyedState(restoreState);
			} else {
				RocksDBRestoreOperation restoreOperation = new RocksDBRestoreOperation(this);
				restoreOperation.doRestore(restoreState);
			}
		} catch (Exception ex) {
			dispose();
			throw ex;
		}
	}

	/**
	 * Encapsulates the process of restoring a RocksDBKeyedStateBackend from a snapshot.
	 */
	static final class RocksDBRestoreOperation {

		private final RocksDBKeyedStateBackend<?> rocksDBKeyedStateBackend;

		/** Current key-groups state handle from which we restore key-groups */
		private KeyGroupsStateHandle currentKeyGroupsStateHandle;
		/** Current input stream we obtained from currentKeyGroupsStateHandle */
		private FSDataInputStream currentStateHandleInStream;
		/** Current data input view that wraps currentStateHandleInStream */
		private DataInputView currentStateHandleInView;
		/** Current list of ColumnFamilyHandles for all column families we restore from currentKeyGroupsStateHandle */
		private List<ColumnFamilyHandle> currentStateHandleKVStateColumnFamilies;

		/**
		 * Creates a restore operation object for the given state backend instance.
		 *
		 * @param rocksDBKeyedStateBackend the state backend into which we restore
		 */
		public RocksDBRestoreOperation(RocksDBKeyedStateBackend<?> rocksDBKeyedStateBackend) {
			this.rocksDBKeyedStateBackend = Preconditions.checkNotNull(rocksDBKeyedStateBackend);
		}

		/**
		 * Restores all key-groups data that is referenced by the passed state handles.
		 *
		 * @param keyedStateHandles List of all key groups state handles that shall be restored.
		 * @throws IOException
		 * @throws ClassNotFoundException
		 * @throws RocksDBException
		 */
		public void doRestore(Collection<KeyedStateHandle> keyedStateHandles)
				throws IOException, ClassNotFoundException, RocksDBException {

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
		 * Restore one key groups state handle
		 *
		 * @throws IOException
		 * @throws RocksDBException
		 * @throws ClassNotFoundException
		 */
		private void restoreKeyGroupsInStateHandle()
				throws IOException, RocksDBException, ClassNotFoundException {
			try {
				currentStateHandleInStream = currentKeyGroupsStateHandle.openInputStream();
				rocksDBKeyedStateBackend.cancelStreamRegistry.registerClosable(currentStateHandleInStream);
				currentStateHandleInView = new DataInputViewStreamWrapper(currentStateHandleInStream);
				restoreKVStateMetaData();
				restoreKVStateData();
			} finally {
				if (currentStateHandleInStream != null) {
					rocksDBKeyedStateBackend.cancelStreamRegistry.unregisterClosable(currentStateHandleInStream);
					IOUtils.closeQuietly(currentStateHandleInStream);
				}
			}
		}

		/**
		 * Restore the KV-state / ColumnFamily meta data for all key-groups referenced by the current state handle
		 *
		 * @throws IOException
		 * @throws ClassNotFoundException
		 * @throws RocksDBException
		 */
		private void restoreKVStateMetaData() throws IOException, ClassNotFoundException, RocksDBException {

			KeyedBackendSerializationProxy serializationProxy =
					new KeyedBackendSerializationProxy(rocksDBKeyedStateBackend.userCodeClassLoader);

			serializationProxy.read(currentStateHandleInView);

			List<KeyedBackendSerializationProxy.StateMetaInfo<?, ?>> metaInfoProxyList =
					serializationProxy.getNamedStateSerializationProxies();

			currentStateHandleKVStateColumnFamilies = new ArrayList<>(metaInfoProxyList.size());

			for (KeyedBackendSerializationProxy.StateMetaInfo<?, ?> metaInfoProxy : metaInfoProxyList) {
				Tuple2<ColumnFamilyHandle, RegisteredBackendStateMetaInfo<?, ?>> columnFamily =
						rocksDBKeyedStateBackend.kvStateInformation.get(metaInfoProxy.getStateName());

				if (null == columnFamily) {
					ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(
						metaInfoProxy.getStateName().getBytes(ConfigConstants.DEFAULT_CHARSET),
						rocksDBKeyedStateBackend.columnOptions);

					RegisteredBackendStateMetaInfo<?, ?> stateMetaInfo =
							new RegisteredBackendStateMetaInfo<>(metaInfoProxy);

					columnFamily = new Tuple2<ColumnFamilyHandle, RegisteredBackendStateMetaInfo<?, ?>>(
							rocksDBKeyedStateBackend.db.createColumnFamily(columnFamilyDescriptor),
							stateMetaInfo);

					rocksDBKeyedStateBackend.kvStateInformation.put(stateMetaInfo.getName(), columnFamily);
				} else {
					//TODO we could check here for incompatible serializer versions between previous tasks
				}

				currentStateHandleKVStateColumnFamilies.add(columnFamily.f0);
			}
		}

		/**
		 * Restore the KV-state / ColumnFamily data for all key-groups referenced by the current state handle
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
					boolean keyGroupHasMoreKeys = true;
					//TODO this could be aware of keyGroupPrefixBytes and write only one byte if possible
					int kvStateId = currentStateHandleInView.readShort();
					ColumnFamilyHandle handle = currentStateHandleKVStateColumnFamilies.get(kvStateId);
					//insert all k/v pairs into DB
					while (keyGroupHasMoreKeys) {
						byte[] key = BytePrimitiveArraySerializer.INSTANCE.deserialize(currentStateHandleInView);
						byte[] value = BytePrimitiveArraySerializer.INSTANCE.deserialize(currentStateHandleInView);
						if (RocksDBSnapshotOperation.hasMetaDataFollowsFlag(key)) {
							//clear the signal bit in the key to make it ready for insertion again
							RocksDBSnapshotOperation.clearMetaDataFollowsFlag(key);
							rocksDBKeyedStateBackend.db.put(handle, key, value);
							//TODO this could be aware of keyGroupPrefixBytes and write only one byte if possible
							kvStateId = RocksDBSnapshotOperation.END_OF_KEY_GROUP_MARK
									& currentStateHandleInView.readShort();
							if (RocksDBSnapshotOperation.END_OF_KEY_GROUP_MARK == kvStateId) {
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

	// ------------------------------------------------------------------------
	//  State factories
	// ------------------------------------------------------------------------

	/**
	 * Creates a column family handle for use with a k/v state. When restoring from a snapshot
	 * we don't restore the individual k/v states, just the global RocksDB data base and the
	 * list of column families. When a k/v state is first requested we check here whether we
	 * already have a column family for that and return it or create a new one if it doesn't exist.
	 *
	 * <p>This also checks whether the {@link StateDescriptor} for a state matches the one
	 * that we checkpointed, i.e. is already in the map of column families.
	 */
	@SuppressWarnings("rawtypes, unchecked")
	protected <N, S> ColumnFamilyHandle getColumnFamily(
			StateDescriptor<?, S> descriptor, TypeSerializer<N> namespaceSerializer) throws IOException {

		Tuple2<ColumnFamilyHandle, RegisteredBackendStateMetaInfo<?, ?>> stateInfo =
				kvStateInformation.get(descriptor.getName());

		RegisteredBackendStateMetaInfo<N, S> newMetaInfo = new RegisteredBackendStateMetaInfo<>(
				descriptor.getType(),
				descriptor.getName(),
				namespaceSerializer,
				descriptor.getSerializer());

		if (stateInfo != null) {
			if (newMetaInfo.canRestoreFrom(stateInfo.f1)) {
				stateInfo.f1 = newMetaInfo;
				return stateInfo.f0;
			} else {
				throw new IOException("Trying to access state using wrong meta info, was " + stateInfo.f1 +
						" trying access with " + newMetaInfo);
			}
		}

		ColumnFamilyDescriptor columnDescriptor = new ColumnFamilyDescriptor(
				descriptor.getName().getBytes(ConfigConstants.DEFAULT_CHARSET), columnOptions);

		try {
			ColumnFamilyHandle columnFamily = db.createColumnFamily(columnDescriptor);
			Tuple2<ColumnFamilyHandle, RegisteredBackendStateMetaInfo<N, S>> tuple =
					new Tuple2<>(columnFamily, newMetaInfo);
			Map rawAccess = kvStateInformation;
			rawAccess.put(descriptor.getName(), tuple);
			return columnFamily;
		} catch (RocksDBException e) {
			throw new IOException("Error creating ColumnFamilyHandle.", e);
		}
	}

	@Override
	protected <N, T> InternalValueState<N, T> createValueState(
			TypeSerializer<N> namespaceSerializer,
			ValueStateDescriptor<T> stateDesc) throws Exception {

		ColumnFamilyHandle columnFamily = getColumnFamily(stateDesc, namespaceSerializer);

		return new RocksDBValueState<>(columnFamily, namespaceSerializer,  stateDesc, this);
	}

	@Override
	protected <N, T> InternalListState<N, T> createListState(
			TypeSerializer<N> namespaceSerializer,
			ListStateDescriptor<T> stateDesc) throws Exception {

		ColumnFamilyHandle columnFamily = getColumnFamily(stateDesc, namespaceSerializer);

		return new RocksDBListState<>(columnFamily, namespaceSerializer, stateDesc, this);
	}

	@Override
	protected <N, T> InternalReducingState<N, T> createReducingState(
			TypeSerializer<N> namespaceSerializer,
			ReducingStateDescriptor<T> stateDesc) throws Exception {

		ColumnFamilyHandle columnFamily = getColumnFamily(stateDesc, namespaceSerializer);

		return new RocksDBReducingState<>(columnFamily, namespaceSerializer,  stateDesc, this);
	}

	@Override
	protected <N, T, ACC, R> InternalAggregatingState<N, T, R> createAggregatingState(
			TypeSerializer<N> namespaceSerializer,
			AggregatingStateDescriptor<T, ACC, R> stateDesc) throws Exception {

		ColumnFamilyHandle columnFamily = getColumnFamily(stateDesc, namespaceSerializer);
		return new RocksDBAggregatingState<>(columnFamily, namespaceSerializer, stateDesc, this);
	}

	@Override
	protected <N, T, ACC> InternalFoldingState<N, T, ACC> createFoldingState(
			TypeSerializer<N> namespaceSerializer,
			FoldingStateDescriptor<T, ACC> stateDesc) throws Exception {

		ColumnFamilyHandle columnFamily = getColumnFamily(stateDesc, namespaceSerializer);

		return new RocksDBFoldingState<>(columnFamily, namespaceSerializer, stateDesc, this);
	}

	@Override
	protected <N, UK, UV> InternalMapState<N, UK, UV> createMapState(TypeSerializer<N> namespaceSerializer,
			MapStateDescriptor<UK, UV> stateDesc) throws Exception {
		ColumnFamilyHandle columnFamily = getColumnFamily(stateDesc, namespaceSerializer);

		return new RocksDBMapState<>(columnFamily, namespaceSerializer, stateDesc, this);
	}

	/**
	 * Wraps a RocksDB iterator to cache it's current key and assign an id for the key/value state to the iterator.
	 * Used by #MergeIterator.
	 */
	static final class MergeIterator implements AutoCloseable {

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
	 * Iterator that merges multiple RocksDB iterators to partition all states into contiguous key-groups.
	 * The resulting iteration sequence is ordered by (key-group, kv-state).
	 */
	static final class RocksDBMergeIterator implements AutoCloseable {

		private final PriorityQueue<MergeIterator> heap;
		private final int keyGroupPrefixByteCount;
		private boolean newKeyGroup;
		private boolean newKVState;
		private boolean valid;

		private MergeIterator currentSubIterator;

		private static final List<Comparator<MergeIterator>> COMPARATORS;

		static {
			int maxBytes = 4;
			COMPARATORS = new ArrayList<>(maxBytes);
			for (int i = 0; i < maxBytes; ++i) {
				final int currentBytes = i;
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
			this.keyGroupPrefixByteCount = keyGroupPrefixByteCount;

			Comparator<MergeIterator> iteratorComparator = COMPARATORS.get(keyGroupPrefixByteCount);

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
		 * Returns the key-group for the current key.
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
		 * Returns the Id of the k/v state to which the current key belongs.
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
	 * Only visible for testing, DO NOT USE.
	 */
	public File getInstanceBasePath() {
		return instanceBasePath;
	}

	/**
	 * For backwards compatibility, remove again later!
	 *
	 * @deprecated Internal method used for backwards compatibility.
	 */
	@Deprecated
	private void restoreOldSavepointKeyedState(Collection<KeyedStateHandle> restoreState) throws Exception {

		if (restoreState.isEmpty()) {
			return;
		}

		Preconditions.checkState(1 == restoreState.size(), "Only one element expected here.");

		KeyedStateHandle keyedStateHandle = restoreState.iterator().next();
		if (!(keyedStateHandle instanceof MigrationKeyGroupStateHandle)) {
			throw new IllegalStateException("Unexpected state handle type, " +
					"expected: " + MigrationKeyGroupStateHandle.class +
					", but found: " + keyedStateHandle.getClass());
		}

		MigrationKeyGroupStateHandle keyGroupStateHandle = (MigrationKeyGroupStateHandle) keyedStateHandle;

		HashMap<String, RocksDBStateBackend.FinalFullyAsyncSnapshot> namedStates;
		try (FSDataInputStream inputStream = keyGroupStateHandle.openInputStream()) {
			namedStates = InstantiationUtil.deserializeObject(inputStream, userCodeClassLoader);
		}

		Preconditions.checkState(1 == namedStates.size(), "Only one element expected here.");
		DataInputView inputView = namedStates.values().iterator().next().stateHandle.getState(userCodeClassLoader);

		// clear k/v state information before filling it
		kvStateInformation.clear();

		// first get the column family mapping
		int numColumns = inputView.readInt();
		Map<Byte, StateDescriptor<?, ?>> columnFamilyMapping = new HashMap<>(numColumns);
		for (int i = 0; i < numColumns; i++) {
			byte mappingByte = inputView.readByte();

			ObjectInputStream ooIn =
					new InstantiationUtil.ClassLoaderObjectInputStream(
							new DataInputViewStream(inputView), userCodeClassLoader);

			StateDescriptor stateDescriptor = (StateDescriptor) ooIn.readObject();

			columnFamilyMapping.put(mappingByte, stateDescriptor);

			// this will fill in the k/v state information
			getColumnFamily(stateDescriptor, MigrationNamespaceSerializerProxy.INSTANCE);
		}

		// try and read until EOF
		try {
			// the EOFException will get us out of this...
			while (true) {
				byte mappingByte = inputView.readByte();
				ColumnFamilyHandle handle = getColumnFamily(
						columnFamilyMapping.get(mappingByte), MigrationNamespaceSerializerProxy.INSTANCE);

				byte[] keyAndNamespace = BytePrimitiveArraySerializer.INSTANCE.deserialize(inputView);

				ByteArrayInputStreamWithPos bis = new ByteArrayInputStreamWithPos(keyAndNamespace);

				K reconstructedKey = keySerializer.deserialize(new DataInputViewStreamWrapper(bis));
				int len = bis.getPosition();

				int keyGroup = (byte) KeyGroupRangeAssignment.assignToKeyGroup(reconstructedKey, numberOfKeyGroups);

				if (keyGroupPrefixBytes == 1) {
					// copy and override one byte (42) between key and namespace
					System.arraycopy(keyAndNamespace, 0, keyAndNamespace, 1, len);
					keyAndNamespace[0] = (byte) keyGroup;
				} else {
					byte[] largerKey = new byte[1 + keyAndNamespace.length];

					// write key-group
					largerKey[0] = (byte) ((keyGroup >> 8) & 0xFF);
					largerKey[1] = (byte) (keyGroup & 0xFF);

					// write key
					System.arraycopy(keyAndNamespace, 0, largerKey, 2, len);

					//skip one byte (42), write namespace
					System.arraycopy(keyAndNamespace, 1 + len, largerKey, 2 + len, keyAndNamespace.length - len - 1);
					keyAndNamespace = largerKey;
				}

				byte[] value = BytePrimitiveArraySerializer.INSTANCE.deserialize(inputView);
				db.put(handle, keyAndNamespace, value);
			}
		} catch (EOFException e) {
			// expected
		}
	}

	@Override
	public boolean supportsAsynchronousSnapshots() {
		return true;
	}
}
