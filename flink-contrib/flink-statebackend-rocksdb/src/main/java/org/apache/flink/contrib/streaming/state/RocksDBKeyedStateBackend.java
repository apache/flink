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

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.io.async.AbstractAsyncIOCallable;
import org.apache.flink.runtime.io.async.AsyncStoppableTaskWithCallback;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.DoneFuture;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.util.SerializableObject;
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

import javax.annotation.concurrent.GuardedBy;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.RunnableFuture;

/**
 * A {@link KeyedStateBackend} that stores its state in {@code RocksDB} and will serialize state to
 * streams provided by a {@link org.apache.flink.runtime.state.CheckpointStreamFactory} upon
 * checkpointing. This state backend can store very large state that exceeds memory and spills
 * to disk.
 */
public class RocksDBKeyedStateBackend<K> extends KeyedStateBackend<K> {

	private static final Logger LOG = LoggerFactory.getLogger(RocksDBKeyedStateBackend.class);

	/** Operator identifier that is used to uniqueify the RocksDB storage path. */
	private final String operatorIdentifier;

	/** JobID for uniquifying backup paths. */
	private final JobID jobId;

	/** The options from the options factory, cached */
	private final ColumnFamilyOptions columnOptions;

	/** Path where this configured instance stores its data directory */
	private final File instanceBasePath;

	/** Path where this configured instance stores its RocksDB data base */
	private final File instanceRocksDBPath;

	/**
	 * Lock for protecting cleanup of the RocksDB db. We acquire this when doing asynchronous
	 * checkpoints and when disposing the db. Otherwise, the asynchronous snapshot might try
	 * iterating over a disposed db.
	 */
	private final SerializableObject dbDisposeLock = new SerializableObject();

	/**
	 * Our RocksDB data base, this is used by the actual subclasses of {@link AbstractRocksDBState}
	 * to store state. The different k/v states that we have don't each have their own RocksDB
	 * instance. They all write to this instance but to their own column family.
	 */
	@GuardedBy("dbDisposeLock")
	protected volatile RocksDB db;

	/**
	 * Information about the k/v states as we create them. This is used to retrieve the
	 * column family that is used for a state and also for sanity checks when restoring.
	 */
	private Map<String, Tuple2<ColumnFamilyHandle, StateDescriptor>> kvStateInformation;

	/** Number of bytes required to prefix the key groups. */
	private final int keyGroupPrefixBytes;

	public RocksDBKeyedStateBackend(
			JobID jobId,
			String operatorIdentifier,
			File instanceBasePath,
			DBOptions dbOptions,
			ColumnFamilyOptions columnFamilyOptions,
			TaskKvStateRegistry kvStateRegistry,
			TypeSerializer<K> keySerializer,
			int numberOfKeyGroups,
			KeyGroupRange keyGroupRange
	) throws Exception {

		super(kvStateRegistry, keySerializer, numberOfKeyGroups, keyGroupRange);

		this.operatorIdentifier = operatorIdentifier;
		this.jobId = jobId;
		this.columnOptions = columnFamilyOptions;

		this.instanceBasePath = instanceBasePath;
		this.instanceRocksDBPath = new File(instanceBasePath, "db");

		RocksDB.loadLibrary();

		if (!instanceBasePath.exists()) {
			if (!instanceBasePath.mkdirs()) {
				throw new RuntimeException("Could not create RocksDB data directory.");
			}
		}

		// clean it, this will remove the last part of the path but RocksDB will recreate it
		try {
			if (instanceRocksDBPath.exists()) {
				LOG.warn("Deleting already existing db directory {}.", instanceRocksDBPath);
				FileUtils.deleteDirectory(instanceRocksDBPath);
			}
		} catch (IOException e) {
			throw new RuntimeException("Error cleaning RocksDB data directory.", e);
		}

		List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>(1);
		// RocksDB seems to need this...
		columnFamilyDescriptors.add(new ColumnFamilyDescriptor("default".getBytes()));
		List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(1);
		try {
			db = RocksDB.open(dbOptions, instanceRocksDBPath.getAbsolutePath(), columnFamilyDescriptors, columnFamilyHandles);
		} catch (RocksDBException e) {
			throw new RuntimeException("Error while opening RocksDB instance.", e);
		}
		keyGroupPrefixBytes = getNumberOfKeyGroups() > (Byte.MAX_VALUE + 1) ? 2 : 1;
		kvStateInformation = new HashMap<>();
	}

	public RocksDBKeyedStateBackend(
			JobID jobId,
			String operatorIdentifier,
			File instanceBasePath,
			DBOptions dbOptions,
			ColumnFamilyOptions columnFamilyOptions,
			TaskKvStateRegistry kvStateRegistry,
			TypeSerializer<K> keySerializer,
			int numberOfKeyGroups,
			KeyGroupRange keyGroupRange,
			List<KeyGroupsStateHandle> restoreState
	) throws Exception {
		this(
			jobId,
			operatorIdentifier,
			instanceBasePath,
			dbOptions,
			columnFamilyOptions,
			kvStateRegistry,
			keySerializer,
			numberOfKeyGroups,
			keyGroupRange);

		LOG.info("Initializing RocksDB keyed state backend from snapshot.");

		if (LOG.isDebugEnabled()) {
			LOG.debug("Restoring snapshot from state handles: {}.", restoreState);
		}

		RocksDBRestoreOperation restoreOperation = new RocksDBRestoreOperation(this);
		restoreOperation.doRestore(restoreState);
	}

	/**
	 * @see java.io.Closeable
	 *
	 * Should only be called by one thread.
	 *
	 * @throws Exception
	 */
	@Override
	public void close() throws Exception {
		super.close();

		final RocksDB cleanupRockDBReference;

		// Acquire the log on dbDisposeLock, so that no ongoing snapshots access the db during cleanup
		synchronized (dbDisposeLock) {
			// IMPORTANT: null reference to signal potential async checkpoint workers that the db was disposed, as
			// working on the disposed object results in SEGFAULTS. Other code has to check field #db for null
			// and access it in a synchronized block that locks on #dbDisposeLock.
			cleanupRockDBReference = db;
			db = null;
		}

		// Dispose decoupled db
		if (cleanupRockDBReference != null) {
			for (Tuple2<ColumnFamilyHandle, StateDescriptor> column : kvStateInformation.values()) {
				column.f0.dispose();
			}
			cleanupRockDBReference.dispose();
		}

		FileUtils.deleteDirectory(instanceBasePath);
	}

	public int getKeyGroupPrefixBytes() {
		return keyGroupPrefixBytes;
	}

	/**
	 * Triggers an asynchronous snapshot of the keyed state backend from RocksDB. This snapshot can be canceled and
	 * is also stopped when the backend is closed through {@link #close()}. For each backend, this method must always
	 * be called by the same thread.
	 *
	 * @param checkpointId The Id of the checkpoint.
	 * @param timestamp The timestamp of the checkpoint.
	 * @param streamFactory The factory that we can use for writing our state to streams.
	 *
	 * @return Future to the state handle of the snapshot data.
	 * @throws Exception
	 */
	@Override
	public RunnableFuture<KeyGroupsStateHandle> snapshot(
			final long checkpointId,
			final long timestamp,
			final CheckpointStreamFactory streamFactory) throws Exception {

		long startTime = System.currentTimeMillis();

		if (kvStateInformation.isEmpty()) {
			LOG.info("Asynchronous RocksDB snapshot performed on empty keyed state at " + timestamp + " . Returning null.");
			return new DoneFuture<>(null);
		}

		final RocksDBSnapshotOperation snapshotOperation = new RocksDBSnapshotOperation(this, streamFactory);
		// hold the db lock while operation on the db to guard us against async db disposal
		synchronized (dbDisposeLock) {
			if (db != null) {
				snapshotOperation.takeDBSnapShot(checkpointId, timestamp);
			} else {
				throw new IOException("RocksDB closed.");
			}
		}

		// implementation of the async IO operation, based on FutureTask
		AbstractAsyncIOCallable<KeyGroupsStateHandle, CheckpointStreamFactory.CheckpointStateOutputStream> ioCallable =
				new AbstractAsyncIOCallable<KeyGroupsStateHandle, CheckpointStreamFactory.CheckpointStateOutputStream>() {

					@Override
					public CheckpointStreamFactory.CheckpointStateOutputStream openIOHandle() throws Exception {
						snapshotOperation.openCheckpointStream();
						return snapshotOperation.getOutStream();
					}

					@Override
					public KeyGroupsStateHandle performOperation() throws Exception {
						long startTime = System.currentTimeMillis();
						try {
							// hold the db lock while operation on the db to guard us against async db disposal
							synchronized (dbDisposeLock) {
								if (db != null) {
									snapshotOperation.writeDBSnapshot();
								} else {
									throw new IOException("RocksDB closed.");
								}
							}

						} finally {
							snapshotOperation.closeCheckpointStream();
						}

						LOG.info("Asynchronous RocksDB snapshot (" + streamFactory + ", asynchronous part) in thread " +
								Thread.currentThread() + " took " + (System.currentTimeMillis() - startTime) + " ms.");

						return snapshotOperation.getSnapshotResultStateHandle();
					}

					@Override
					public void done() {
						// hold the db lock while operation on the db to guard us against async db disposal
						synchronized (dbDisposeLock) {
							if (db != null) {
								snapshotOperation.releaseDBSnapshot();
							}
						}
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
		private CheckpointStreamFactory.CheckpointStateOutputStream outStream;
		private DataOutputView outputView;
		private List<Tuple2<RocksIterator, Integer>> kvStateIterators;
		private KeyGroupsStateHandle snapshotResultStateHandle;



		public RocksDBSnapshotOperation(
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
		public void takeDBSnapShot(long checkpointId, long checkpointTimeStamp) throws IOException {
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
			outputView = new DataOutputViewStreamWrapper(outStream);
		}

		/**
		 * 3) Write the actual data from RocksDB from the time we took the snapshot object in (1).
		 *
		 * @return
		 * @throws IOException
		 */
		public void writeDBSnapshot() throws IOException, InterruptedException {
			Preconditions.checkNotNull(snapshot, "No ongoing snapshot to write.");
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
			if(outStream != null) {
				snapshotResultStateHandle = closeSnapshotStreamAndGetHandle();
			}
		}

		/**
		 * 5) Release the snapshot object for RocksDB and clean up.
		 *
		 */
		public void releaseDBSnapshot() {
			Preconditions.checkNotNull(snapshot, "No ongoing snapshot to release.");
			stateBackend.db.releaseSnapshot(snapshot);
			snapshot = null;
			outStream = null;
			outputView = null;
			kvStateIterators = null;
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

		private void writeKVStateMetaData() throws IOException, InterruptedException {
			//write number of k/v states
			outputView.writeInt(stateBackend.kvStateInformation.size());

			int kvStateId = 0;
			//iterate all column families, where each column family holds one k/v state, to write the metadata
			for (Map.Entry<String, Tuple2<ColumnFamilyHandle, StateDescriptor>> column : stateBackend.kvStateInformation.entrySet()) {

				//be cooperative and check for interruption from time to time in the hot loop
				checkInterrupted();

				//write StateDescriptor for this k/v state
				ObjectOutputStream ooOut = new ObjectOutputStream(outStream);
				ooOut.writeObject(column.getValue().f1);
				//retrieve iterator for this k/v states
				ReadOptions readOptions = new ReadOptions();
				readOptions.setSnapshot(snapshot);
				RocksIterator iterator = stateBackend.db.newIterator(column.getValue().f0, readOptions);
				kvStateIterators.add(new Tuple2<RocksIterator, Integer>(iterator, kvStateId));
				++kvStateId;
			}
		}

		private void writeKVStateData() throws IOException, InterruptedException {

			RocksDBMergeIterator iterator = new RocksDBMergeIterator(kvStateIterators, stateBackend.keyGroupPrefixBytes);

			byte[] previousKey = null;
			byte[] previousValue = null;

			//preamble: setup with first key-group as our lookahead
			if (iterator.isValid()) {
				//begin first key-group by recording the offset
				keyGroupRangeOffsets.setKeyGroupOffset(iterator.keyGroup(), outStream.getPos());
				//write the k/v-state id as metadata
				//TODO this could be aware of keyGroupPrefixBytes and write only one byte if possible
				outputView.writeShort(iterator.kvStateId());
				previousKey = iterator.key();
				previousValue = iterator.value();
				iterator.next();
			}

			//main loop: write k/v pairs ordered by (key-group, kv-state), thereby tracking key-group offsets.
			while (iterator.isValid()) {

				assert (!hasMetaDataFollowsFlag(previousKey));

				//set signal in first key byte that meta data will follow in the stream after this k/v pair
				if (iterator.isNewKeyGroup() || iterator.isNewKeyValueState()) {

					//be cooperative and check for interruption from time to time in the hot loop
					checkInterrupted();

					setMetaDataFollowsFlagInKey(previousKey);
				}

				writeKeyValuePair(previousKey, previousValue);

				//write meta data if we have to
				if (iterator.isNewKeyGroup()) {
					//
					//TODO this could be aware of keyGroupPrefixBytes and write only one byte if possible
					outputView.writeShort(END_OF_KEY_GROUP_MARK);
					//begin new key-group
					keyGroupRangeOffsets.setKeyGroupOffset(iterator.keyGroup(), outStream.getPos());
					//write the kev-state
					//TODO this could be aware of keyGroupPrefixBytes and write only one byte if possible
					outputView.writeShort(iterator.kvStateId());
				} else if (iterator.isNewKeyValueState()) {
					//write the k/v-state
					//TODO this could be aware of keyGroupPrefixBytes and write only one byte if possible
					outputView.writeShort(iterator.kvStateId());
				}

				//request next k/v pair
				previousKey = iterator.key();
				previousValue = iterator.value();
				iterator.next();
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
			StreamStateHandle stateHandle = outStream.closeAndGetHandle();
			outStream = null;
			if (stateHandle != null) {
				return new KeyGroupsStateHandle(keyGroupRangeOffsets, stateHandle);
			} else {
				throw new IOException("Output stream returned null on close.");
			}
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
			if(Thread.currentThread().isInterrupted()) {
				throw new InterruptedException("Snapshot canceled.");
			}
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
		 * @param keyGroupsStateHandles List of all key groups state handles that shall be restored.
		 * @throws IOException
		 * @throws ClassNotFoundException
		 * @throws RocksDBException
		 */
		public void doRestore(List<KeyGroupsStateHandle> keyGroupsStateHandles)
				throws IOException, ClassNotFoundException, RocksDBException {

			for (KeyGroupsStateHandle keyGroupsStateHandle : keyGroupsStateHandles) {
				if (keyGroupsStateHandle != null) {
					this.currentKeyGroupsStateHandle = keyGroupsStateHandle;
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
				currentStateHandleInStream = currentKeyGroupsStateHandle.getStateHandle().openInputStream();
				currentStateHandleInView = new DataInputViewStreamWrapper(currentStateHandleInStream);
				restoreKVStateMetaData();
				restoreKVStateData();
			} finally {
				if(currentStateHandleInStream != null) {
					currentStateHandleInStream.close();
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
			//read number of k/v states
			int numColumns = currentStateHandleInView.readInt();

			//those two lists are aligned and should later have the same size!
			currentStateHandleKVStateColumnFamilies = new ArrayList<>(numColumns);

			//restore the empty columns for the k/v states through the metadata
			for (int i = 0; i < numColumns; i++) {
				ObjectInputStream ooIn = new ObjectInputStream(currentStateHandleInStream);
				StateDescriptor stateDescriptor = (StateDescriptor) ooIn.readObject();
				Tuple2<ColumnFamilyHandle, StateDescriptor> columnFamily = rocksDBKeyedStateBackend.
						kvStateInformation.get(stateDescriptor.getName());

				if(null == columnFamily) {
					ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(
							stateDescriptor.getName().getBytes(), rocksDBKeyedStateBackend.columnOptions);

					columnFamily = new Tuple2<>(rocksDBKeyedStateBackend.db.
							createColumnFamily(columnFamilyDescriptor), stateDescriptor);
					rocksDBKeyedStateBackend.kvStateInformation.put(stateDescriptor.getName(), columnFamily);
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
	protected ColumnFamilyHandle getColumnFamily(StateDescriptor descriptor) {

		Tuple2<ColumnFamilyHandle, StateDescriptor> stateInfo = kvStateInformation.get(descriptor.getName());

		if (stateInfo != null) {
			if (!stateInfo.f1.equals(descriptor)) {
				throw new RuntimeException("Trying to access state using wrong StateDescriptor, was " + stateInfo.f1 +
						" trying access with " + descriptor);
			}
			return stateInfo.f0;
		}

		ColumnFamilyDescriptor columnDescriptor = new ColumnFamilyDescriptor(
				descriptor.getName().getBytes(), columnOptions);

		try {
			ColumnFamilyHandle columnFamily = db.createColumnFamily(columnDescriptor);
			kvStateInformation.put(descriptor.getName(), new Tuple2<>(columnFamily, descriptor));
			return columnFamily;
		} catch (RocksDBException e) {
			throw new RuntimeException("Error creating ColumnFamilyHandle.", e);
		}
	}

	@Override
	protected <N, T> ValueState<T> createValueState(TypeSerializer<N> namespaceSerializer,
			ValueStateDescriptor<T> stateDesc) throws Exception {

		ColumnFamilyHandle columnFamily = getColumnFamily(stateDesc);

		return new RocksDBValueState<>(columnFamily, namespaceSerializer,  stateDesc, this);
	}

	@Override
	protected <N, T> ListState<T> createListState(TypeSerializer<N> namespaceSerializer,
			ListStateDescriptor<T> stateDesc) throws Exception {

		ColumnFamilyHandle columnFamily = getColumnFamily(stateDesc);

		return new RocksDBListState<>(columnFamily, namespaceSerializer, stateDesc, this);
	}

	@Override
	protected <N, T> ReducingState<T> createReducingState(TypeSerializer<N> namespaceSerializer,
			ReducingStateDescriptor<T> stateDesc) throws Exception {

		ColumnFamilyHandle columnFamily = getColumnFamily(stateDesc);

		return new RocksDBReducingState<>(columnFamily, namespaceSerializer,  stateDesc, this);
	}

	@Override
	protected <N, T, ACC> FoldingState<T, ACC> createFoldingState(TypeSerializer<N> namespaceSerializer,
			FoldingStateDescriptor<T, ACC> stateDesc) throws Exception {

		ColumnFamilyHandle columnFamily = getColumnFamily(stateDesc);

		return new RocksDBFoldingState<>(columnFamily, namespaceSerializer, stateDesc, this);
	}

	/**
	 * Wraps a RocksDB iterator to cache it's current key and assign an id for the key/value state to the iterator.
	 * Used by #MergeIterator.
	 */
	static final class MergeIterator {

		/**
		 *
		 * @param iterator The #RocksIterator to wrap .
		 * @param kvStateId Id of the K/V state to which this iterator belongs.
		 */
		public MergeIterator(RocksIterator iterator, int kvStateId) {
			this.iterator = Preconditions.checkNotNull(iterator);
			this.currentKey = iterator.key();
			this.kvStateId = kvStateId;
		}

		private byte[] currentKey;
		private final RocksIterator iterator;
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
	}

	/**
	 * Iterator that merges multiple RocksDB iterators to partition all states into contiguous key-groups.
	 * The resulting iteration sequence is ordered by (key-group, kv-state).
	 */
	static final class RocksDBMergeIterator {

		private final PriorityQueue<MergeIterator> heap;
		private final int keyGroupPrefixByteCount;
		private boolean newKeyGroup;
		private boolean newKVState;
		private boolean valid;

		private MergeIterator currentSubIterator;

		RocksDBMergeIterator(List<Tuple2<RocksIterator, Integer>> kvStateIterators, final int keyGroupPrefixByteCount) throws IOException {
			Preconditions.checkNotNull(kvStateIterators);
			this.keyGroupPrefixByteCount = keyGroupPrefixByteCount;

			Comparator<MergeIterator> iteratorComparator = new Comparator<MergeIterator>() {
				@Override
				public int compare(MergeIterator o1, MergeIterator o2) {
					int arrayCmpRes = compareKeyGroupsForByteArrays(
							o1.currentKey, o2.currentKey, keyGroupPrefixByteCount);
					return arrayCmpRes == 0 ? o1.getKvStateId() - o2.getKvStateId() : arrayCmpRes;
				}
			};

			if (kvStateIterators.size() > 0) {
				this.heap = new PriorityQueue<>(kvStateIterators.size(), iteratorComparator);

				for (Tuple2<RocksIterator, Integer> rocksIteratorWithKVStateId : kvStateIterators) {
					RocksIterator rocksIterator = rocksIteratorWithKVStateId.f0;
					rocksIterator.seekToFirst();
					if (rocksIterator.isValid()) {
						heap.offer(new MergeIterator(rocksIterator, rocksIteratorWithKVStateId.f1));
					}
				}
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
			} else if (heap.isEmpty()) {
				valid = false;
			} else {
				currentSubIterator = heap.poll();
				newKVState = true;
				detectNewKeyGroup(oldKey);
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
	}

	/**
	 * Only visible for testing, DO NOT USE.
	 */
	public File getInstanceBasePath() {
		return instanceBasePath;
	}
}
