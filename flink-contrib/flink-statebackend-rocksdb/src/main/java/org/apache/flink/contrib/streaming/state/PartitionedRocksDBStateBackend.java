/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.runtime.state.AbstractPartitionedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.AsynchronousKvStateSnapshot;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.flink.runtime.state.PartitionedStateSnapshot;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.util.HDFSCopyFromLocal;
import org.apache.flink.streaming.util.HDFSCopyToLocal;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.fs.FileSystem;
import org.rocksdb.BackupEngine;
import org.rocksdb.BackupableDBOptions;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Env;
import org.rocksdb.ReadOptions;
import org.rocksdb.RestoreOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;


public class PartitionedRocksDBStateBackend<KEY> extends AbstractPartitionedStateBackend<KEY> {

	private static final Logger LOG = LoggerFactory.getLogger(RocksDBStateBackend.class);

	private static final String STATE_NAME = "dummy_state";

	// ------------------------------------------------------------------------
	//  Static configuration values
	// ------------------------------------------------------------------------

	/** The checkpoint directory that we copy the RocksDB backups to. */
	private final Path checkpointDirectory;

	/** Whether we do snapshots fully asynchronous */
	private boolean fullyAsyncBackup = false;

	/** Operator identifier that is used to uniqueify the RocksDB storage path. */
	private String operatorIdentifier;

	/** JobID for uniquifying backup paths. */
	private JobID jobId;

	// DB storage directories

	/** Base paths for RocksDB directory, as initialized */
	private File[] initializedDbBasePaths;

	private int nextDirectory;

	// RocksDB options

	/** The options from the options factory, cached */
	private final DBOptions dbOptions;

	private final ColumnFamilyOptions columnOptions;

	// ------------------------------------------------------------------------
	//  Per operator values that are set in initializerForJob
	// ------------------------------------------------------------------------

	/** Path where this configured instance stores its data directory */
	private final File instanceBasePath;

	/** Path where this configured instance stores its RocksDB data base */
	private final File instanceRocksDBPath;

	/** Base path where this configured instance stores checkpoints */
	private final String instanceCheckpointPath;

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
	private Map<String, Tuple2<ColumnFamilyHandle, StateDescriptor>> kvStateInformation;

	// ------------------------------------------------------------------------

	/**
	 * Creates a new {@code RocksDBStateBackend} that stores its checkpoint data in the
	 * file system and location defined by the given URI.
	 *
	 * <p>A state backend that stores checkpoints in HDFS or S3 must specify the file system
	 * host and port in the URI, or have the Hadoop configuration that describes the file system
	 * (host / high-availability group / possibly credentials) either referenced from the Flink
	 * config, or included in the classpath.
	 *
	 * @param checkpointDataUri The URI describing the filesystem and path to the checkpoint data directory.
	 * @throws IOException Thrown, if no file system can be found for the scheme in the URI.
	 */
	public PartitionedRocksDBStateBackend(
		TypeSerializer<KEY> keySerializer,
		ClassLoader classLoader,
		Path checkpointDirectory,
		String operatorIdentifier,
		JobID jobId,
		File[] initializedDbBasePaths,
		DBOptions dbOptions,
		ColumnFamilyOptions columnOptions,
		boolean fullyAsyncBackup) throws IOException {
		super(keySerializer, classLoader);

		this.checkpointDirectory = Preconditions.checkNotNull(checkpointDirectory);
		this.operatorIdentifier = Preconditions.checkNotNull(operatorIdentifier).replace(" ", "");
		this.jobId = Preconditions.checkNotNull(jobId);

		this.initializedDbBasePaths = Preconditions.checkNotNull(initializedDbBasePaths);
		this.dbOptions = Preconditions.checkNotNull(dbOptions);
		this.columnOptions = Preconditions.checkNotNull(columnOptions);

		this.fullyAsyncBackup = fullyAsyncBackup;

		nextDirectory = new Random().nextInt(initializedDbBasePaths.length);

		instanceBasePath = new File(getDbPath(STATE_NAME), UUID.randomUUID().toString());
		instanceCheckpointPath = getCheckpointPath(STATE_NAME);
		instanceRocksDBPath = new File(instanceBasePath, "db");

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
			db = RocksDB.open(
				dbOptions,
				instanceRocksDBPath.getAbsolutePath(),
				columnFamilyDescriptors,
				columnFamilyHandles);
		} catch (RocksDBException e) {
			throw new RuntimeException("Error while opening RocksDB instance.", e);
		}

		kvStateInformation = new HashMap<>();
	}

	// ------------------------------------------------------------------------
	//  State backend methods
	// ------------------------------------------------------------------------

	@Override
	public void disposeAllStateForCurrentJob() throws Exception {
		// we don't have to dispose state for the partitioned state backend
	}

	@Override
	public void close() throws Exception {
		super.close();
		if (this.dbOptions != null) {
			this.dbOptions.dispose();
		}
		for (Tuple2<ColumnFamilyHandle, StateDescriptor> column: kvStateInformation.values()) {
			column.f0.dispose();
		}
		db.dispose();
	}

	private File getDbPath(String stateName) {
		return new File(new File(new File(getNextStoragePath(), jobId.toString()), operatorIdentifier), stateName);
	}

	private String getCheckpointPath(String stateName) {
		return checkpointDirectory + "/" + jobId.toString() + "/" + operatorIdentifier + "/" + UUID.randomUUID().toString() + "/" + stateName;
	}

	private File getNextStoragePath() {
		int ni = nextDirectory + 1;
		ni = ni >= initializedDbBasePaths.length ? 0 : ni;
		nextDirectory = ni;

		return initializedDbBasePaths[ni];
	}

	/**
	 * Visible for tests.
	 */
	public File[] getStoragePaths() {
		return initializedDbBasePaths;
	}

	// ------------------------------------------------------------------------
	//  Snapshot and restore
	// ------------------------------------------------------------------------

	@Override
	public PartitionedStateSnapshot snapshotPartitionedState(long checkpointId, long timestamp) throws Exception {
		if (keyValueStatesByName == null || keyValueStatesByName.size() == 0) {
			return null;
		} else {
			if (fullyAsyncBackup) {
				return performFullyAsyncSnapshot(checkpointId, timestamp);
			} else {
				return performSemiAsyncSnapshot(checkpointId, timestamp);
			}
		}
	}

	/**
	 * Performs a checkpoint by using the RocksDB backup feature to backup to a directory.
	 * This backup is the asynchronously copied to the final checkpoint location.
	 */
	private PartitionedStateSnapshot performSemiAsyncSnapshot(long checkpointId, long timestamp) throws Exception {
		// We don't snapshot individual k/v states since everything is stored in a central
		// RocksDB data base. Create a dummy KvStateSnapshot that holds the information about
		// that checkpoint. We use the in restorePartitionedState to restore.

		final File localBackupPath = new File(instanceBasePath, "local-chk-" + checkpointId);
		final URI backupUri = new URI(instanceCheckpointPath + "/chk-" + checkpointId);

		if (!localBackupPath.exists()) {
			if (!localBackupPath.mkdirs()) {
				throw new RuntimeException("Could not create local backup path " + localBackupPath);
			}
		}

		long startTime = System.currentTimeMillis();

		BackupableDBOptions backupOptions = new BackupableDBOptions(localBackupPath.getAbsolutePath());
		// we disabled the WAL
		backupOptions.setBackupLogFiles(false);
		// no need to sync since we use the backup only as intermediate data before writing to FileSystem snapshot
		backupOptions.setSync(false);

		try (BackupEngine backupEngine = BackupEngine.open(Env.getDefault(), backupOptions)) {
			// wait before flush with "true"
			backupEngine.createNewBackup(db, true);
		}

		long endTime = System.currentTimeMillis();
		LOG.info("RocksDB (" + instanceRocksDBPath + ") backup (synchronous part) took " + (endTime - startTime) + " ms.");

		// draw a copy in case it get's changed while performing the async snapshot
		List<StateDescriptor> kvStateInformationCopy = new ArrayList<>();
		for (Tuple2<ColumnFamilyHandle, StateDescriptor> state: kvStateInformation.values()) {
			kvStateInformationCopy.add(state.f1);
		}
		PartitionedRocksDBStateBackend.SemiAsyncSnapshot dummySnapshot = new PartitionedRocksDBStateBackend.SemiAsyncSnapshot(localBackupPath,
			backupUri,
			kvStateInformationCopy,
			checkpointId);

		PartitionedStateSnapshot result = new PartitionedStateSnapshot();
		result.put(STATE_NAME, dummySnapshot);

		return result;
	}

	/**
	 * Performs a checkpoint by drawing a {@link org.rocksdb.Snapshot} from RocksDB and then
	 * iterating over all key/value pairs in RocksDB to store them in the final checkpoint
	 * location. The only synchronous part is the drawing of the {@code Snapshot} which
	 * is essentially free.
	 */
	private PartitionedStateSnapshot performFullyAsyncSnapshot(long checkpointId, long timestamp) throws Exception {
		// we draw a snapshot from RocksDB then iterate over all keys at that point
		// and store them in the backup location

		final URI backupUri = new URI(instanceCheckpointPath + "/chk-" + checkpointId);

		long startTime = System.currentTimeMillis();

		org.rocksdb.Snapshot snapshot = db.getSnapshot();

		long endTime = System.currentTimeMillis();
		LOG.info("Fully asynchronous RocksDB (" + instanceRocksDBPath + ") backup (synchronous part) took " + (endTime - startTime) + " ms.");

		// draw a copy in case it get's changed while performing the async snapshot
		Map<String, Tuple2<ColumnFamilyHandle, StateDescriptor>> columnFamiliesCopy = new HashMap<>();
		columnFamiliesCopy.putAll(kvStateInformation);
		PartitionedRocksDBStateBackend.FullyAsynSnapshot dummySnapshot = new PartitionedRocksDBStateBackend.FullyAsynSnapshot(db,
			snapshot,
			backupUri,
			columnFamiliesCopy,
			checkpointId);

		PartitionedStateSnapshot result = new PartitionedStateSnapshot();
		result.put(STATE_NAME, dummySnapshot);

		return result;
	}

	@Override
	public void restorePartitionedState(PartitionedStateSnapshot partitionedStateSnapshot, long recoveryTimestamp) throws Exception {
		if (partitionedStateSnapshot.containsKey(STATE_NAME)) {
			KvStateSnapshot dummyState = partitionedStateSnapshot.get(STATE_NAME);
			if (dummyState instanceof PartitionedRocksDBStateBackend.FinalSemiAsyncSnapshot) {
				restoreFromSemiAsyncSnapshot((PartitionedRocksDBStateBackend.FinalSemiAsyncSnapshot) dummyState);
			} else if (dummyState instanceof PartitionedRocksDBStateBackend.FinalFullyAsyncSnapshot) {
				restoreFromFullyAsyncSnapshot((PartitionedRocksDBStateBackend.FinalFullyAsyncSnapshot) dummyState);
			} else {
				throw new RuntimeException("Unknown RocksDB snapshot: " + dummyState);
			}
		}
	}

	private void restoreFromSemiAsyncSnapshot(PartitionedRocksDBStateBackend.FinalSemiAsyncSnapshot snapshot) throws Exception {
		// This does mostly the same work as initializeForJob, we remove the existing RocksDB
		// directory and create a new one from the backup.
		// This must be refactored. The StateBackend should either be initialized from
		// scratch or from a snapshot.

		if (!instanceBasePath.exists()) {
			if (!instanceBasePath.mkdirs()) {
				throw new RuntimeException("Could not create RocksDB data directory.");
			}
		}

		db.dispose();

		// clean it, this will remove the last part of the path but RocksDB will recreate it
		try {
			if (instanceRocksDBPath.exists()) {
				LOG.warn("Deleting already existing db directory {}.", instanceRocksDBPath);
				FileUtils.deleteDirectory(instanceRocksDBPath);
			}
		} catch (IOException e) {
			throw new RuntimeException("Error cleaning RocksDB data directory.", e);
		}

		final File localBackupPath = new File(instanceBasePath, "chk-" + snapshot.checkpointId);

		if (localBackupPath.exists()) {
			try {
				LOG.warn("Deleting already existing local backup directory {}.", localBackupPath);
				FileUtils.deleteDirectory(localBackupPath);
			} catch (IOException e) {
				throw new RuntimeException("Error cleaning RocksDB local backup directory.", e);
			}
		}

		HDFSCopyToLocal.copyToLocal(snapshot.backupUri, instanceBasePath);

		try (BackupEngine backupEngine = BackupEngine.open(Env.getDefault(), new BackupableDBOptions(localBackupPath.getAbsolutePath()))) {
			backupEngine.restoreDbFromLatestBackup(instanceRocksDBPath.getAbsolutePath(), instanceRocksDBPath.getAbsolutePath(), new RestoreOptions(true));
		} catch (RocksDBException|IllegalArgumentException e) {
			throw new RuntimeException("Error while restoring RocksDB state from " + localBackupPath, e);
		} finally {
			try {
				FileUtils.deleteDirectory(localBackupPath);
			} catch (IOException e) {
				LOG.error("Error cleaning up local restore directory " + localBackupPath, e);
			}
		}


		List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>(snapshot.stateDescriptors.size());
		for (StateDescriptor stateDescriptor: snapshot.stateDescriptors) {
			columnFamilyDescriptors.add(new ColumnFamilyDescriptor(stateDescriptor.getName().getBytes(), columnOptions));
		}

		// RocksDB seems to need this...
		columnFamilyDescriptors.add(new ColumnFamilyDescriptor("default".getBytes()));
		List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(snapshot.stateDescriptors.size());
		try {
			db = RocksDB.open(dbOptions, instanceRocksDBPath.getAbsolutePath(), columnFamilyDescriptors, columnFamilyHandles);
			this.kvStateInformation = new HashMap<>();
			for (int i = 0; i < snapshot.stateDescriptors.size(); i++) {
				this.kvStateInformation.put(snapshot.stateDescriptors.get(i).getName(), new Tuple2<>(columnFamilyHandles.get(i), snapshot.stateDescriptors.get(i)));
			}

		} catch (RocksDBException e) {
			throw new RuntimeException("Error while opening RocksDB instance.", e);
		}
	}

	private void restoreFromFullyAsyncSnapshot(PartitionedRocksDBStateBackend.FinalFullyAsyncSnapshot snapshot) throws Exception {

		DataInputView inputView = snapshot.stateHandle.getState(classLoader);

		// clear k/v state information before filling it
		kvStateInformation.clear();

		// first get the column family mapping
		int numColumns = inputView.readInt();
		Map<Byte, StateDescriptor> columnFamilyMapping = new HashMap<>(numColumns);
		for (int i = 0; i < numColumns; i++) {
			byte mappingByte = inputView.readByte();

			ObjectInputStream ooIn = new ObjectInputStream(new DataInputViewStream(inputView));
			StateDescriptor stateDescriptor = (StateDescriptor) ooIn.readObject();

			columnFamilyMapping.put(mappingByte, stateDescriptor);

			// this will fill in the k/v state information
			getColumnFamily(stateDescriptor);
		}

		// try and read until EOF
		try {
			// the EOFException will get us out of this...
			while (true) {
				byte mappingByte = inputView.readByte();
				ColumnFamilyHandle handle = getColumnFamily(columnFamilyMapping.get(mappingByte));
				byte[] key = BytePrimitiveArraySerializer.INSTANCE.deserialize(inputView);
				byte[] value = BytePrimitiveArraySerializer.INSTANCE.deserialize(inputView);
				db.put(handle, key, value);
			}
		} catch (EOFException e) {
			// expected
		}
	}

	// ------------------------------------------------------------------------
	//  Semi-asynchronous Backup Classes
	// ------------------------------------------------------------------------

	/**
	 * Upon snapshotting the RocksDB backup is created synchronously. The asynchronous part is
	 * copying the backup to a (possibly) remote filesystem. This is done in {@link #materialize()}.
	 */
	private static class SemiAsyncSnapshot extends AsynchronousKvStateSnapshot<Object, Object, ValueState<Object>, ValueStateDescriptor<Object>, PartitionedRocksDBStateBackend<Object>> {
		private static final long serialVersionUID = 1L;
		private final File localBackupPath;
		private final URI backupUri;
		private final List<StateDescriptor> stateDescriptors;
		private final long checkpointId;

		private SemiAsyncSnapshot(
			File localBackupPath,
			URI backupUri,
			List<StateDescriptor> columnFamilies,
			long checkpointId) {

			this.localBackupPath = localBackupPath;
			this.backupUri = backupUri;
			this.stateDescriptors = columnFamilies;
			this.checkpointId = checkpointId;
		}

		@Override
		public KvStateSnapshot<Object, Object, ValueState<Object>, ValueStateDescriptor<Object>, PartitionedRocksDBStateBackend<Object>> materialize() throws Exception {
			try {
				long startTime = System.currentTimeMillis();
				HDFSCopyFromLocal.copyFromLocal(localBackupPath, backupUri);
				long endTime = System.currentTimeMillis();
				LOG.info("RocksDB materialization from " + localBackupPath + " to " + backupUri + " (asynchronous part) took " + (endTime - startTime) + " ms.");
				return new PartitionedRocksDBStateBackend.FinalSemiAsyncSnapshot(backupUri, checkpointId, stateDescriptors);
			} catch (Exception e) {
				FileSystem fs = FileSystem.get(backupUri, HadoopFileSystem.getHadoopConfiguration());
				fs.delete(new org.apache.hadoop.fs.Path(backupUri), true);
				throw e;
			} finally {
				FileUtils.deleteQuietly(localBackupPath);
			}
		}
	}

	/**
	 * Dummy {@link KvStateSnapshot} that holds the state of our one RocksDB data base. This
	 * also stores the column families that we had at the time of the snapshot so that we can
	 * restore these. This results from {@link PartitionedRocksDBStateBackend.SemiAsyncSnapshot}.
	 */
	private static class FinalSemiAsyncSnapshot implements KvStateSnapshot<Object, Object, ValueState<Object>, ValueStateDescriptor<Object>, PartitionedRocksDBStateBackend<Object>> {
		private static final long serialVersionUID = 1L;

		final URI backupUri;
		final long checkpointId;
		private final List<StateDescriptor> stateDescriptors;

		/**
		 * Creates a new snapshot from the given state parameters.
		 */
		private FinalSemiAsyncSnapshot(URI backupUri, long checkpointId, List<StateDescriptor> stateDescriptors) {
			this.backupUri = backupUri;
			this.checkpointId = checkpointId;
			this.stateDescriptors = stateDescriptors;
		}

		@Override
		public final KvState<Object, Object, ValueState<Object>, ValueStateDescriptor<Object>, PartitionedRocksDBStateBackend<Object>> restoreState(
			PartitionedRocksDBStateBackend<Object> stateBackend,
			TypeSerializer<Object> keySerializer,
			ClassLoader classLoader,
			long recoveryTimestamp) throws Exception {
			throw new RuntimeException("Should never happen.");
		}

		@Override
		public final void discardState() throws Exception {
			FileSystem fs = FileSystem.get(backupUri, HadoopFileSystem.getHadoopConfiguration());
			fs.delete(new org.apache.hadoop.fs.Path(backupUri), true);
		}

		@Override
		public final long getStateSize() throws Exception {
			FileSystem fs = FileSystem.get(backupUri, HadoopFileSystem.getHadoopConfiguration());
			return fs.getContentSummary(new org.apache.hadoop.fs.Path(backupUri)).getLength();
		}
	}

	// ------------------------------------------------------------------------
	//  Fully asynchronous Backup Classes
	// ------------------------------------------------------------------------

	/**
	 * This does the snapshot using a RocksDB snapshot and an iterator over all keys
	 * at the point of that snapshot.
	 */
	private static class FullyAsynSnapshot extends AsynchronousKvStateSnapshot<Object, Object, ValueState<Object>, ValueStateDescriptor<Object>, PartitionedRocksDBStateBackend<Object>> {
		private static final long serialVersionUID = 1L;

		private transient final RocksDB db;
		private transient org.rocksdb.Snapshot snapshot;

		private final URI backupUri;
		private final Map<String, Tuple2<ColumnFamilyHandle, StateDescriptor>> columnFamilies;
		private final long checkpointId;

		private FullyAsynSnapshot(
			RocksDB db,
			org.rocksdb.Snapshot snapshot,
			URI backupUri,
			Map<String, Tuple2<ColumnFamilyHandle, StateDescriptor>> columnFamilies,
			long checkpointId) {
			this.db = db;
			this.snapshot = snapshot;
			this.backupUri = backupUri;
			this.columnFamilies = columnFamilies;
			this.checkpointId = checkpointId;
		}

		@Override
		public KvStateSnapshot<Object, Object, ValueState<Object>, ValueStateDescriptor<Object>, PartitionedRocksDBStateBackend<Object>> materialize() throws Exception {
			try {
				long startTime = System.currentTimeMillis();

				Path basePath = FsStateBackend.validateAndNormalizeUri(backupUri);
				org.apache.flink.core.fs.FileSystem fileSystem = basePath.getFileSystem();

				AbstractStateBackend.CheckpointStateOutputView outputView = new AbstractStateBackend.CheckpointStateOutputView(
					new FsStateBackend.FsCheckpointStateOutputStream(
						basePath,
						fileSystem,
						FsStateBackend.DEFAULT_WRITE_BUFFER_SIZE,
						FsStateBackend.DEFAULT_FILE_STATE_THRESHOLD));

				outputView.writeInt(columnFamilies.size());

				// we don't know how many key/value pairs there are in each column family.
				// We prefix every written element with a byte that signifies to which
				// column family it belongs, this way we can restore the column families
				byte count = 0;
				Map<String, Byte> columnFamilyMapping = new HashMap<>();
				for (Map.Entry<String, Tuple2<ColumnFamilyHandle, StateDescriptor>> column: columnFamilies.entrySet()) {
					columnFamilyMapping.put(column.getKey(), count);

					outputView.writeByte(count);

					ObjectOutputStream ooOut = new ObjectOutputStream(outputView);
					ooOut.writeObject(column.getValue().f1);
					ooOut.flush();

					count++;
				}

				for (Map.Entry<String, Tuple2<ColumnFamilyHandle, StateDescriptor>> column: columnFamilies.entrySet()) {
					byte columnByte = columnFamilyMapping.get(column.getKey());
					ReadOptions readOptions = new ReadOptions();
					readOptions.setSnapshot(snapshot);
					RocksIterator iterator = db.newIterator(column.getValue().f0, readOptions);
					iterator.seekToFirst();
					while (iterator.isValid()) {
						outputView.writeByte(columnByte);
						BytePrimitiveArraySerializer.INSTANCE.serialize(iterator.key(), outputView);
						BytePrimitiveArraySerializer.INSTANCE.serialize(iterator.value(), outputView);
						iterator.next();
					}
				}

				StateHandle<DataInputView> stateHandle = outputView.closeAndGetHandle();

				long endTime = System.currentTimeMillis();
				LOG.info("Fully asynchronous RocksDB materialization to " + backupUri + " (asynchronous part) took " + (endTime - startTime) + " ms.");
				return new PartitionedRocksDBStateBackend.FinalFullyAsyncSnapshot(stateHandle, checkpointId);
			} finally {
				db.releaseSnapshot(snapshot);
				snapshot = null;
			}
		}

		@Override
		protected void finalize() throws Throwable {
			if (snapshot != null) {
				db.releaseSnapshot(snapshot);
			}
			super.finalize();
		}
	}

	/**
	 * Dummy {@link KvStateSnapshot} that holds the state of our one RocksDB data base. This
	 * results from {@link PartitionedRocksDBStateBackend.FullyAsynSnapshot}.
	 */
	private static class FinalFullyAsyncSnapshot implements KvStateSnapshot<Object, Object, ValueState<Object>, ValueStateDescriptor<Object>, PartitionedRocksDBStateBackend<Object>> {
		private static final long serialVersionUID = 1L;

		final StateHandle<DataInputView> stateHandle;
		final long checkpointId;

		/**
		 * Creates a new snapshot from the given state parameters.
		 */
		private FinalFullyAsyncSnapshot(StateHandle<DataInputView> stateHandle, long checkpointId) {
			this.stateHandle = stateHandle;
			this.checkpointId = checkpointId;
		}

		@Override
		public final KvState<Object, Object, ValueState<Object>, ValueStateDescriptor<Object>, PartitionedRocksDBStateBackend<Object>> restoreState(
			PartitionedRocksDBStateBackend<Object> stateBackend,
			TypeSerializer<Object> keySerializer,
			ClassLoader classLoader,
			long recoveryTimestamp) throws Exception {
			throw new RuntimeException("Should never happen.");
		}

		@Override
		public final void discardState() throws Exception {
			stateHandle.discardState();
		}

		@Override
		public final long getStateSize() throws Exception {
			return stateHandle.getStateSize();
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
	protected ColumnFamilyHandle getColumnFamily(StateDescriptor descriptor)  {

		Tuple2<ColumnFamilyHandle, StateDescriptor> stateInfo = kvStateInformation.get(descriptor.getName());

		if (stateInfo != null) {
			if (!stateInfo.f1.equals(descriptor)) {
				throw new RuntimeException("Trying to access state using wrong StateDescriptor, was " + stateInfo.f1 + " trying access with " + descriptor);
			}
			return stateInfo.f0;
		}

		ColumnFamilyDescriptor columnDescriptor = new ColumnFamilyDescriptor(descriptor.getName().getBytes(), columnOptions);

		try {
			ColumnFamilyHandle columnFamily = db.createColumnFamily(columnDescriptor);
			kvStateInformation.put(descriptor.getName(), new Tuple2<>(columnFamily, descriptor));
			return columnFamily;
		} catch (RocksDBException e) {
			throw new RuntimeException("Error creating ColumnFamilyHandle.", e);
		}
	}

	/**
	 * Used by k/v states to access the current key.
	 */
	public Object currentKey() {
		return currentKey;
	}

	/**
	 * Used by k/v states to access the key serializer.
	 */
	public TypeSerializer keySerializer() {
		return keySerializer;
	}

	@Override
	protected <N, T> ValueState<T> createValueState(
		TypeSerializer<N> namespaceSerializer,
		ValueStateDescriptor<T> stateDesc) throws Exception {

		ColumnFamilyHandle columnFamily = getColumnFamily(stateDesc);

		return new RocksDBValueState<>(columnFamily, namespaceSerializer,  stateDesc, this);
	}

	@Override
	protected <N, T> ListState<T> createListState(
		TypeSerializer<N> namespaceSerializer,
		ListStateDescriptor<T> stateDesc) throws Exception {

		ColumnFamilyHandle columnFamily = getColumnFamily(stateDesc);

		return new RocksDBListState<>(columnFamily, namespaceSerializer, stateDesc, this);
	}

	@Override
	protected <N, T> ReducingState<T> createReducingState(
		TypeSerializer<N> namespaceSerializer,
		ReducingStateDescriptor<T> stateDesc) throws Exception {

		ColumnFamilyHandle columnFamily = getColumnFamily(stateDesc);

		return new RocksDBReducingState<>(columnFamily, namespaceSerializer,  stateDesc, this);
	}

	@Override
	protected <N, T, ACC> FoldingState<T, ACC> createFoldingState(
		TypeSerializer<N> namespaceSerializer,
		FoldingStateDescriptor<T, ACC> stateDesc) throws Exception {

		ColumnFamilyHandle columnFamily = getColumnFamily(stateDesc);

		return new RocksDBFoldingState<>(columnFamily, namespaceSerializer, stateDesc, this);
	}
}
