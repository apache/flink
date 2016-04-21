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

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

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
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.AsynchronousKvStateSnapshot;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.api.common.state.StateBackend;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.util.HDFSCopyFromLocal;
import org.apache.flink.streaming.util.HDFSCopyToLocal;
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

import static java.util.Objects.requireNonNull;

/**
 * A {@link StateBackend} that stores its state in {@code RocksDB}. This state backend can
 * store very large state that exceeds memory and spills to disk.
 * 
 * <p>All key/value state (including windows) is stored in the key/value index of RocksDB.
 * For persistence against loss of machines, checkpoints take a snapshot of the
 * RocksDB database, and persist that snapshot in a file system (by default) or
 * another configurable state backend.
 * 
 * <p>The behavior of the RocksDB instances can be parametrized by setting RocksDB Options
 * using the methods {@link #setPredefinedOptions(PredefinedOptions)} and
 * {@link #setOptions(OptionsFactory)}.
 */
public class RocksDBStateBackend extends AbstractStateBackend {
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(RocksDBStateBackend.class);

	// ------------------------------------------------------------------------
	//  Static configuration values
	// ------------------------------------------------------------------------
	
	/** The checkpoint directory that we copy the RocksDB backups to. */
	private final Path checkpointDirectory;

	/** The state backend that stores the non-partitioned state */
	private final AbstractStateBackend nonPartitionedStateBackend;

	/** Whether we do snapshots fully asynchronous */
	private boolean fullyAsyncBackup = false;

	/** Operator identifier that is used to uniqueify the RocksDB storage path. */
	private String operatorIdentifier;

	/** JobID for uniquifying backup paths. */
	private JobID jobId;

	// DB storage directories
	
	/** Base paths for RocksDB directory, as configured. May be null. */
	private Path[] configuredDbBasePaths;

	/** Base paths for RocksDB directory, as initialized */
	private File[] initializedDbBasePaths;
	
	private int nextDirectory;
	
	// RocksDB options
	
	/** The pre-configured option settings */
	private PredefinedOptions predefinedOptions = PredefinedOptions.DEFAULT;
	
	/** The options factory to create the RocksDB options in the cluster */
	private OptionsFactory optionsFactory;
	
	/** The options from the options factory, cached */
	private transient DBOptions dbOptions;
	private transient ColumnFamilyOptions columnOptions;

	// ------------------------------------------------------------------------
	//  Per operator values that are set in initializerForJob
	// ------------------------------------------------------------------------

	/** Path where this configured instance stores its data directory */
	private transient File instanceBasePath;

	/** Path where this configured instance stores its RocksDB data base */
	private transient File instanceRocksDBPath;

	/** Base path where this configured instance stores checkpoints */
	private transient String instanceCheckpointPath;

	/**
	 * Our RocksDB data base, this is used by the actual subclasses of {@link AbstractRocksDBState}
	 * to store state. The different k/v states that we have don't each have their own RocksDB
	 * instance. They all write to this instance but to their own column family.
	 */
	protected transient RocksDB db;

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
	public RocksDBStateBackend(String checkpointDataUri) throws IOException {
		this(new Path(checkpointDataUri).toUri());
	}

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
	public RocksDBStateBackend(URI checkpointDataUri) throws IOException {
		// creating the FsStateBackend automatically sanity checks the URI
		FsStateBackend fsStateBackend = new FsStateBackend(checkpointDataUri);
		
		this.nonPartitionedStateBackend = fsStateBackend;
		this.checkpointDirectory = fsStateBackend.getBasePath();
	}


	public RocksDBStateBackend(String checkpointDataUri, AbstractStateBackend nonPartitionedStateBackend) throws IOException {
		this(new Path(checkpointDataUri).toUri(), nonPartitionedStateBackend);
	}
	
	public RocksDBStateBackend(URI checkpointDataUri, AbstractStateBackend nonPartitionedStateBackend) throws IOException {
		this.nonPartitionedStateBackend = requireNonNull(nonPartitionedStateBackend);
		this.checkpointDirectory = FsStateBackend.validateAndNormalizeUri(checkpointDataUri);
	}

	// ------------------------------------------------------------------------
	//  State backend methods
	// ------------------------------------------------------------------------
	
	@Override
	public void initializeForJob(
			Environment env, 
			String operatorIdentifier,
			TypeSerializer<?> keySerializer) throws Exception {
		
		super.initializeForJob(env, operatorIdentifier, keySerializer);

		this.nonPartitionedStateBackend.initializeForJob(env, operatorIdentifier, keySerializer);
		
		this.operatorIdentifier = operatorIdentifier.replace(" ", "");
		this.jobId = env.getJobID();
		
		// initialize the paths where the local RocksDB files should be stored
		if (configuredDbBasePaths == null) {
			// initialize from the temp directories
			initializedDbBasePaths = env.getIOManager().getSpillingDirectories();
		}
		else {
			List<File> dirs = new ArrayList<>(configuredDbBasePaths.length);
			String errorMessage = "";
			
			for (Path path : configuredDbBasePaths) {
				File f = new File(path.toUri().getPath());
				File testDir = new File(f, UUID.randomUUID().toString());
				if (!testDir.mkdirs()) {
					String msg = "Local DB files directory '" + path
							+ "' does not exist and cannot be created. ";
					LOG.error(msg);
					errorMessage += msg;
				} else {
					dirs.add(f);
				}
			}
			
			if (dirs.isEmpty()) {
				throw new Exception("No local storage directories available. " + errorMessage);
			} else {
				initializedDbBasePaths = dirs.toArray(new File[dirs.size()]);
			}
		}
		
		nextDirectory = new Random().nextInt(initializedDbBasePaths.length);

		instanceBasePath = new File(getDbPath("dummy_state"), UUID.randomUUID().toString());
		instanceCheckpointPath = getCheckpointPath("dummy_state");
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
			db = RocksDB.open(getDbOptions(), instanceRocksDBPath.getAbsolutePath(), columnFamilyDescriptors, columnFamilyHandles);
		} catch (RocksDBException e) {
			throw new RuntimeException("Error while opening RocksDB instance.", e);
		}

		kvStateInformation = new HashMap<>();
	}

	@Override
	public void disposeAllStateForCurrentJob() throws Exception {
		nonPartitionedStateBackend.disposeAllStateForCurrentJob();
	}

	@Override
	public void dispose() {
		super.dispose();
		nonPartitionedStateBackend.dispose();

		if (this.dbOptions != null) {
			this.dbOptions.dispose();
			this.dbOptions = null;
		}
		for (Tuple2<ColumnFamilyHandle, StateDescriptor> column: kvStateInformation.values()) {
			column.f0.dispose();
		}
		db.dispose();
	}

	@Override
	public void close() throws Exception {
		nonPartitionedStateBackend.close();
		
		if (this.dbOptions != null) {
			this.dbOptions.dispose();
			this.dbOptions = null;
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
		return checkpointDirectory + "/" + jobId.toString() + "/" + operatorIdentifier + "/" + stateName;
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
	public HashMap<String, KvStateSnapshot<?, ?, ?, ?, ?>> snapshotPartitionedState(long checkpointId, long timestamp) throws Exception {
		if (keyValueStatesByName == null || keyValueStatesByName.size() == 0) {
			return new HashMap<>();
		}

		if (fullyAsyncBackup) {
			return performFullyAsyncSnapshot(checkpointId, timestamp);
		} else {
			return performSemiAsyncSnapshot(checkpointId, timestamp);
		}
	}

	/**
	 * Performs a checkpoint by using the RocksDB backup feature to backup to a directory.
	 * This backup is the asynchronously copied to the final checkpoint location.
	 */
	private HashMap<String, KvStateSnapshot<?, ?, ?, ?, ?>> performSemiAsyncSnapshot(long checkpointId, long timestamp) throws Exception {
		// We don't snapshot individual k/v states since everything is stored in a central
		// RocksDB data base. Create a dummy KvStateSnapshot that holds the information about
		// that checkpoint. We use the in injectKeyValueStateSnapshots to restore.

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
		SemiAsyncSnapshot dummySnapshot = new SemiAsyncSnapshot(localBackupPath,
				backupUri,
				kvStateInformationCopy,
				checkpointId);


		HashMap<String, KvStateSnapshot<?, ?, ?, ?, ?>> result = new HashMap<>();
		result.put("dummy_state", dummySnapshot);
		return result;
	}

	/**
	 * Performs a checkpoint by drawing a {@link org.rocksdb.Snapshot} from RocksDB and then
	 * iterating over all key/value pairs in RocksDB to store them in the final checkpoint
	 * location. The only synchronous part is the drawing of the {@code Snapshot} which
	 * is essentially free.
	 */
	private HashMap<String, KvStateSnapshot<?, ?, ?, ?, ?>> performFullyAsyncSnapshot(long checkpointId, long timestamp) throws Exception {
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
		FullyAsynSnapshot dummySnapshot = new FullyAsynSnapshot(db,
				snapshot,
				this,
				backupUri,
				columnFamiliesCopy,
				checkpointId);


		HashMap<String, KvStateSnapshot<?, ?, ?, ?, ?>> result = new HashMap<>();
		result.put("dummy_state", dummySnapshot);
		return result;
	}

	@Override
	public final void injectKeyValueStateSnapshots(HashMap<String, KvStateSnapshot> keyValueStateSnapshots, long recoveryTimestamp) throws Exception {
		if (keyValueStateSnapshots.size() == 0) {
			return;
		}

		KvStateSnapshot dummyState = keyValueStateSnapshots.get("dummy_state");
		if (dummyState instanceof FinalSemiAsyncSnapshot) {
			restoreFromSemiAsyncSnapshot((FinalSemiAsyncSnapshot) dummyState);
		} else if (dummyState instanceof FinalFullyAsyncSnapshot) {
			restoreFromFullyAsyncSnapshot((FinalFullyAsyncSnapshot) dummyState);
		} else {
			throw new RuntimeException("Unknown RocksDB snapshot: " + dummyState);
		}
	}

	private void restoreFromSemiAsyncSnapshot(FinalSemiAsyncSnapshot snapshot) throws Exception {
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
			columnFamilyDescriptors.add(new ColumnFamilyDescriptor(stateDescriptor.getName().getBytes(), getColumnOptions()));
		}

		// RocksDB seems to need this...
		columnFamilyDescriptors.add(new ColumnFamilyDescriptor("default".getBytes()));
		List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(snapshot.stateDescriptors.size());
		try {

			db = RocksDB.open(getDbOptions(), instanceRocksDBPath.getAbsolutePath(), columnFamilyDescriptors, columnFamilyHandles);
			this.kvStateInformation = new HashMap<>();
			for (int i = 0; i < snapshot.stateDescriptors.size(); i++) {
				this.kvStateInformation.put(snapshot.stateDescriptors.get(i).getName(), new Tuple2<>(columnFamilyHandles.get(i), snapshot.stateDescriptors.get(i)));
			}

		} catch (RocksDBException e) {
			throw new RuntimeException("Error while opening RocksDB instance.", e);
		}
	}

	private void restoreFromFullyAsyncSnapshot(FinalFullyAsyncSnapshot snapshot) throws Exception {

		DataInputView inputView = snapshot.stateHandle.getState(userCodeClassLoader);

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
	private static class SemiAsyncSnapshot extends AsynchronousKvStateSnapshot<Object, Object, ValueState<Object>, ValueStateDescriptor<Object>, RocksDBStateBackend> {
		private static final long serialVersionUID = 1L;
		private final File localBackupPath;
		private final URI backupUri;
		private final List<StateDescriptor> stateDescriptors;
		private final long checkpointId;

		private SemiAsyncSnapshot(File localBackupPath,
				URI backupUri,
				List<StateDescriptor> columnFamilies,
				long checkpointId) {
			this.localBackupPath = localBackupPath;
			this.backupUri = backupUri;
			this.stateDescriptors = columnFamilies;
			this.checkpointId = checkpointId;
		}

		@Override
		public KvStateSnapshot<Object, Object, ValueState<Object>, ValueStateDescriptor<Object>, RocksDBStateBackend> materialize() throws Exception {
			try {
				long startTime = System.currentTimeMillis();
				HDFSCopyFromLocal.copyFromLocal(localBackupPath, backupUri);
				long endTime = System.currentTimeMillis();
				LOG.info("RocksDB materialization from " + localBackupPath + " to " + backupUri + " (asynchronous part) took " + (endTime - startTime) + " ms.");
				return new FinalSemiAsyncSnapshot(backupUri, checkpointId, stateDescriptors);
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
	 * restore these. This results from {@link SemiAsyncSnapshot}.
	 */
	private static class FinalSemiAsyncSnapshot implements KvStateSnapshot<Object, Object, ValueState<Object>, ValueStateDescriptor<Object>, RocksDBStateBackend> {
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
		public final KvState<Object, Object, ValueState<Object>, ValueStateDescriptor<Object>, RocksDBStateBackend> restoreState(
				RocksDBStateBackend stateBackend,
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
	private static class FullyAsynSnapshot extends AsynchronousKvStateSnapshot<Object, Object, ValueState<Object>, ValueStateDescriptor<Object>, RocksDBStateBackend> {
		private static final long serialVersionUID = 1L;

		private transient final RocksDB db;
		private transient org.rocksdb.Snapshot snapshot;
		private transient AbstractStateBackend backend;

		private final URI backupUri;
		private final Map<String, Tuple2<ColumnFamilyHandle, StateDescriptor>> columnFamilies;
		private final long checkpointId;

		private FullyAsynSnapshot(RocksDB db,
				org.rocksdb.Snapshot snapshot,
				AbstractStateBackend backend,
				URI backupUri,
				Map<String, Tuple2<ColumnFamilyHandle, StateDescriptor>> columnFamilies,
				long checkpointId) {
			this.db = db;
			this.snapshot = snapshot;
			this.backend = backend;
			this.backupUri = backupUri;
			this.columnFamilies = columnFamilies;
			this.checkpointId = checkpointId;
		}

		@Override
		public KvStateSnapshot<Object, Object, ValueState<Object>, ValueStateDescriptor<Object>, RocksDBStateBackend> materialize() throws Exception {
			try {
				long startTime = System.currentTimeMillis();

				CheckpointStateOutputView outputView = backend.createCheckpointStateOutputView(checkpointId, startTime);

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
				return new FinalFullyAsyncSnapshot(stateHandle, checkpointId);
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
	 * results from {@link FullyAsynSnapshot}.
	 */
	private static class FinalFullyAsyncSnapshot implements KvStateSnapshot<Object, Object, ValueState<Object>, ValueStateDescriptor<Object>, RocksDBStateBackend> {
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
		public final KvState<Object, Object, ValueState<Object>, ValueStateDescriptor<Object>, RocksDBStateBackend> restoreState(
				RocksDBStateBackend stateBackend,
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

		ColumnFamilyDescriptor columnDescriptor = new ColumnFamilyDescriptor(descriptor.getName().getBytes(), getColumnOptions());

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

	// ------------------------------------------------------------------------
	//  Non-partitioned state
	// ------------------------------------------------------------------------

	@Override
	public CheckpointStateOutputStream createCheckpointStateOutputStream(
			long checkpointID, long timestamp) throws Exception {
		
		return nonPartitionedStateBackend.createCheckpointStateOutputStream(checkpointID, timestamp);
	}

	@Override
	public <S extends Serializable> StateHandle<S> checkpointStateSerializable(
			S state, long checkpointID, long timestamp) throws Exception {
		
		return nonPartitionedStateBackend.checkpointStateSerializable(state, checkpointID, timestamp);
	}

	// ------------------------------------------------------------------------
	//  Parameters
	// ------------------------------------------------------------------------

	/**
	 * Enables fully asynchronous snapshotting of the partitioned state held in RocksDB.
	 *
	 * <p>By default, this is disabled. This means that RocksDB state is copied in a synchronous
	 * step, during which normal processing of elements pauses, followed by an asynchronous step
	 * of copying the RocksDB backup to the final checkpoint location. Fully asynchronous
	 * snapshots take longer (linear time requirement with respect to number of unique keys)
	 * but normal processing of elements is not paused.
	 */
	public void enableFullyAsyncSnapshots() {
		this.fullyAsyncBackup = true;
	}

	/**
	 * Disables fully asynchronous snapshotting of the partitioned state held in RocksDB.
	 *
	 * <p>By default, this is disabled.
	 */
	public void disableFullyAsyncSnapshots() {
		this.fullyAsyncBackup = false;
	}

	/**
	 * Sets the path where the RocksDB local database files should be stored on the local
	 * file system. Setting this path overrides the default behavior, where the
	 * files are stored across the configured temp directories.
	 * 
	 * <p>Passing {@code null} to this function restores the default behavior, where the configured
	 * temp directories will be used.
	 * 
	 * @param path The path where the local RocksDB database files are stored.
	 */
	public void setDbStoragePath(String path) {
		setDbStoragePaths(path == null ? null : new String[] { path });
	}

	/**
	 * Sets the paths across which the local RocksDB database files are distributed on the local
	 * file system. Setting these paths overrides the default behavior, where the
	 * files are stored across the configured temp directories.
	 * 
	 * <p>Each distinct state will be stored in one path, but when the state backend creates
	 * multiple states, they will store their files on different paths.
	 * 
	 * <p>Passing {@code null} to this function restores the default behavior, where the configured
	 * temp directories will be used.
	 * 
	 * @param paths The paths across which the local RocksDB database files will be spread. 
	 */
	public void setDbStoragePaths(String... paths) {
		if (paths == null) {
			configuredDbBasePaths = null;
		} 
		else if (paths.length == 0) {
			throw new IllegalArgumentException("empty paths");
		}
		else {
			Path[] pp = new Path[paths.length];
			
			for (int i = 0; i < paths.length; i++) {
				if (paths[i] == null) {
					throw new IllegalArgumentException("null path");
				}
				
				pp[i] = new Path(paths[i]);
				String scheme = pp[i].toUri().getScheme();
				if (scheme != null && !scheme.equalsIgnoreCase("file")) {
					throw new IllegalArgumentException("Path " + paths[i] + " has a non local scheme");
				}
			}
			
			configuredDbBasePaths = pp;
		}
	}

	/**
	 * 
	 * @return The configured DB storage paths, or null, if none were configured. 
	 */
	public String[] getDbStoragePaths() {
		if (configuredDbBasePaths == null) {
			return null;
		} else {
			String[] paths = new String[configuredDbBasePaths.length];
			for (int i = 0; i < paths.length; i++) {
				paths[i] = configuredDbBasePaths[i].toString();
			}
			return paths;
		}
	}
	
	// ------------------------------------------------------------------------
	//  Parametrize with RocksDB Options
	// ------------------------------------------------------------------------

	/**
	 * Sets the predefined options for RocksDB.
	 * 
	 * <p>If a user-defined options factory is set (via {@link #setOptions(OptionsFactory)}),
	 * then the options from the factory are applied on top of the here specified
	 * predefined options.
	 * 
	 * @param options The options to set (must not be null).
	 */
	public void setPredefinedOptions(PredefinedOptions options) {
		predefinedOptions = requireNonNull(options);
	}

	/**
	 * Gets the currently set predefined options for RocksDB.
	 * The default options (if nothing was set via {@link #setPredefinedOptions(PredefinedOptions)})
	 * are {@link PredefinedOptions#DEFAULT}.
	 * 
	 * <p>If a user-defined  options factory is set (via {@link #setOptions(OptionsFactory)}),
	 * then the options from the factory are applied on top of the predefined options.
	 * 
	 * @return The currently set predefined options for RocksDB.
	 */
	public PredefinedOptions getPredefinedOptions() {
		return predefinedOptions;
	}

	/**
	 * Sets {@link org.rocksdb.Options} for the RocksDB instances.
	 * Because the options are not serializable and hold native code references,
	 * they must be specified through a factory.
	 * 
	 * <p>The options created by the factory here are applied on top of the pre-defined 
	 * options profile selected via {@link #setPredefinedOptions(PredefinedOptions)}.
	 * If the pre-defined options profile is the default
	 * ({@link PredefinedOptions#DEFAULT}), then the factory fully controls the RocksDB
	 * options.
	 * 
	 * @param optionsFactory The options factory that lazily creates the RocksDB options.
	 */
	public void setOptions(OptionsFactory optionsFactory) {
		this.optionsFactory = optionsFactory;
	}

	/**
	 * Gets the options factory that lazily creates the RocksDB options.
	 * 
	 * @return The options factory.
	 */
	public OptionsFactory getOptions() {
		return optionsFactory;
	}

	/**
	 * Gets the RocksDB {@link DBOptions} to be used for all RocksDB instances.
	 */
	public DBOptions getDbOptions() {
		if (dbOptions == null) {
			// initial options from pre-defined profile
			DBOptions opt = predefinedOptions.createDBOptions();

			// add user-defined options, if specified
			if (optionsFactory != null) {
				opt = optionsFactory.createDBOptions(opt);
			}
			
			// add necessary default options
			opt = opt.setCreateIfMissing(true);

			dbOptions = opt;
		}
		return dbOptions;
	}

	/**
	 * Gets the RocksDB {@link ColumnFamilyOptions} to be used for all RocksDB instances.
	 */
	public ColumnFamilyOptions getColumnOptions() {
		if (columnOptions == null) {
			// initial options from pre-defined profile
			ColumnFamilyOptions opt = predefinedOptions.createColumnOptions();

			// add user-defined options, if specified
			if (optionsFactory != null) {
				opt = optionsFactory.createColumnOptions(opt);
			}

			columnOptions = opt;
		}
		return columnOptions;
	}
}
