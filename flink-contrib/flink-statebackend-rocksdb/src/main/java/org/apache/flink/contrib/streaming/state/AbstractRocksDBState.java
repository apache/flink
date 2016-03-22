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

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.runtime.state.AsynchronousKvStateSnapshot;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.runtime.state.KvStateSnapshot;

import org.apache.flink.streaming.util.HDFSCopyFromLocal;
import org.apache.flink.streaming.util.HDFSCopyToLocal;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.rocksdb.BackupEngine;
import org.rocksdb.BackupableDBOptions;
import org.rocksdb.Env;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RestoreOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

/**
 * Base class for {@link State} implementations that store state in a RocksDB database.
 *
 * <p>This base class is responsible for setting up the RocksDB database, for
 * checkpointing/restoring the database and for disposal in the {@link #dispose()} method. The
 * concrete subclasses just use the RocksDB handle to store/retrieve state.
 *
 * <p>State is checkpointed asynchronously. The synchronous part is drawing the actual backup
 * from RocksDB, this is done in {@link #snapshot(long, long)}. This will return a
 * {@link AsyncRocksDBSnapshot} that will perform the copying of the backup to the remote
 * file system.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <S> The type of {@link State}.
 * @param <SD> The type of {@link StateDescriptor}.
 */
public abstract class AbstractRocksDBState<K, N, S extends State, SD extends StateDescriptor<S, ?>>
	implements KvState<K, N, S, SD, RocksDBStateBackend>, State {

	private static final Logger LOG = LoggerFactory.getLogger(AbstractRocksDBState.class);

	/** Serializer for the keys */
	protected final TypeSerializer<K> keySerializer;

	/** Serializer for the namespace */
	protected final TypeSerializer<N> namespaceSerializer;

	/** The current key, which the next value methods will refer to */
	protected K currentKey;

	/** The current namespace, which the next value methods will refer to */
	protected N currentNamespace;

	/** Store it so that we can clean up in dispose() */
	protected final File basePath;

	/** FileSystem path where checkpoints are stored */
	protected final String checkpointPath;

	/** Directory in "basePath" where the actual RocksDB data base instance stores its files */
	protected final File rocksDbPath;

	/** Our RocksDB instance */
	protected final RocksDB db;

	/**
	 * Creates a new RocksDB backed state.
	 *
	 * @param keySerializer The serializer for the keys.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param basePath The path on the local system where RocksDB data should be stored.
	 */
	protected AbstractRocksDBState(
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			File basePath,
			String checkpointPath,
			Options options) {

		rocksDbPath = new File(basePath, "db" + UUID.randomUUID().toString());

		this.keySerializer = requireNonNull(keySerializer);
		this.namespaceSerializer = namespaceSerializer;
		this.basePath = basePath;
		this.checkpointPath = checkpointPath;

		RocksDB.loadLibrary();

		if (!basePath.exists()) {
			if (!basePath.mkdirs()) {
				throw new RuntimeException("Could not create RocksDB data directory.");
			}
		}

		// clean it, this will remove the last part of the path but RocksDB will recreate it
		try {
			if (rocksDbPath.exists()) {
				LOG.warn("Deleting already existing db directory {}.", rocksDbPath);
				FileUtils.deleteDirectory(rocksDbPath);
			}
		} catch (IOException e) {
			throw new RuntimeException("Error cleaning RocksDB data directory.", e);
		}

		try {
			db = RocksDB.open(options, rocksDbPath.getAbsolutePath());
		} catch (RocksDBException e) {
			throw new RuntimeException("Error while opening RocksDB instance.", e);
		}

	}

	/**
	 * Creates a new RocksDB backed state and restores from the given backup directory. After
	 * restoring the backup directory is deleted.
	 *
	 * @param keySerializer The serializer for the keys.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param basePath The path on the local system where RocksDB data should be stored.
	 * @param restorePath The path to a backup directory from which to restore RocksDb database.
	 */
	protected AbstractRocksDBState(
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			File basePath,
			String checkpointPath,
			String restorePath,
			Options options) {

		rocksDbPath = new File(basePath, "db" + UUID.randomUUID().toString());

		RocksDB.loadLibrary();

		// clean it, this will remove the last part of the path but RocksDB will recreate it
		try {
			if (rocksDbPath.exists()) {
				LOG.warn("Deleting already existing db directory {}.", rocksDbPath);
				FileUtils.deleteDirectory(rocksDbPath);
			}
		} catch (IOException e) {
			throw new RuntimeException("Error cleaning RocksDB data directory.", e);
		}

		try (BackupEngine backupEngine = BackupEngine.open(Env.getDefault(), new BackupableDBOptions(restorePath + "/"))) {
			backupEngine.restoreDbFromLatestBackup(rocksDbPath.getAbsolutePath(), rocksDbPath.getAbsolutePath(), new RestoreOptions(true));
		} catch (RocksDBException|IllegalArgumentException e) {
			throw new RuntimeException("Error while restoring RocksDB state from " + restorePath, e);
		} finally {
			try {
				FileUtils.deleteDirectory(new File(restorePath));
			} catch (IOException e) {
				LOG.error("Error cleaning up local restore directory " + restorePath, e);
			}
		}

		this.keySerializer = requireNonNull(keySerializer);
		this.namespaceSerializer = namespaceSerializer;
		this.basePath = basePath;
		this.checkpointPath = checkpointPath;

		if (!basePath.exists()) {
			if (!basePath.mkdirs()) {
				throw new RuntimeException("Could not create RocksDB data directory.");
			}
		}

		try {
			db = RocksDB.open(options, rocksDbPath.getAbsolutePath());
		} catch (RocksDBException e) {
			throw new RuntimeException("Error while opening RocksDB instance.", e);
		}
	}

	// ------------------------------------------------------------------------

	@Override
	final public void clear() {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(baos);
		try {
			writeKeyAndNamespace(out);
			byte[] key = baos.toByteArray();
			db.remove(key);
		} catch (IOException|RocksDBException e) {
			throw new RuntimeException("Error while removing entry from RocksDB", e);
		}
	}

	protected void writeKeyAndNamespace(DataOutputView out) throws IOException {
		keySerializer.serialize(currentKey, out);
		out.writeByte(42);
		namespaceSerializer.serialize(currentNamespace, out);
	}

	@Override
	final public void setCurrentKey(K currentKey) {
		this.currentKey = currentKey;
	}

	@Override
	final public void setCurrentNamespace(N namespace) {
		this.currentNamespace = namespace;
	}

	protected abstract AbstractRocksDBSnapshot<K, N, S, SD> createRocksDBSnapshot(URI backupUri, long checkpointId);

	@Override
	public final KvStateSnapshot<K, N, S, SD, RocksDBStateBackend> snapshot(final long checkpointId, long timestamp) throws Exception {

		final File localBackupPath = new File(basePath, "local-chk-" + checkpointId);
		final URI backupUri = new URI(checkpointPath + "/chk-" + checkpointId);


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
		try (BackupEngine backupEngine = BackupEngine.open(Env.getDefault(),
				backupOptions)) {
			// make sure to flush because we don't write to the write-ahead-log
			db.flush(new FlushOptions().setWaitForFlush(true));
			backupEngine.createNewBackup(db);
		}
		long endTime = System.currentTimeMillis();
		LOG.info("RocksDB (" + rocksDbPath + ") backup (synchronous part) took " + (endTime - startTime) + " ms.");

		return new AsyncRocksDBSnapshot<>(
			localBackupPath,
			backupUri,
			checkpointId,
			this);
	}

	@Override
	final public void dispose() {
		db.dispose();
		try {
			FileUtils.deleteDirectory(basePath);
		} catch (IOException e) {
			throw new RuntimeException("Error disposing RocksDB data directory.", e);
		}
	}

	protected static abstract class AbstractRocksDBSnapshot<K, N, S extends State, SD extends StateDescriptor<S, ?>>
			implements KvStateSnapshot<K, N, S, SD, RocksDBStateBackend> {
		private static final long serialVersionUID = 1L;

		private static final Logger LOG = LoggerFactory.getLogger(AbstractRocksDBSnapshot.class);

		// ------------------------------------------------------------------------
		//  Ctor parameters for RocksDB state
		// ------------------------------------------------------------------------

		/** Store it so that we can clean up in dispose() */
		protected final File basePath;

		/** Where we should put RocksDB backups */
		protected final String checkpointPath;

		// ------------------------------------------------------------------------
		//  Info about this checkpoint
		// ------------------------------------------------------------------------

		protected final URI backupUri;

		protected long checkpointId;

		// ------------------------------------------------------------------------
		//  For sanity checks
		// ------------------------------------------------------------------------

		/** Key serializer */
		protected final TypeSerializer<K> keySerializer;

		/** Namespace serializer */
		protected final TypeSerializer<N> namespaceSerializer;

		/** Hash of the StateDescriptor, for sanity checks */
		protected final SD stateDesc;

		/**
		 * Creates a new snapshot from the given state parameters.
		 */
		public AbstractRocksDBSnapshot(File basePath,
				String checkpointPath,
				URI backupUri,
				long checkpointId,
				TypeSerializer<K> keySerializer,
				TypeSerializer<N> namespaceSerializer,
				SD stateDesc) {
			
			this.basePath = basePath;
			this.checkpointPath = checkpointPath;
			this.backupUri = backupUri;
			this.checkpointId = checkpointId;

			this.stateDesc = stateDesc;
			this.keySerializer = keySerializer;
			this.namespaceSerializer = namespaceSerializer;
		}

		/**
		 * Subclasses must implement this for creating a concrete RocksDB state.
		 */
		protected abstract KvState<K, N, S, SD, RocksDBStateBackend> createRocksDBState(
				TypeSerializer<K> keySerializer,
				TypeSerializer<N> namespaceSerializer,
				SD stateDesc,
				File basePath,
				String backupPath,
				String restorePath,
				Options options) throws Exception;

		@Override
		public final KvState<K, N, S, SD, RocksDBStateBackend> restoreState(
				RocksDBStateBackend stateBackend,
				TypeSerializer<K> keySerializer,
				ClassLoader classLoader,
				long recoveryTimestamp) throws Exception {

			// validity checks
			if (!this.keySerializer.equals(keySerializer)) {
				throw new IllegalArgumentException(
					"Cannot restore the state from the snapshot with the given serializers. " +
						"State (K/V) was serialized with " +
						"(" + keySerializer + ") " +
						"now is (" + keySerializer + ")");
			}

			if (!basePath.exists()) {
				if (!basePath.mkdirs()) {
					throw new RuntimeException("Could not create RocksDB base path " + basePath);
				}
			}

			final File localBackupPath = new File(basePath, "chk-" + checkpointId);

			if (localBackupPath.exists()) {
				try {
					LOG.warn("Deleting already existing local backup directory {}.", localBackupPath);
					FileUtils.deleteDirectory(localBackupPath);
				} catch (IOException e) {
					throw new RuntimeException("Error cleaning RocksDB local backup directory.", e);
				}
			}

			HDFSCopyToLocal.copyToLocal(backupUri, basePath);
			return createRocksDBState(keySerializer, namespaceSerializer, stateDesc, basePath,
					checkpointPath, localBackupPath.getAbsolutePath(), stateBackend.getRocksDBOptions());
		}

		@Override
		public final void discardState() throws Exception {
			FileSystem fs = FileSystem.get(backupUri, HadoopFileSystem.getHadoopConfiguration());
			fs.delete(new Path(backupUri), true);
		}

		@Override
		public final long getStateSize() throws Exception {
			FileSystem fs = FileSystem.get(backupUri, HadoopFileSystem.getHadoopConfiguration());
			return fs.getContentSummary(new Path(backupUri)).getLength();
		}
	}

	/**
	 * Upon snapshotting the RocksDB backup is created synchronously. The asynchronous part is
	 * copying the backup to a (possibly) remote filesystem. This is done in {@link #materialize()}
	 * of this class.
	 */
	private static class AsyncRocksDBSnapshot<K, N, S extends State, SD extends StateDescriptor<S, ?>> extends AsynchronousKvStateSnapshot<K, N, S, SD, RocksDBStateBackend> {
		private static final long serialVersionUID = 1L;
		private final File localBackupPath;
		private final URI backupUri;
		private final long checkpointId;
		private transient AbstractRocksDBState<K, N, S, SD> state;

		public AsyncRocksDBSnapshot(File localBackupPath,
				URI backupUri,
				long checkpointId,
				AbstractRocksDBState<K, N, S, SD> state) {
				this.localBackupPath = localBackupPath;
				this.backupUri = backupUri;
				this.checkpointId = checkpointId;
			this.state = state;
		}

		@Override
		public KvStateSnapshot<K, N, S, SD, RocksDBStateBackend> materialize() throws Exception {
			try {
				long startTime = System.currentTimeMillis();
				HDFSCopyFromLocal.copyFromLocal(localBackupPath, backupUri);
				long endTime = System.currentTimeMillis();
				LOG.info("RocksDB materialization from " + localBackupPath + " to " + backupUri + " (asynchronous part) took " + (endTime - startTime) + " ms.");
				return state.createRocksDBSnapshot(backupUri, checkpointId);
			} catch (Exception e) {
				FileSystem fs = FileSystem.get(backupUri, HadoopFileSystem.getHadoopConfiguration());
				fs.delete(new Path(backupUri), true);
				throw e;
			} finally {
				FileUtils.deleteQuietly(localBackupPath);
			}
		}
	}
}

