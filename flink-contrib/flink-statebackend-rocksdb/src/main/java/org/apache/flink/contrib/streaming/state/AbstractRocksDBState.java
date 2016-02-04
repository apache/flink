/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
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
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.flink.util.HDFSCopyFromLocal;
import org.apache.flink.util.HDFSCopyToLocal;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.rocksdb.BackupEngine;
import org.rocksdb.BackupableDBOptions;
import org.rocksdb.Env;
import org.rocksdb.Options;
import org.rocksdb.RestoreOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.StringAppendOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;

import static java.util.Objects.requireNonNull;

/**
 * Base class for {@link State} implementations that store state in a RocksDB database.
 *
 * <p>This base class is responsible for setting up the RocksDB database, for
 * checkpointing/restoring the database and for disposal in the {@link #dispose()} method. The
 * concrete subclasses just use the RocksDB handle to store/retrieve state.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <S> The type of {@link State}.
 * @param <SD> The type of {@link StateDescriptor}.
 * @param <Backend> The type of the backend that snapshots this key/value state.
 */
public abstract class AbstractRocksDBState<K, N, S extends State, SD extends StateDescriptor<S, ?>, Backend extends AbstractStateBackend>
	implements KvState<K, N, S, SD, Backend>, State {

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
	protected final File dbPath;

	protected final String checkpointPath;

	/** Our RocksDB instance */
	protected final RocksDB db;

	/**
	 * Creates a new RocksDB backed state.
	 *
	 * @param keySerializer The serializer for the keys.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param dbPath The path on the local system where RocksDB data should be stored.
	 */
	protected AbstractRocksDBState(TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		File dbPath,
		String checkpointPath) {
		this.keySerializer = requireNonNull(keySerializer);
		this.namespaceSerializer = namespaceSerializer;
		this.dbPath = dbPath;
		this.checkpointPath = checkpointPath;

		RocksDB.loadLibrary();

		Options options = new Options().setCreateIfMissing(true);
		options.setMergeOperator(new StringAppendOperator());

		if (!dbPath.exists()) {
			if (!dbPath.mkdirs()) {
				throw new RuntimeException("Could not create RocksDB data directory.");
			}
		}

		// clean it, this will remove the last part of the path but RocksDB will recreate it
		try {
			File db = new File(dbPath, "db");
			LOG.warn("Deleting already existing db directory {}.", db);
			FileUtils.deleteDirectory(db);
		} catch (IOException e) {
			throw new RuntimeException("Error cleaning RocksDB data directory.", e);
		}

		try {
			db = RocksDB.open(options, new File(dbPath, "db").getAbsolutePath());
		} catch (RocksDBException e) {
			throw new RuntimeException("Error while opening RocksDB instance.", e);
		}

		options.dispose();

	}

	/**
	 * Creates a new RocksDB backed state and restores from the given backup directory. After
	 * restoring the backup directory is deleted.
	 *
	 * @param keySerializer The serializer for the keys.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param dbPath The path on the local system where RocksDB data should be stored.
	 * @param restorePath The path to a backup directory from which to restore RocksDb database.
	 */
	protected AbstractRocksDBState(TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		File dbPath,
		String checkpointPath,
		String restorePath) {

		RocksDB.loadLibrary();

		try (BackupEngine backupEngine = BackupEngine.open(Env.getDefault(), new BackupableDBOptions(restorePath + "/"))) {
			backupEngine.restoreDbFromLatestBackup(new File(dbPath, "db").getAbsolutePath(), new File(dbPath, "db").getAbsolutePath(), new RestoreOptions(true));
			FileUtils.deleteDirectory(new File(restorePath));
		} catch (RocksDBException|IOException|IllegalArgumentException e) {
			throw new RuntimeException("Error while restoring RocksDB state from " + restorePath, e);
		}

		this.keySerializer = requireNonNull(keySerializer);
		this.namespaceSerializer = namespaceSerializer;
		this.dbPath = dbPath;
		this.checkpointPath = checkpointPath;

		Options options = new Options().setCreateIfMissing(true);
		options.setMergeOperator(new StringAppendOperator());

		if (!dbPath.exists()) {
			if (!dbPath.mkdirs()) {
				throw new RuntimeException("Could not create RocksDB data directory.");
			}
		}

		try {
			db = RocksDB.open(options, new File(dbPath, "db").getAbsolutePath());
		} catch (RocksDBException e) {
			throw new RuntimeException("Error while opening RocksDB instance.", e);
		}

		options.dispose();
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

	protected abstract KvStateSnapshot<K, N, S, SD, Backend> createRocksDBSnapshot(URI backupUri, long checkpointId);

	@Override
	final public KvStateSnapshot<K, N, S, SD, Backend> snapshot(
		long checkpointId,
		long timestamp) throws Exception {
		boolean success = false;

		final File localBackupPath = new File(dbPath, "backup-" + checkpointId);
		final URI backupUri = new URI(checkpointPath + "/chk-" + checkpointId);

		try {
			if (!localBackupPath.exists()) {
				if (!localBackupPath.mkdirs()) {
					throw new RuntimeException("Could not create local backup path " + localBackupPath);
				}
			}

			try (BackupEngine backupEngine = BackupEngine.open(Env.getDefault(), new BackupableDBOptions(localBackupPath.getAbsolutePath()))) {
				backupEngine.createNewBackup(db);
			}

			HDFSCopyFromLocal.copyFromLocal(localBackupPath, backupUri);
			KvStateSnapshot<K, N, S, SD, Backend> result = createRocksDBSnapshot(backupUri, checkpointId);
			success = true;
			return result;
		} finally {
			FileUtils.deleteDirectory(localBackupPath);
			if (!success) {
				FileSystem fs = FileSystem.get(backupUri, new Configuration());
				fs.delete(new Path(backupUri), true);
			}
		}
	}

	@Override
	final public void dispose() {
		db.dispose();
		try {
			FileUtils.deleteDirectory(dbPath);
		} catch (IOException e) {
			throw new RuntimeException("Error disposing RocksDB data directory.", e);
		}
	}

	public static abstract class AbstractRocksDBSnapshot<K, N, S extends State, SD extends StateDescriptor<S, ?>, Backend extends AbstractStateBackend> implements KvStateSnapshot<K, N, S, SD, Backend> {
		private static final long serialVersionUID = 1L;

		private static final Logger LOG = LoggerFactory.getLogger(AbstractRocksDBSnapshot.class);

		// ------------------------------------------------------------------------
		//  Ctor parameters for RocksDB state
		// ------------------------------------------------------------------------

		/** Store it so that we can clean up in dispose() */
		protected final File dbPath;

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

		public AbstractRocksDBSnapshot(File dbPath,
			String checkpointPath,
			URI backupUri,
			long checkpointId,
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			SD stateDesc) {
			this.dbPath = dbPath;
			this.checkpointPath = checkpointPath;
			this.backupUri = backupUri;
			this.checkpointId = checkpointId;

			this.stateDesc = stateDesc;
			this.keySerializer = keySerializer;
			this.namespaceSerializer = namespaceSerializer;
		}

		protected abstract KvState<K, N, S, SD, Backend> createRocksDBState(TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			SD stateDesc,
			File dbPath,
			String backupPath,
			String restorePath) throws Exception;

		@Override
		public final KvState<K, N, S, SD, Backend> restoreState(
			Backend stateBackend,
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

			if (!dbPath.exists()) {
				if (!dbPath.mkdirs()) {
					throw new RuntimeException("Could not create RocksDB base path " + dbPath);
				}
			}

			FileSystem fs = FileSystem.get(backupUri, new Configuration());

			final File localBackupPath = new File(dbPath, "chk-" + checkpointId);

			if (localBackupPath.exists()) {
				try {
					LOG.warn("Deleting already existing local backup directory {}.", localBackupPath);
					FileUtils.deleteDirectory(localBackupPath);
				} catch (IOException e) {
					throw new RuntimeException("Error cleaning RocksDB local backup directory.", e);
				}
			}

			HDFSCopyToLocal.copyToLocal(backupUri, dbPath);
			return createRocksDBState(keySerializer, namespaceSerializer, stateDesc, dbPath, checkpointPath, localBackupPath.getAbsolutePath());
		}

		@Override
		public final void discardState() throws Exception {
			FileSystem fs = FileSystem.get(backupUri, new Configuration());
			fs.delete(new Path(backupUri), true);
		}

		@Override
		public final long getStateSize() throws Exception {
			return 0;
		}
	}
}

