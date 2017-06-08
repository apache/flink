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
import org.apache.commons.io.IOUtils;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

/**
 * Base class for {@link State} implementations that store state in a Redis database.
 *
 * <p>This base class is responsible for setting up the Redis database, for
 * checkpointing/restoring the database and for disposal in the {@link #dispose()} method. The
 * concrete subclasses just use the Redis handle to store/retrieve state.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <S> The type of {@link State}.
 * @param <SD> The type of {@link StateDescriptor}.
 * @param <Backend> The type of the backend that snapshots this key/value state.
 */
public abstract class AbstractRedisState<K, N, S extends State, SD extends StateDescriptor<S, ?>, Backend extends AbstractStateBackend>
	implements KvState<K, N, S, SD, Backend>, State {

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

	/** Our Redis instance */
	protected Process redisServerProcess;
	protected final BinaryJedis db;

	/**
	 * Creates a new Redis backed state.
	 *
	 * @param redisExecPath The path on the local system where Redis executable file can be found.
	 * @param port The port to start Redis server
	 * @param keySerializer The serializer for the keys.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param dbPath The path on the local system where Redis data should be stored.
	 */
	protected AbstractRedisState(String redisExecPath, int port, TypeSerializer<K> keySerializer, TypeSerializer<N> namespaceSerializer, File dbPath, String checkpointPath) {
		this.keySerializer = keySerializer;
		this.namespaceSerializer = namespaceSerializer;
		this.dbPath = dbPath;
		this.checkpointPath = checkpointPath;

		if (!dbPath.exists()) {
			if (!dbPath.mkdirs()) {
				throw new RuntimeException("Could not create Redis data directory.");
			}
		}

		try {
			startRedisServer(redisExecPath, "--port", String.valueOf(port), "--dir", dbPath.getAbsolutePath());
		} catch (IOException e) {
			throw new RuntimeException("Could not start Redis server", e);
		}

		try {
			db = new BinaryJedis("localhost", port);
			db.ping();
		} catch (JedisConnectionException e) {
			throw new RuntimeException("Error while opening connection to Redis server", e);
		}
	}

	/**
	 * Creates a new Redis backed state and restores from the given backup directory. After
	 * restoring the backup directory is deleted.
	 *
	 * @param redisExecPath The path on the local system where Redis executable file can be found.
	 * @param port The port to start Redis server
	 * @param keySerializer The serializer for the keys.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param dbPath The path on the local system where RocksDB data should be stored.
	 * @param restorePath The path to a backup directory from which to restore Redis database.
	 */
	protected AbstractRedisState(String redisExecPath, int port, TypeSerializer<K> keySerializer, TypeSerializer<N> namespaceSerializer, File dbPath, String checkpointPath, String restorePath) {
		try {
			HDFSCopyToLocal.copyToLocal(new URI(restorePath + "/dump.rdb"), dbPath);
		} catch (Exception e) {
			throw new RuntimeException("Error while restoring Redis state from " + restorePath, e);
		}

		try {
			startRedisServer(redisExecPath, "--port", String.valueOf(port), "--dir", dbPath.getAbsolutePath());
		} catch (IOException e) {
			throw new RuntimeException("Could not start Redis server");
		}

		this.keySerializer = keySerializer;
		this.namespaceSerializer = namespaceSerializer;
		this.dbPath = dbPath;
		this.checkpointPath = checkpointPath;

		if (!dbPath.exists()) {
			if (!dbPath.mkdirs()) {
				throw new RuntimeException("Could not create Redis data directory.");
			}
		}

		try {
			db = new BinaryJedis("localhost", port);
			db.ping();
		} catch (JedisConnectionException e) {
			throw new RuntimeException("Error while opening connection to Redis server", e);
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
			db.del(key);
		} catch (IOException e) {
			throw new RuntimeException("Error while removing entry from Redis", e);
		}
	}

	protected void writeKeyAndNamespace(DataOutputView out) throws IOException {
		keySerializer.serialize(currentKey, out);
		out.writeByte(42);
		namespaceSerializer.serialize(currentNamespace, out);
	}

	@Override
	public void setCurrentKey(K currentKey) {
		this.currentKey = currentKey;
	}

	@Override
	public void setCurrentNamespace(N currentNamespace) {
		this.currentNamespace = currentNamespace;
	}

	protected abstract KvStateSnapshot<K, N, S, SD, Backend> createRedisSnapshot(URI backupUri, long checkpointId);

	@Override
	public KvStateSnapshot<K, N, S, SD, Backend> snapshot(long checkpointId, long timestamp) throws Exception {
		boolean success = false;

		final File localBackupPath = new File(dbPath, "backup-" + checkpointId);
		final URI backupUri = new URI(checkpointPath + "/chk-" + checkpointId);

		try {
			if (!localBackupPath.exists()) {
				if (!localBackupPath.mkdirs()) {
					throw new RuntimeException("Could not create local backup path " + localBackupPath);
				}
			}

			db.configSet("dir".getBytes(), localBackupPath.getAbsolutePath().getBytes());
			db.save();

			HDFSCopyFromLocal.copyFromLocal(localBackupPath, backupUri);
			KvStateSnapshot<K, N, S, SD, Backend> result = createRedisSnapshot(backupUri, checkpointId);
			success = true;
			return result;
		}finally {
			FileUtils.deleteDirectory(localBackupPath);
			if (!success) {
				FileSystem fs = FileSystem.get(backupUri, new Configuration());
				fs.delete(new Path(backupUri), true);
			}
		}
	}

	@Override
	public void dispose() {
		db.disconnect();
		stopRedisServer();
		try {
			FileUtils.deleteDirectory(dbPath);
		} catch (IOException e) {
			throw new RuntimeException("Error disposing Redis data directory.", e);
		}
	}

	/**
	 * Starts the Redis server
	 *
	 * @param args
	 * @throws IOException
     */
	private synchronized void startRedisServer(String... args) throws IOException {
		List<String> argsList = Arrays.asList(args);
		File executable = new File(argsList.get(0));
		ProcessBuilder pb = new ProcessBuilder(argsList);
		pb.directory(executable.getParentFile());

		redisServerProcess = pb.start();

		BufferedReader reader = new BufferedReader(new InputStreamReader(redisServerProcess.getInputStream()));
		try {
			String outputLine;
			do {
				outputLine = reader.readLine();
				if (outputLine == null) {
					//Something goes wrong. Stream is ended before server was activated.
					throw new RuntimeException("Can't start redis server. Check logs for details.");
				}
			} while (!outputLine.matches(".*The server is now ready to accept connections on port.*"));
		} finally {
			IOUtils.closeQuietly(reader);
		}
	}

	/**
	 * Stops the Redis server
	 */
	private synchronized void stopRedisServer() {
		try {
			redisServerProcess.destroy();
			redisServerProcess.waitFor();
		} catch (InterruptedException e) {
			throw new RuntimeException("Failed to stop redis instance", e);
		}
	}

	public static abstract class AbstractRedisSnapshot<K, N, S extends State, SD extends StateDescriptor<S, ?>, Backend extends AbstractStateBackend> implements KvStateSnapshot<K, N, S, SD, Backend> {
		private static final long serialVersionUID = 1L;

		private static final Logger LOG = LoggerFactory.getLogger(AbstractRedisSnapshot.class);

		// ------------------------------------------------------------------------
		//  Ctor parameters for Redis state
		// ------------------------------------------------------------------------

		/** Store it so that we can clean up in dispose() */
		protected final File dbPath;

		/** Where we should put Redis backups */
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

		public AbstractRedisSnapshot(File dbPath, String checkpointPath, URI backupUri, long checkpointId, TypeSerializer<K> keySerializer, TypeSerializer<N> namespaceSerializer, SD stateDesc) {
			this.dbPath = dbPath;
			this.checkpointPath = checkpointPath;
			this.backupUri = backupUri;
			this.checkpointId = checkpointId;

			this.stateDesc = stateDesc;
			this.keySerializer = keySerializer;
			this.namespaceSerializer = namespaceSerializer;
		}

		protected abstract KvState<K, N, S, SD, Backend> createRedisState(TypeSerializer<K> keySerializer, TypeSerializer<N> namespaceSerializer, SD stateDesc, File dbPath, String backupPath, String restorePath) throws Exception;

		@Override
		public KvState<K, N, S, SD, Backend> restoreState(Backend stateBackend, TypeSerializer<K> keySerializer, ClassLoader classLoader, long recoveryTimestamp) throws Exception {

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
					throw new RuntimeException("Could not create Redis base path " + dbPath);
				}
			}

			FileSystem fs = FileSystem.get(backupUri, new Configuration());

			final File localBackupPath = new File(dbPath, "chk-" + checkpointId);

			if (localBackupPath.exists()) {
				try {
					LOG.warn("Deleting already existing local backup directory {}.", localBackupPath);
					FileUtils.deleteDirectory(localBackupPath);
				} catch (IOException e) {
					throw new RuntimeException("Error cleaning Redis local backup directory.", e);
				}
			}

			HDFSCopyToLocal.copyToLocal(backupUri, dbPath);
			return createRedisState(keySerializer, namespaceSerializer, stateDesc, dbPath, checkpointPath, localBackupPath.getAbsolutePath());
		}

		@Override
		public void discardState() throws Exception {
			FileSystem fs = FileSystem.get(backupUri, new Configuration());
			fs.delete(new Path(backupUri), true);
		}

		@Override
		public long getStateSize() throws Exception {
			return 0;
		}
	}
}
