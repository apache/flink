/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.db;

import static org.apache.flink.contrib.streaming.state.db.SQLRetrier.retry;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.OutOfCoreKvState;
import org.apache.flink.contrib.streaming.state.db.ShardedConnection.ShardedStatement;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.flink.util.InstantiationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

/**
 * 
 * Lazily fetched {@link KvState} using a SQL backend. Key-value pairs are
 * cached on heap and are lazily retrieved on access.
 * 
 */
public class LazyDbValueState<K, N, V> extends OutOfCoreKvState<K, N, V, DbStateBackend> {

	private static final Logger LOG = LoggerFactory.getLogger(LazyDbValueState.class);

	// ------------------------------------------------------

	private final boolean compact;

	// ------------------------------------------------------

	// Max number of retries for failed database operations
	private final int numSqlRetries;
	// Sleep time between two retries
	private final int sqlRetrySleep;
	// Max number of key-value pairs inserted in one batch to the database
	private final int maxInsertBatchSize;
	// Executor for compactions
	private ExecutorService executor = null;

	// Database properties
	private final DbBackendConfig conf;
	private final ShardedConnection connections;
	private final DbAdapter dbAdapter;

	// Convenience object for handling inserts to the database
	private final BatchInserter dbInserter;

	// Statements for key-lookups and inserts as prepared by the dbAdapter
	private ShardedStatement selectStatements;
	private ShardedStatement insertStatements;

	// ------------------------------------------------------

	// ------------------------------------------------------

	/**
	 * Constructor to initialize the {@link LazyDbValueState} the first time the
	 * job starts.
	 */
	public LazyDbValueState(DbStateBackend backend, String kvStateId, boolean compact, ShardedConnection cons,
			DbBackendConfig conf,
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			ValueStateDescriptor<V> stateDesc) throws IOException {
		this(backend, kvStateId, compact, cons, conf, keySerializer, namespaceSerializer, stateDesc, 0, 1);
	}

	/**
	 * Initialize the {@link LazyDbValueState} from a snapshot.
	 */
	public LazyDbValueState(DbStateBackend backend, String kvStateId, boolean compact, ShardedConnection cons,
			final DbBackendConfig conf, TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			ValueStateDescriptor<V> stateDesc, long lastCheckpointTs, long currentTs) throws IOException {

		super(backend, kvStateId, conf, keySerializer, namespaceSerializer, stateDesc, 0, lastCheckpointTs, currentTs);

		this.compact = compact;

		if (compact) {
			// Compactions will run in a separate thread in the background
			executor = Executors.newSingleThreadExecutor();
		}

		this.maxInsertBatchSize = conf.getMaxKvInsertBatchSize();
		this.conf = conf;
		this.connections = cons;
		this.dbAdapter = conf.getDbAdapter();
		this.numSqlRetries = conf.getMaxNumberOfSqlRetries();
		this.sqlRetrySleep = conf.getSleepBetweenSqlRetries();

		initDB(this.connections);

		dbInserter = new BatchInserter(connections.getNumShards());

		if (LOG.isDebugEnabled()) {
			LOG.debug("Lazy database kv-state ({}) successfully initialized", kvStateId);
		}
	}

	/**
	 * Create a table for the kvstate checkpoints if not present (based on the
	 * kvStateId) and prepare the statements used during checkpointing.
	 */
	private void initDB(final ShardedConnection cons) throws IOException {

		retry(new Callable<Void>() {
			public Void call() throws Exception {

				for (Connection con : cons.connections()) {
					dbAdapter.createKVStateTable(stateId, con);
				}

				insertStatements = cons.prepareStatement(dbAdapter.prepareKVCheckpointInsert(stateId));
				selectStatements = cons.prepareStatement(dbAdapter.prepareKeyLookup(stateId));

				return null;
			}

		}, numSqlRetries, sqlRetrySleep);
	}

	public boolean isCompactor() {
		return compact;
	}

	/**
	 * This method is used for testing purposes.
	 * 
	 * @return
	 */
	protected ExecutorService getCompactionExecutor() {
		return executor;
	}

	@Override
	public KvStateSnapshot<K, N, ValueState<V>, ValueStateDescriptor<V>, DbStateBackend> snapshotStates(
			Collection<Entry<Tuple2<K, N>, Optional<V>>> modifiedKVs,
			long checkpointId, long timestamp) throws IOException {

		// We simply write the remaining modified data to the database with the
		// checkpoint timestamp
		evictModified(modifiedKVs, lastCheckpointId, lastCheckpointTs, timestamp);
		// Return a new snapshot pointing to the current snapshot timestamp
		return new DbKvStateSnapshot<K, N, V>(stateId, timestamp, namespaceSerializer, stateDesc);
	}

	@Override
	public void evictModified(Collection<Entry<Tuple2<K, N>, Optional<V>>> KVsToEvict, long lastCheckpointId,
			long lastCheckpointTs, long currentTs) throws IOException {

		for (Entry<Tuple2<K, N>, Optional<V>> state : KVsToEvict) {
			dbInserter.add(state, currentTs);
		}
		dbInserter.flush(currentTs);
	}

	@Override
	public Optional<V> lookupLatest(final Tuple2<K, N> key, final byte[] serializedKey) throws IOException {
		// We use the adapter to lookup the key from the database
		return retry(new Callable<Optional<V>>() {
			public Optional<V> call() throws Exception {
				byte[] serializedVal = dbAdapter.lookupKey(stateId,
						selectStatements.getForKey(key), serializedKey, currentTs);

				return serializedVal != null
						? Optional.of(InstantiationUtil.deserializeFromByteArray(valueSerializer, serializedVal))
						: Optional.<V> absent();
			}
		}, numSqlRetries, sqlRetrySleep);
	}

	@Override
	public void compact(long from, long to) throws IOException {
		if (compact) {
			executor.execute(new DBCompactor(from, to));
		}
	}

	@Override
	public void dispose() {
		// We are only closing the statements here, the connection is borrowed
		// from the state backend and will be closed there.
		try {
			selectStatements.close();
		} catch (SQLException e) {
			// There is not much to do about this
		}
		try {
			insertStatements.close();
		} catch (SQLException e) {
			// There is not much to do about this
		}

		if (executor != null) {
			executor.shutdown();
		}
	}

	/**
	 * Object for handling inserts to the database by batching them together
	 * partitioned on the sharding key. The batches are written to the database
	 * when they are full or when the inserter is flushed.
	 *
	 */
	private class BatchInserter {

		// Map from shard index to the kv pairs to be inserted
		// Map<Integer, List<Tuple2<byte[], byte[]>>> inserts = new HashMap<>();

		List<Tuple2<byte[], byte[]>>[] inserts;

		@SuppressWarnings("unchecked")
		public BatchInserter(int numShards) {
			inserts = new List[numShards];
			for (int i = 0; i < numShards; i++) {
				inserts[i] = new ArrayList<>();
			}
		}

		public void add(Entry<Tuple2<K, N>, Optional<V>> next, long timestamp) throws IOException {

			Tuple2<K, N> key = next.getKey();
			V value = next.getValue().orNull();

			// Get the current partition if present or initialize empty list
			int shardIndex = connections.getShardIndex(key);

			List<Tuple2<byte[], byte[]>> insertPartition = inserts[shardIndex];

			// Add the k-v pair to the partition
			byte[] k = serializeKeyAndNamespace(key);
			byte[] v = value != null ? InstantiationUtil.serializeToByteArray(valueSerializer, value) : null;
			if (bloomFilter != null) {
				bloomFilter.put(k);
			}
			insertPartition.add(Tuple2.of(k, v));

			// If partition is full write to the database and clear
			if (insertPartition.size() == maxInsertBatchSize) {
				executeInsert(insertPartition, shardIndex, timestamp);
				insertPartition.clear();
			}
		}

		public void flush(long timestamp) throws IOException {
			// We flush all non-empty partitions
			for (int i = 0; i < inserts.length; i++) {
				List<Tuple2<byte[], byte[]>> insertPartition = inserts[i];
				if (!insertPartition.isEmpty()) {
					executeInsert(insertPartition, i, timestamp);
					insertPartition.clear();
				}
			}

		}

		private void executeInsert(List<Tuple2<byte[], byte[]>> kvPairs, int shardIndex, long timestamp)
				throws IOException {

			// Used for debugging purposes
			Long startTime = null;
			if (LOG.isDebugEnabled()) {
				startTime = System.nanoTime();
				LOG.debug("Executing batch state insert for {} shard {} with {} records", stateId,
						conf.getShardUrl(shardIndex), kvPairs.size());
			}

			dbAdapter.insertBatch(stateId, conf, connections.getForIndex(shardIndex),
					insertStatements.getForIndex(shardIndex), timestamp, kvPairs);

			if (LOG.isDebugEnabled()) {
				long insertTime = (System.nanoTime() - startTime) / 1000000;
				long keySize = 0L;
				long valueSize = 0L;
				for (Tuple2<byte[], byte[]> t : kvPairs) {
					keySize += t.f0.length;
					valueSize += t.f1.length;
				}
				keySize = keySize / 1024;
				valueSize = valueSize / 1024;
				LOG.debug(
						"Successfully inserted state batch (insertTime(ms), numRecords, totKeySize(kb), totValueSize(kb), stateName, shardUrl, sql): "
								+ "({},{},{},{},{},{},{})",
						insertTime, kvPairs.size(), keySize, valueSize, stateId, conf.getShardUrl(shardIndex),
						insertStatements.getSqlString());
			}
		}
	}

	private class DBCompactor implements Runnable {

		private long from;
		private long to;

		public DBCompactor(long from, long to) {
			this.from = from;
			this.to = to;
		}

		@Override
		public void run() {
			// We create new database connections to make sure we don't
			// interfere with the checkpointing (connections are not thread
			// safe)
			try (ShardedConnection sc = conf.createShardedConnection()) {

				LOG.info("Starting compaction for {} between {} and {}.", stateId,
						from,
						to);

				long compactionStart = System.nanoTime();

				for (final Connection c : sc.connections()) {
					SQLRetrier.retry(new Callable<Void>() {
						@Override
						public Void call() throws Exception {
							dbAdapter.compactKvStates(stateId, c, from, to);
							return null;
						}
					}, numSqlRetries, sqlRetrySleep);
				}

				LOG.info("State succesfully compacted for {} between {} and {} in {} ms.", stateId,
						from,
						to,
						(System.nanoTime() - compactionStart) / 1000000);
			} catch (SQLException | IOException e) {
				LOG.warn("State compaction failed due: {}", e);
			}
		}

	}

	/**
	 * Snapshot that stores a specific checkpoint timestamp and state id, and
	 * also rolls back the database to that point upon restore. The rollback is
	 * done by removing all state checkpoints that have timestamps between the
	 * checkpoint and recovery timestamp.
	 *
	 */
	private static class DbKvStateSnapshot<K, N, V>
			implements KvStateSnapshot<K, N, ValueState<V>, ValueStateDescriptor<V>, DbStateBackend> {

		private static final long serialVersionUID = 1L;

		private final String kvStateId;
		private final long checkpointTimestamp;

		/** Namespace Serializer */
		private final TypeSerializer<N> namespaceSerializer;

		/** StateDescriptor, for sanity checks */
		private final ValueStateDescriptor<V> stateDesc;

		public DbKvStateSnapshot(String kvStateId, long checkpointTimestamp, TypeSerializer<N> namespaceSerializer,
				ValueStateDescriptor<V> stateDesc) {
			this.checkpointTimestamp = checkpointTimestamp;
			this.kvStateId = kvStateId;
			this.namespaceSerializer = namespaceSerializer;
			this.stateDesc = stateDesc;
		}

		@Override
		public KvState<K, N, ValueState<V>, ValueStateDescriptor<V>, DbStateBackend> restoreState(
				final DbStateBackend stateBackend, TypeSerializer<K> keySerializer, ClassLoader classLoader,
				final long recoveryTimestamp) throws Exception {

			// Validate timing assumptions
			if (recoveryTimestamp <= checkpointTimestamp) {
				throw new RuntimeException(
						"Recovery timestamp is smaller or equal to checkpoint timestamp. "
								+ "This might happen if the job was started with a new JobManager "
								+ "and the clocks got really out of sync.");
			}

			final DbBackendConfig conf = stateBackend.getConfiguration();

			// First we clean up the states written by partially failed
			// snapshots
			retry(new Callable<Void>() {
				public Void call() throws Exception {

					// We need to perform cleanup on all shards to be safe here
					for (Connection c : stateBackend.getConnections().connections()) {
						conf.getDbAdapter().cleanupFailedCheckpoints(
								conf, kvStateId, c, checkpointTimestamp, recoveryTimestamp);
					}

					return null;
				}
			}, conf.getMaxNumberOfSqlRetries(), conf.getSleepBetweenSqlRetries());

			boolean compactor = stateBackend.getEnvironment().getTaskInfo().getIndexOfThisSubtask() == 0;

			// Restore the KvState
			LazyDbValueState<K, N, V> restored = new LazyDbValueState<K, N, V>(stateBackend, kvStateId, compactor,
					stateBackend.getConnections(), conf, keySerializer, namespaceSerializer,
					stateDesc, checkpointTimestamp, recoveryTimestamp + 1);

			if (LOG.isDebugEnabled()) {
				LOG.debug("KV state({},{}) restored.", kvStateId, recoveryTimestamp);
			}

			return restored;
		}

		@Override
		public void discardState() throws Exception {
			// Don't discard, it will be compacted by the LazyDbKvState
		}

		@Override
		public long getStateSize() throws Exception {
			// Because the state is serialzied in a lazy fashion we don't know
			// the size of the state yet.
			return 0;
		}
	}

}
