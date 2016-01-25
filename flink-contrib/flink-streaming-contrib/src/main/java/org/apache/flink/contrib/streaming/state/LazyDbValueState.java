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

package org.apache.flink.contrib.streaming.state;

import static org.apache.flink.contrib.streaming.state.SQLRetrier.retry;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.ShardedConnection.ShardedStatement;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.flink.runtime.state.CheckpointListener;
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
public class LazyDbValueState<K, N, V>
	implements KvState<K, N, ValueState<V>, ValueStateDescriptor<V>, DbStateBackend>, ValueState<V>, CheckpointListener {

	private static final Logger LOG = LoggerFactory.getLogger(LazyDbValueState.class);

	// ------------------------------------------------------

	// Unique id for this state (appID_operatorID_stateName)
	private final String kvStateId;
	private final boolean compact;

	private K currentKey;
	private N currentNamespace;
	private final V defaultValue;

	private final TypeSerializer<K> keySerializer;
	private final TypeSerializer<N> namespaceSerializer;
	private final TypeSerializer<V> valueSerializer;
	private final ValueStateDescriptor<V> stateDesc;

	// ------------------------------------------------------

	// Max number of retries for failed database operations
	private final int numSqlRetries;
	// Sleep time between two retries
	private final int sqlRetrySleep;
	// Max number of key-value pairs inserted in one batch to the database
	private final int maxInsertBatchSize;
	// We will do database compaction every so many checkpoints
	private final int compactEvery;
	// Executor for automatic compactions
	private ExecutorService executor = null;

	// Database properties
	private final DbBackendConfig conf;
	private final ShardedConnection connections;
	private final DbAdapter dbAdapter;

	// Convenience object for handling inserts to the database
	private final BatchInserter batchInsert;

	// Statements for key-lookups and inserts as prepared by the dbAdapter
	private ShardedStatement selectStatements;
	private ShardedStatement insertStatements;

	// ------------------------------------------------------

	// LRU cache for the key-value states backed by the database
	private final StateCache cache;

	private long nextTs;
	private Map<Long, Long> completedCheckpoints = new HashMap<>();

	private volatile long lastCompactedTs;

	// ------------------------------------------------------

	/**
	 * Constructor to initialize the {@link LazyDbValueState} the first time the
	 * job starts.
	 */
	public LazyDbValueState(String kvStateId,
		boolean compact,
		ShardedConnection cons,
		DbBackendConfig conf,
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		ValueStateDescriptor<V> stateDesc) throws IOException {
		this(kvStateId, compact, cons, conf, keySerializer, namespaceSerializer, stateDesc, 1, 0);
	}

	/**
	 * Initialize the {@link LazyDbValueState} from a snapshot.
	 */
	public LazyDbValueState(String kvStateId,
		boolean compact,
		ShardedConnection cons,
		final DbBackendConfig conf,
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		ValueStateDescriptor<V> stateDesc,
		long nextTs,
		long lastCompactedTs) throws IOException {

		this.kvStateId = kvStateId;
		this.compact = compact;
		if (compact) {
			// Compactions will run in a seperate thread
			executor = Executors.newSingleThreadExecutor();
		}

		this.keySerializer = keySerializer;
		this.namespaceSerializer = namespaceSerializer;
		this.valueSerializer = stateDesc.getSerializer();
		this.defaultValue = stateDesc.getDefaultValue();
		this.stateDesc = stateDesc;

		this.maxInsertBatchSize = conf.getMaxKvInsertBatchSize();
		this.conf = conf;
		this.connections = cons;
		this.dbAdapter = conf.getDbAdapter();
		this.compactEvery = conf.getKvStateCompactionFrequency();
		this.numSqlRetries = conf.getMaxNumberOfSqlRetries();
		this.sqlRetrySleep = conf.getSleepBetweenSqlRetries();

		this.nextTs = nextTs;
		this.lastCompactedTs = lastCompactedTs;

		this.cache = new StateCache(conf.getKvCacheSize(), conf.getNumElementsToEvict());

		initDB(this.connections);

		batchInsert = new BatchInserter(connections.getNumShards());

		if (LOG.isDebugEnabled()) {
			LOG.debug("Lazy database kv-state ({}) successfully initialized", kvStateId);
		}
	}

	@Override
	public void setCurrentKey(K key) {
		this.currentKey = key;
	}

	@Override
	public void setCurrentNamespace(N namespace) {
		this.currentNamespace = namespace;
	}

	@Override
	public void update(V value) throws IOException {
		try {
			cache.put(Tuple2.of(currentKey, currentNamespace), Optional.fromNullable(value));
		} catch (RuntimeException e) {
			// We need to catch the RuntimeExceptions thrown in the StateCache
			// methods here
			throw new IOException(e);
		}
	}

	@Override
	public V value() throws IOException {
		try {
			// We get the value from the cache (which will automatically load it
			// from the database if necessary). If null, we return a copy of the
			// default value
			V val = cache.get(Tuple2.of(currentKey, currentNamespace)).orNull();
			return val != null ? val : copyDefault();
		} catch (RuntimeException e) {
			// We need to catch the RuntimeExceptions thrown in the StateCache
			// methods here
			throw new IOException(e);
		}
	}

	@Override
	public void clear() {
		cache.put(Tuple2.of(currentKey, currentNamespace), Optional.<V>fromNullable(null));
	}

	@Override
	public DbKvStateSnapshot<K, N, V> snapshot(long checkpointId, long timestamp) throws IOException {

		// Validate timing assumptions
		if (timestamp <= nextTs) {
			throw new RuntimeException("Checkpoint timestamp is smaller than previous ts + 1, "
					+ "this should not happen.");
		}

		// If there are any modified states we perform the inserts
		if (!cache.modified.isEmpty()) {
			// We insert the modified elements to the database with the current
			// timestamp then clear the modified states
			for (Entry<Tuple2<K, N>, Optional<V>> state : cache.modified.entrySet()) {
				batchInsert.add(state, timestamp);
			}
			batchInsert.flush(timestamp);
			cache.modified.clear();
		} else if (compact) {
			// Otherwise we call the keep alive method to avoid dropped
			// connections (only call this on the compactor instance)
			for (final Connection c : connections.connections()) {
				SQLRetrier.retry(new Callable<Void>() {
					@Override
					public Void call() throws Exception {
						dbAdapter.keepAlive(c);
						return null;
					}
				}, numSqlRetries, sqlRetrySleep);
			}
		}

		nextTs = timestamp + 1;
		completedCheckpoints.put(checkpointId, timestamp);
		return new DbKvStateSnapshot<K, N, V>(kvStateId, timestamp, lastCompactedTs, namespaceSerializer, stateDesc);
	}

	/**
	 * Returns the number of elements currently stored in the task's cache. Note
	 * that the number of elements in the database is not counted here.
	 */
	public int size() {
		return cache.size();
	}

	/**
	 * Return a copy the default value or null if the default was null.
	 * 
	 */
	private V copyDefault() {
		return defaultValue != null ? valueSerializer.copy(defaultValue) : null;
	}

	/**
	 * Create a table for the kvstate checkpoints (based on the kvStateId) and
	 * prepare the statements used during checkpointing.
	 */
	private void initDB(final ShardedConnection cons) throws IOException {

		retry(new Callable<Void>() {
			public Void call() throws Exception {

				for (Connection con : cons.connections()) {
					dbAdapter.createKVStateTable(kvStateId, con);
				}

				insertStatements = cons.prepareStatement(dbAdapter.prepareKVCheckpointInsert(kvStateId));
				selectStatements = cons.prepareStatement(dbAdapter.prepareKeyLookup(kvStateId));

				return null;
			}

		}, numSqlRetries, sqlRetrySleep);
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) {
		final Long ts = completedCheckpoints.remove(checkpointId);
		if (ts == null) {
			LOG.warn("Complete notification for missing checkpoint: " + checkpointId);
		} else {
			// If compaction is turned on we compact on the compactor subtask
			// asynchronously in the background
			if (compactEvery > 0 && compact && checkpointId % compactEvery == 0) {
				executor.execute(new Compactor(ts));
			}
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
	 * Return the Map of cached states.
	 * 
	 */
	public Map<Tuple2<K, N>, Optional<V>> getStateCache() {
		return cache;
	}

	/**
	 * Return the Map of modified states that hasn't been written to the
	 * database yet.
	 * 
	 */
	public Map<Tuple2<K, N>, Optional<V>> getModified() {
		return cache.modified;
	}

	/**
	 * Used for testing purposes
	 */
	public boolean isCompactor() {
		return compact;
	}

	/**
	 * Used for testing purposes
	 */
	public ExecutorService getExecutor() {
		return executor;
	}

	/**
	 * Snapshot that stores a specific checkpoint timestamp and state id, and
	 * also rolls back the database to that point upon restore. The rollback is
	 * done by removing all state checkpoints that have timestamps between the
	 * checkpoint and recovery timestamp.
	 *
	 */
	private static class DbKvStateSnapshot<K, N, V> implements KvStateSnapshot<K, N, ValueState<V>, ValueStateDescriptor<V>, DbStateBackend> {

		private static final long serialVersionUID = 1L;

		private final String kvStateId;
		private final long checkpointTimestamp;
		private final long lastCompactedTimestamp;

		/** Namespace Serializer */
		private final TypeSerializer<N> namespaceSerializer;

		/** StateDescriptor, for sanity checks */
		private final ValueStateDescriptor<V> stateDesc;

		public DbKvStateSnapshot(String kvStateId,
			long checkpointTimestamp,
			long lastCompactedTs,
			TypeSerializer<N> namespaceSerializer,
			ValueStateDescriptor<V> stateDesc) {
			this.checkpointTimestamp = checkpointTimestamp;
			this.kvStateId = kvStateId;
			this.lastCompactedTimestamp = lastCompactedTs;
			this.namespaceSerializer = namespaceSerializer;
			this.stateDesc = stateDesc;
		}

		@Override
		public KvState<K, N, ValueState<V>, ValueStateDescriptor<V>, DbStateBackend> restoreState(
			final DbStateBackend stateBackend,
			TypeSerializer<K> keySerializer,
			ClassLoader classLoader,
			final long recoveryTimestamp) throws Exception {

			// Validate timing assumptions
			if (recoveryTimestamp <= checkpointTimestamp) {
				throw new RuntimeException(
						"Recovery timestamp is smaller or equal to checkpoint timestamp. "
								+ "This might happen if the job was started with a new JobManager "
								+ "and the clocks got really out of sync.");
			}

			// First we clean up the states written by partially failed
			// snapshots
			retry(new Callable<Void>() {
				public Void call() throws Exception {

					// We need to perform cleanup on all shards to be safe here
					for (Connection c : stateBackend.getConnections().connections()) {
						stateBackend.getConfiguration().getDbAdapter().cleanupFailedCheckpoints(kvStateId,
								c, checkpointTimestamp, recoveryTimestamp);
					}

					return null;
				}
			}, stateBackend.getConfiguration().getMaxNumberOfSqlRetries(),
					stateBackend.getConfiguration().getSleepBetweenSqlRetries());

			boolean cleanup = stateBackend.getEnvironment().getTaskInfo().getIndexOfThisSubtask() == 0;

			// Restore the KvState
			LazyDbValueState<K, N, V> restored = new LazyDbValueState<>(kvStateId,
				cleanup,
				stateBackend.getConnections(),
				stateBackend.getConfiguration(),
				keySerializer,
				namespaceSerializer,
				stateDesc,
				recoveryTimestamp,
				lastCompactedTimestamp);

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

	/**
	 * LRU cache implementation for storing the key-value states. When the cache
	 * is full elements are not evicted one by one but are evicted in a batch
	 * defined by the evictionSize parameter.
	 * <p>
	 * Keys not found in the cached will be retrieved from the underlying
	 * database
	 */
	private final class StateCache extends LinkedHashMap<Tuple2<K, N>, Optional<V>> {
		private static final long serialVersionUID = 1L;

		private final int cacheSize;
		private final int evictionSize;

		// We keep track the state modified since the last checkpoint
		private final Map<Tuple2<K, N>, Optional<V>> modified = new HashMap<>();

		public StateCache(int cacheSize, int evictionSize) {
			super(cacheSize, 0.75f, true);
			this.cacheSize = cacheSize;
			this.evictionSize = evictionSize;
		}

		@Override
		public Optional<V> put(Tuple2<K, N> key, Optional<V> value) {
			// Put kv pair in the cache and evict elements if the cache is full
			Optional<V> old = super.put(key, value);
			modified.put(key, value);
			evictIfFull();
			return old;
		}

		@SuppressWarnings("unchecked")
		@Override
		public Optional<V> get(Object key) {
			// First we check whether the value is cached
			Optional<V> value = super.get(key);
			if (value == null) {
				// If it doesn't try to load it from the database
				value = Optional.fromNullable(getFromDatabaseOrNull((Tuple2<K, N>) key));
				put((Tuple2<K, N>) key, value);
			}
			return value;
		}

		@Override
		protected boolean removeEldestEntry(Entry<Tuple2<K, N>, Optional<V>> eldest) {
			// We need to remove elements manually if the cache becomes full, so
			// we always return false here.
			return false;
		}

		/**
		 * Fetch the current value from the database if exists or return null.
		 * 
		 * @param key
		 * @return The value corresponding to the key and the last checkpointid
		 *         from the database if exists or null.
		 */
		private V getFromDatabaseOrNull(final Tuple2<K, N> key) {
			try {
				return retry(new Callable<V>() {
					public V call() throws Exception {
						ByteArrayOutputStream baos = new ByteArrayOutputStream();
						DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(baos);
						keySerializer.serialize(key.f0, out);
						namespaceSerializer.serialize(key.f1, out);
						out.close();

						// We lookup using the adapter and serialize/deserialize
						// with the TypeSerializers
						byte[] serializedVal = dbAdapter.lookupKey(kvStateId,
								selectStatements.getForKey(key.f0), baos.toByteArray(), nextTs);

						return serializedVal != null
								? InstantiationUtil.deserializeFromByteArray(valueSerializer, serializedVal) : null;
					}
				}, numSqlRetries, sqlRetrySleep);
			} catch (IOException e) {
				// We need to re-throw this exception to conform to the map
				// interface, we will catch this when we call the the put/get
				throw new RuntimeException(e);
			}
		}

		/**
		 * If the cache is full we remove the evictionSize least recently
		 * accessed elements and write them to the database if they were
		 * modified since the last checkpoint.
		 */
		private void evictIfFull() {
			if (size() > cacheSize) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("State cache is full for {}, evicting {} elements.", kvStateId, evictionSize);
				}
				try {
					int numEvicted = 0;

					Iterator<Entry<Tuple2<K, N>, Optional<V>>> entryIterator = entrySet().iterator();
					while (numEvicted++ < evictionSize && entryIterator.hasNext()) {

						Entry<Tuple2<K, N>, Optional<V>> next = entryIterator.next();

						// We only need to write to the database if modified
						if (modified.remove(next.getKey()) != null) {
							batchInsert.add(next, nextTs);
						}

						entryIterator.remove();
					}

					batchInsert.flush(nextTs);

				} catch (IOException e) {
					// We need to re-throw this exception to conform to the map
					// interface, we will catch this when we call the the
					// put/get
					throw new RuntimeException(e);
				}
			}
		}

		@Override
		public void putAll(Map<? extends Tuple2<K, N>, ? extends Optional<V>> m) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void clear() {
			throw new UnsupportedOperationException();
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

			K key = next.getKey().f0;
			N namespace = next.getKey().f1;
			V value = next.getValue().orNull();

			// Get the current partition if present or initialize empty list
			int shardIndex = connections.getShardIndex(key);

			List<Tuple2<byte[], byte[]>> insertPartition = inserts[shardIndex];

			// Add the k-v pair to the partition
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(baos);
			keySerializer.serialize(key, out);
			namespaceSerializer.serialize(namespace, out);
			out.close();

			byte[] kn = baos.toByteArray();
			byte[] v = value != null ? InstantiationUtil.serializeToByteArray(valueSerializer, value) : null;
			insertPartition.add(Tuple2.of(kn, v));

			// If partition is full write to the database and clear
			if (insertPartition.size() == maxInsertBatchSize) {
				dbAdapter.insertBatch(kvStateId, conf,
						connections.getForIndex(shardIndex),
						insertStatements.getForIndex(shardIndex),
						timestamp, insertPartition);

				insertPartition.clear();
			}
		}

		public void flush(long timestamp) throws IOException {
			// We flush all non-empty partitions
			for (int i = 0; i < inserts.length; i++) {
				List<Tuple2<byte[], byte[]>> insertPartition = inserts[i];
				if (!insertPartition.isEmpty()) {
					dbAdapter.insertBatch(kvStateId, conf, connections.getForIndex(i),
							insertStatements.getForIndex(i), timestamp, insertPartition);
					insertPartition.clear();
				}
			}

		}
	}

	private class Compactor implements Runnable {

		private long upperBound;

		public Compactor(long upperBound) {
			this.upperBound = upperBound;
		}

		@Override
		public void run() {
			// We create new database connections to make sure we don't
			// interfere with the checkpointing (connections are not thread
			// safe)
			try (ShardedConnection sc = conf.createShardedConnection()) {
				for (final Connection c : sc.connections()) {
					SQLRetrier.retry(new Callable<Void>() {
						@Override
						public Void call() throws Exception {
							dbAdapter.compactKvStates(kvStateId, c, lastCompactedTs, upperBound);
							return null;
						}
					}, numSqlRetries, sqlRetrySleep);
				}
				if (LOG.isInfoEnabled()) {
					LOG.info("State succesfully compacted for {} between {} and {}.", kvStateId,
							lastCompactedTs,
							upperBound);
				}
				lastCompactedTs = upperBound;
			} catch (SQLException | IOException e) {
				LOG.warn("State compaction failed due: {}", e);
			}
		}

	}
}
