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

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import org.apache.derby.client.am.SqlException;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.flink.streaming.api.checkpoint.CheckpointNotifier;
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
public class LazyDbKvState<K, V> implements KvState<K, V, DbStateBackend>, CheckpointNotifier {

	private static final Logger LOG = LoggerFactory.getLogger(LazyDbKvState.class);

	// ------------------------------------------------------

	// Unique id for this state (jobID_operatorID_stateName)
	private final String kvStateId;
	private final boolean compact;

	private K currentKey;
	private final V defaultValue;

	private final TypeSerializer<K> keySerializer;
	private final TypeSerializer<V> valueSerializer;

	// ------------------------------------------------------

	// Max number of retries for failed database operations
	private final int numSqlRetries;
	// Sleep time between two retries
	private final int sqlRetrySleep;
	// Max number of key-value pairs inserted in one batch to the database
	private final int maxInsertBatchSize;
	// We will do database compaction every so many checkpoints
	private final int compactEvery;

	// Database properties
	private final DbBackendConfig conf;
	private final Connection con;
	private final MySqlAdapter dbAdapter;
	private final BatchInsert batchInsert;

	// Statements for key-lookups and inserts as prepared by the dbAdapter
	private PreparedStatement selectStatement;
	private PreparedStatement insertStatement;

	// ------------------------------------------------------

	// LRU cache for the key-value states backed by the database
	private final StateCache cache;

	private long nextCheckpointId;

	// ------------------------------------------------------

	/**
	 * Constructor to initialize the {@link LazyDbKvState} the first time the
	 * job starts.
	 */
	public LazyDbKvState(String kvStateId, boolean compact, Connection con, DbBackendConfig conf,
			TypeSerializer<K> keySerializer, TypeSerializer<V> valueSerializer, V defaultValue) throws IOException {
		this(kvStateId, compact, con, conf, keySerializer, valueSerializer, defaultValue, 1);
	}

	/**
	 * Initialize the {@link LazyDbKvState} from a snapshot.
	 */
	public LazyDbKvState(String kvStateId, boolean compact, Connection con, final DbBackendConfig conf,
			TypeSerializer<K> keySerializer, TypeSerializer<V> valueSerializer, V defaultValue, long nextId)
					throws IOException {

		this.kvStateId = kvStateId;
		this.compact = compact;

		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		this.defaultValue = defaultValue;

		this.maxInsertBatchSize = conf.getMaxKvInsertBatchSize();
		this.conf = conf;
		this.con = con;
		this.dbAdapter = conf.getDbAdapter();
		this.compactEvery = conf.getKvStateCompactionFrequency();
		this.numSqlRetries = conf.getMaxNumberOfSqlRetries();
		this.sqlRetrySleep = conf.getSleepBetweenSqlRetries();

		this.nextCheckpointId = nextId;

		this.cache = new StateCache(conf.getKvCacheSize(), conf.getNumElementsToEvict());

		initDB(this.con);

		batchInsert = new BatchInsert();

		if (LOG.isDebugEnabled()) {
			LOG.debug("Lazy database kv-state ({}) successfully initialized", kvStateId);
		}
	}

	@Override
	public void setCurrentKey(K key) {
		this.currentKey = key;
	}

	@Override
	public void update(V value) throws IOException {
		try {
			cache.put(currentKey, Optional.fromNullable(value));
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
			V val = cache.get(currentKey).orNull();
			return val != null ? val : copyDefault();
		} catch (RuntimeException e) {
			// We need to catch the RuntimeExceptions thrown in the StateCache
			// methods here
			throw new IOException(e);
		}
	}

	@Override
	public DbKvStateSnapshot<K, V> shapshot(long checkpointId, long timestamp) throws IOException {

		// We insert the modified elements to the database with the current id
		// then clear the modified states
		for (Entry<K, Optional<V>> state : cache.modified.entrySet()) {
			batchInsert.add(state, checkpointId);
		}
		batchInsert.flush(checkpointId);
		cache.modified.clear();

		// We increase the next checkpoint id
		nextCheckpointId = checkpointId + 1;

		return new DbKvStateSnapshot<K, V>(kvStateId, checkpointId);
	}

	/**
	 * Returns the number of elements currently stored in the task's cache. Note
	 * that the number of elements in the database is not counted here.
	 */
	@Override
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
	private void initDB(final Connection con) throws IOException {

		retry(new Callable<Void>() {
			public Void call() throws Exception {

				dbAdapter.createKVStateTable(kvStateId, con);

				insertStatement = dbAdapter.prepareKVCheckpointInsert(kvStateId, con);
				selectStatement = dbAdapter.prepareKeyLookup(kvStateId, con);

				return null;
			}

		}, numSqlRetries, sqlRetrySleep);
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) {
		// If compaction is turned on we compact on the first subtask
		if (compactEvery > 0 && compact && checkpointId % compactEvery == 0) {
			try {
				dbAdapter.compactKvStates(kvStateId, con, 0, checkpointId);
				if (LOG.isDebugEnabled()) {
					LOG.debug("State succesfully compacted for {}.", kvStateId);
				}
			} catch (SQLException e) {
				LOG.warn("State compaction failed due: {}", e);
			}
		}
	}

	@Override
	public void dispose() {
		// We are only closing the statements here, the connection is borrowed
		// from the state backend and will be closed there.
		try {
			selectStatement.close();
		} catch (SQLException e) {
			// There is not much to do about this
		}
		try {
			insertStatement.close();
		} catch (SQLException e) {
			// There is not much to do about this
		}
	}

	/**
	 * Return the Map of cached states.
	 * 
	 */
	public Map<K, Optional<V>> getStateCache() {
		return cache;
	}

	/**
	 * Return the Map of modified states that hasn't been written to the
	 * database yet.
	 * 
	 */
	public Map<K, Optional<V>> getModified() {
		return cache.modified;
	}

	public boolean isCompacter() {
		return compact;
	}

	/**
	 * Snapshot that stores a specific checkpoint id and state id, and also
	 * rolls back the database to that point upon restore. The rollback is done
	 * by removing all state checkpoints that have ids between the checkpoint
	 * and recovery id.
	 *
	 */
	private static class DbKvStateSnapshot<K, V> implements KvStateSnapshot<K, V, DbStateBackend> {

		private static final long serialVersionUID = 1L;

		private final String kvStateId;
		private final long checkpointId;

		public DbKvStateSnapshot(String kvStateId, long checkpointId) {
			this.checkpointId = checkpointId;
			this.kvStateId = kvStateId;
		}

		@Override
		public LazyDbKvState<K, V> restoreState(final DbStateBackend stateBackend,
				final TypeSerializer<K> keySerializer, final TypeSerializer<V> valueSerializer, final V defaultValue,
				ClassLoader classLoader, final long nextId) throws IOException {

			// First we clean up the states written by partially failed
			// snapshots (we only do it on 1 subtask)
			retry(new Callable<Void>() {
				public Void call() throws Exception {
					stateBackend.getConfiguration().getDbAdapter().cleanupFailedCheckpoints(kvStateId,
							stateBackend.getConnection(), checkpointId, nextId);

					return null;
				}
			}, stateBackend.getConfiguration().getMaxNumberOfSqlRetries(),
					stateBackend.getConfiguration().getSleepBetweenSqlRetries());

			boolean cleanup = stateBackend.getEnvironment().getIndexInSubtaskGroup() == stateBackend.getShardIndex();

			// Restore the KvState
			LazyDbKvState<K, V> restored = new LazyDbKvState<K, V>(kvStateId, cleanup,
					stateBackend.getConnection(), stateBackend.getConfiguration(), keySerializer, valueSerializer,
					defaultValue, nextId);

			if (LOG.isDebugEnabled()) {
				LOG.debug("KV state({},{}) restored.", kvStateId, nextId);
			}

			return restored;
		}

		@Override
		public void discardState() throws Exception {
			// Don't discard, it will be compacted by the LazyDbKvState
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
	private final class StateCache extends LinkedHashMap<K, Optional<V>> {
		private static final long serialVersionUID = 1L;

		private final int cacheSize;
		private final int evictionSize;

		// We keep track the state modified since the last checkpoint
		private final Map<K, Optional<V>> modified = new HashMap<>();

		public StateCache(int cacheSize, int evictionSize) {
			super(cacheSize, 0.75f, true);
			this.cacheSize = cacheSize;
			this.evictionSize = evictionSize;
		}

		@Override
		public Optional<V> put(K key, Optional<V> value) {
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
				value = Optional.fromNullable(getFromDatabaseOrNull((K) key));
				put((K) key, value);
			}
			// We currently mark elements that were retreived also as modified
			// in case the user applies some mutation without update.
			modified.put((K) key, value);
			return value;
		}

		@Override
		protected boolean removeEldestEntry(Entry<K, Optional<V>> eldest) {
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
		private V getFromDatabaseOrNull(final K key) {
			try {
				return retry(new Callable<V>() {
					public V call() throws Exception {
						// We lookup using the adapter and serialize/deserialize
						// with the TypeSerializers
						byte[] serializedVal = dbAdapter.lookupKey(kvStateId, selectStatement,
								InstantiationUtil.serializeToByteArray(keySerializer, key), nextCheckpointId);

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

					Iterator<Entry<K, Optional<V>>> entryIterator = entrySet().iterator();
					while (numEvicted++ < evictionSize && entryIterator.hasNext()) {

						Entry<K, Optional<V>> next = entryIterator.next();

						// We only need to write to the database if modified
						if (modified.remove(next.getKey()) != null) {
							batchInsert.add(next, nextCheckpointId);
						}

						entryIterator.remove();
					}

					batchInsert.flush(nextCheckpointId);

				} catch (IOException e) {
					// We need to re-throw this exception to conform to the map
					// interface, we will catch this when we call the the
					// put/get
					throw new RuntimeException(e);
				}
			}
		}

		@Override
		public void putAll(Map<? extends K, ? extends Optional<V>> m) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void clear() {
			throw new UnsupportedOperationException();
		}
	}

	// Object for handling inserts to the database by batching them together
	private class BatchInsert {
		List<Tuple2<byte[], byte[]>> toInsert = new ArrayList<>();

		public void add(Entry<K, Optional<V>> next, long checkpointId) throws IOException {
			K k = next.getKey();
			V v = next.getValue().orNull();
			toInsert.add(Tuple2.of(InstantiationUtil.serializeToByteArray(keySerializer, k),
					v != null ? InstantiationUtil.serializeToByteArray(valueSerializer, v) : null));
			if (toInsert.size() == maxInsertBatchSize) {
				flush(checkpointId);
			}
		}

		public void flush(long checkpointId) throws IOException {
			if (!toInsert.isEmpty()) {
				dbAdapter.insertBatch(kvStateId, conf, con, insertStatement, checkpointId, toInsert);
				toInsert.clear();
			}
		}
	}
}
