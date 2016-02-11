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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.hash.BloomFilter;

public abstract class OutOfCoreKvState<K, N, V, S extends AbstractStateBackend>
		implements KvState<K, N, ValueState<V>, ValueStateDescriptor<V>, S>, ValueState<V>,
		CheckpointListener {

	private static final Logger LOG = LoggerFactory.getLogger(OutOfCoreKvState.class);

	protected final String stateId;

	protected final KvStateConfig<?> conf;

	protected final TypeSerializer<K> keySerializer;
	protected final TypeSerializer<V> valueSerializer;
	protected final TypeSerializer<N> namespaceSerializer;

	protected final ValueStateDescriptor<V> stateDesc;

	private N currentNamespace;
	protected K currentKey;
	protected final V defaultValue;

	protected final InMemoryStateCache<K, N, V> cache;

	protected long lastCheckpointId;
	protected long lastCheckpointTs;
	protected long currentTs;

	protected final SortedMap<Long, Long> pendingCheckpoints = new TreeMap<>();
	protected final SortedMap<Long, Long> completedCheckpoints = new TreeMap<>();

	protected BloomFilter<byte[]> bloomFilter = null;
	protected final S backend;
	protected final CompactionStrategy compactionStrategy;

	public OutOfCoreKvState(
			S backend,
			String stateId,
			KvStateConfig<?> kvStateConf,
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			ValueStateDescriptor<V> stateDesc,
			long lastCheckpointId,
			long lastCheckpointTs,
			long currentTs) {

		this.stateId = stateId;
		this.conf = kvStateConf;
		this.keySerializer = keySerializer;
		this.namespaceSerializer = namespaceSerializer;
		this.valueSerializer = stateDesc.getSerializer();
		this.defaultValue = stateDesc.getDefaultValue();
		this.stateDesc = stateDesc;

		this.cache = new InMemoryStateCache<>(conf.getKvCacheSize(), conf.getNumElementsToEvict(), this);

		this.lastCheckpointId = lastCheckpointId;
		this.lastCheckpointTs = lastCheckpointTs;
		this.currentTs = currentTs;
		this.backend = backend;
		this.compactionStrategy = conf.getCompactionStrategy();

		// If bloomfilter is enabled we create a new one based on the config
		if (conf.hasBloomFilter()) {
			LOG.debug("Bloomfilter enabled for {}.", stateId);
			bloomFilter = BloomFilter.create(new KeyFunnel(), conf.getBloomFilterExpectedInserts(),
					conf.getBloomFilterFPP());
		}
	}

	/**
	 * Restores the bloomfilter (if necessary) from the checkpoint @throws
	 * Exception @throws
	 */
	public void restoreBloomFilter(StreamStateHandle filterCheckpoint, ClassLoader cl) throws Exception {
		if (filterCheckpoint != null && conf.hasBloomFilter()) {
			try (InputStream in = filterCheckpoint.getState(cl)) {
				this.bloomFilter = BloomFilter.readFrom(in, new KeyFunnel());
			}

			LOG.debug("Restored bloomfilter from checkpoint for {}.", stateId);
		}
	}

	/**
	 * Returns the state cache of this KvState
	 */
	public InMemoryStateCache<K, N, V> getCache() {
		return cache;
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
			// We put directly in the cache. Evictions will write the data to
			// the persistent storage
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
	public KvStateSnapshot<K, N, ValueState<V>, ValueStateDescriptor<V>, S> snapshot(long checkpointId,
			long timestamp)
					throws Exception {

		LOG.debug("Starting snapshot {} for {}.", checkpointId, stateId);

		// Validate timing assumptions
		if (timestamp <= currentTs) {
			throw new RuntimeException("Checkpoint timestamp is smaller than previous ts + 1, "
					+ "this should not happen.");
		}

		preSnapshot(checkpointId, timestamp);

		// If the bloomfilter is enabled we need to checkpoint that
		StreamStateHandle filterCheckpoint = null;

		if (bloomFilter != null) {
			LOG.debug("Checkpointing bloomfilter for {}...", stateId);
			// We use the non-partitioned backend for checkpointing the filter
			CheckpointStateOutputStream cpStream = backend.createCheckpointStateOutputStream(checkpointId, timestamp);
			bloomFilter.writeTo(cpStream);
			filterCheckpoint = cpStream.closeAndGetHandle();
			LOG.debug("Bloomfilter successfully checkpointed for {}.", stateId);
		}

		// Create a snapshot of the kvstate (without the filter)
		LOG.debug("Snapshotting modified states for {}...", stateId);
		KvStateSnapshot<K, N, ValueState<V>, ValueStateDescriptor<V>, S> snapshot = snapshotStates(
				cache.modified.entrySet(), checkpointId, timestamp);

		LOG.debug("Modified states succesfully checkpointed for {}...", stateId);

		// We wrap the state checkpoint together with the filter checkpoint
		KvStateSnapshot<K, N, ValueState<V>, ValueStateDescriptor<V>, S> wrappedSnapshot = new SnapshotWrapper<>(
				snapshot, filterCheckpoint,
				completedCheckpoints);

		// Clear the modified values
		cache.modified.clear();

		postSnapshot(checkpointId, timestamp);

		if (compactionStrategy.enabled) {
			pendingCheckpoints.put(checkpointId, lastCheckpointTs + 1);
		}

		lastCheckpointTs = timestamp;
		currentTs = timestamp + 1;
		lastCheckpointId = checkpointId;

		LOG.debug("Completed snapshot {} for {}.", checkpointId, stateId);

		return wrappedSnapshot;
	}

	private static class SnapshotWrapper<K, N, V, S extends AbstractStateBackend>
			implements KvStateSnapshot<K, N, ValueState<V>, ValueStateDescriptor<V>, S> {
		private static final long serialVersionUID = 1L;
		private final KvStateSnapshot<K, N, ValueState<V>, ValueStateDescriptor<V>, S> wrapped;
		private final StreamStateHandle filterCheckpoint;
		private final Map<Long, Long> completedCheckpoints;

		public SnapshotWrapper(KvStateSnapshot<K, N, ValueState<V>, ValueStateDescriptor<V>, S> wrapped,
				StreamStateHandle filterCheckpoint,
				Map<Long, Long> completedCheckpoints) {
			this.wrapped = wrapped;
			this.filterCheckpoint = filterCheckpoint;
			this.completedCheckpoints = completedCheckpoints;
		}

		@Override
		public KvState<K, N, ValueState<V>, ValueStateDescriptor<V>, S> restoreState(S stateBackend,
				TypeSerializer<K> keySerializer, ClassLoader classLoader, long recoveryTimestamp) throws Exception {
			OutOfCoreKvState<K, N, V, S> restored = (OutOfCoreKvState<K, N, V, S>) wrapped.restoreState(stateBackend,
					keySerializer, classLoader, recoveryTimestamp);
			restored.completedCheckpoints.putAll(completedCheckpoints);
			if (filterCheckpoint != null) {
				restored.restoreBloomFilter(filterCheckpoint, classLoader);
			}
			return restored;
		}

		@Override
		public void discardState() throws Exception {
			wrapped.discardState();
			if (filterCheckpoint != null) {
				filterCheckpoint.discardState();
			}
		}

		@Override
		public long getStateSize() throws Exception {
			return wrapped.getStateSize() + (filterCheckpoint != null ? filterCheckpoint.getStateSize() : 0);
		}

	}

	/**
	 * Snapshot (save) the states that were modified since the last checkpoint
	 * to the out-of-core storage layer. (For instance write to database or
	 * disk).
	 * 
	 * Returns a {@link KvStateSnapshot} for the current id and timestamp. It is
	 * not assumed that a checkpoint will always successfully complete (it might
	 * fail at other tasks as well). Therefore the snapshot should contain
	 * enough information so that it can clean up the partially failed records
	 * to maintain the exactly-once semantics.
	 * <p>
	 * For instance if the snapshot is taken based on the timestamp, we can use
	 * the checkpoint timestamp and recovery timestamp to delete records between
	 * those two.
	 * 
	 * @param modifiedKVs
	 *            Collection of Key-Optional<State> entries to be checkpointed.
	 * @param checkpointId
	 *            The current checkpoint id. This is not assumed to be always
	 *            increasing.
	 * @param timestamp
	 *            The current checkpoint timestamp. This is assumed to be
	 *            increasing.
	 * @return The current {@link KvStateSnapshot}
	 * 
	 */
	public abstract KvStateSnapshot<K, N, ValueState<V>, ValueStateDescriptor<V>, S> snapshotStates(
			Collection<Entry<Tuple2<K, N>, Optional<V>>> modifiedKVs,
			long checkpointId, long timestamp) throws IOException;

	/**
	 * Save the given collection of state entries to the out-of-core storage so
	 * that it can retrieved later. This method is called when the state cache
	 * is full and wants to evict elements.
	 * <p>
	 * Records written by this method will not be part of the previous snapshot
	 * but should be part of the next one.
	 * 
	 * @param KVsToEvict
	 *            Collection of Key-Optional<State> entries to be evicted.
	 * @param lastCheckpointId
	 *            The checkpoint id of the last checkpoint.
	 * @param lastCheckpointTs
	 *            The timestamp of the last checkpoint.
	 * @param currentTs
	 *            Current timestamp (greater or equal to the last checkpoint
	 *            timestamp)
	 * @throws IOException
	 */
	public abstract void evictModified(Collection<Entry<Tuple2<K, N>, Optional<V>>> KVsToEvict, long lastCheckpointId,
			long lastCheckpointTs, long currentTs) throws IOException;

	/**
	 * Lookup latest entry for a specific key from the out-of-core storage.
	 * 
	 * @param key
	 *            Key to lookup.
	 * @param serializedKey
	 *            Serialized key to lookup.
	 * @return Returns {@link Optional#of(..)} if exists or
	 *         {@link Optional#absent()} if missing.
	 */
	public abstract Optional<V> lookupLatest(Tuple2<K, N> key, byte[] serializedKey) throws Exception;

	/**
	 * Return a copy the default value or null if the default was null.
	 * 
	 */
	private V copyDefault() {
		return defaultValue != null ? valueSerializer.copy(defaultValue) : null;
	}

	/**
	 * Pre-snapshot hook
	 */
	public void preSnapshot(long checkpointId, long timestamp) throws Exception {

	}

	/**
	 * Post-snapshot hook
	 */
	public void postSnapshot(long checkpointId, long timestamp) throws Exception {

	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws IOException {
		if (compactionStrategy.enabled) {
			if (pendingCheckpoints.containsKey(checkpointId)) {
				completedCheckpoints.put(checkpointId, pendingCheckpoints.remove(checkpointId));

				if (completedCheckpoints.size() == compactionStrategy.frequency) {
					if (compactionStrategy.compactFromBeginning) {
						compact(0, completedCheckpoints.get(completedCheckpoints.lastKey()));
					} else {
						compact(completedCheckpoints.get(completedCheckpoints.firstKey()),
								completedCheckpoints.get(completedCheckpoints.lastKey()));
					}
					completedCheckpoints.clear();
				}

			}
		}
	}

	/**
	 * Compact state changes between (inclusive) two timestamps
	 * 
	 * @param from
	 *            Lower bound timestamp
	 * @param to
	 *            Upper bound timestamp
	 */
	public abstract void compact(long from, long to) throws IOException;

	/**
	 * LRU cache implementation for storing the key-value states. When the cache
	 * is full elements are not evicted one by one but are evicted in a batch
	 * defined in the {@link KvStateConfig}.
	 * <p>
	 * Keys not found in the cached will be retrieved from the underlying
	 * out-of-core storage
	 */
	public static final class InMemoryStateCache<K, N, V> extends LinkedHashMap<Tuple2<K, N>, Optional<V>> {
		private static final long serialVersionUID = 1L;

		private final OutOfCoreKvState<K, N, V, ?> kvState;

		private final int cacheSize;
		private final int evictionSize;

		// We keep track the state modified since the last checkpoint
		protected final Map<Tuple2<K, N>, Optional<V>> modified = new HashMap<>();

		public InMemoryStateCache(int cacheSize, int evictionSize, OutOfCoreKvState<K, N, V, ?> kvState) {
			super(cacheSize, 0.75f, true);

			this.cacheSize = cacheSize;
			this.evictionSize = evictionSize;

			this.kvState = kvState;
		}

		@Override
		public Optional<V> put(Tuple2<K, N> key, Optional<V> value) {
			// Put kv pair in the cache and evict elements if the cache is full
			Optional<V> old = super.put(key, value);
			modified.put(key, value);
			try {
				if (kvState.bloomFilter != null) {
					kvState.bloomFilter.put(kvState.serializeKeyAndNamespace(key));
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			evictIfFull();
			return old;
		}

		@SuppressWarnings("unchecked")
		@Override
		public Optional<V> get(Object key) {
			// First we check whether the value is cached
			Optional<V> value = super.get(key);

			// If value is not cached we first chek the bloomfilter than we try
			// to retrieve from the external store
			if (value == null) {
				byte[] serializedKey = null;
				try {
					serializedKey = kvState.serializeKeyAndNamespace((Tuple2<K, N>) key);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}

				if (kvState.bloomFilter == null || kvState.bloomFilter.mightContain(serializedKey)) {
					try {
						value = kvState.lookupLatest((Tuple2<K, N>) key, serializedKey);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
					super.put((Tuple2<K, N>) key, value);
					evictIfFull();
				} else {
					value = Optional.absent();
				}
			}

			return value;
		}

		public Map<Tuple2<K, N>, Optional<V>> getModified() {
			return modified;
		}

		@Override
		protected boolean removeEldestEntry(Entry<Tuple2<K, N>, Optional<V>> eldest) {
			// We need to remove elements manually if the cache becomes full, so
			// we always return false here.
			return false;
		}

		private void evictIfFull() {
			if (size() > cacheSize) {

				LOG.debug("State cache full for {}. Evicting up to {} modified values...", kvState.stateId,
						evictionSize);

				int numEvicted = 0;
				Iterator<Entry<Tuple2<K, N>, Optional<V>>> entryIterator = entrySet().iterator();
				List<Entry<Tuple2<K, N>, Optional<V>>> toEvict = new ArrayList<>();

				while (numEvicted++ < evictionSize && entryIterator.hasNext()) {

					Entry<Tuple2<K, N>, Optional<V>> next = entryIterator.next();

					// We only need to write to the database if modified
					if (modified.remove(next.getKey()) != null) {
						toEvict.add(next);
					}

					entryIterator.remove();
				}

				try {
					if (!toEvict.isEmpty()) {
						kvState.evictModified(toEvict, kvState.lastCheckpointId, kvState.lastCheckpointTs,
								kvState.currentTs);
						kvState.currentTs++;
					}
				} catch (IOException e) {
					throw new RuntimeException(e);
				}

				LOG.debug("Successfully evicted {} modified values for {}.", numEvicted, kvState.stateId);
			}
		}

		@Override
		public void putAll(Map<? extends Tuple2<K, N>, ? extends Optional<V>> m) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void clear() {
			super.clear();
			modified.clear();
		}

		@Override
		public String toString() {
			return "Cache: " + super.toString() + "\nModified: " + modified;
		}
	}

	public byte[] serializeKeyAndNamespace(Tuple2<K, N> keyAndNamespace) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(baos);
		keySerializer.serialize(keyAndNamespace.f0, out);
		namespaceSerializer.serialize(keyAndNamespace.f1, out);
		out.close();
		return baos.toByteArray();
	}

	@Override
	public void dispose() {

	}

	@Override
	public void clear() {
		cache.put(Tuple2.of(currentKey, currentNamespace), Optional.<V> fromNullable(null));
	}

}
