/*
 * Copyright 2018 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;


/**
 * Auxiliary class for scanning RocksDB with given prefix.
 */
public abstract class RocksDBPrefixIterator<K, V> implements Iterator<Map.Entry<K, V>> {

	static final int CACHE_SIZE_BASE = 1;
	static final int CACHE_SIZE_LIMIT = 128;

	/** A map entry in RocksDBMapState. */
	class RocksDBPrefixEntry<K, V> implements Map.Entry<K, V> {

		/** Deserializer of raw bytes to key type. */
		private final Function<byte[], K> keyDeserializer;

		/** Deserializer of raw bytes to value type. */
		private final Function<byte[], V> valueDeserializer;

		/** Serializer of value to raw bytes. */
		private final Function<V, byte[]> valueSerializer;

		/** The raw bytes of the key stored in RocksDB. Each user key is stored in RocksDB
		 * with the format #KeyGroup#Key#Namespace#UserKey. */
		private final byte[] rawKeyBytes;

		/** The raw bytes of the value stored in RocksDB. */
		private byte[] rawValueBytes;

		/** True if the entry has been deleted. */
		private boolean deleted;

		/** Deserialized key (cached). */
		@Nullable
		K key;

		/** Deserialized value (cached). */
		@Nullable
		V value;

		RocksDBPrefixEntry(
						final byte[] rawKeyBytes, final byte[] rawValueBytes,
						final Function<byte[], K> keyDeserializer,
						final Function<byte[], V> valueDeserializer,
						final Function<V, byte[]> valueSerializer) {

			this.keyDeserializer = keyDeserializer;
			this.valueDeserializer = valueDeserializer;
			this.valueSerializer = valueSerializer;
			this.rawKeyBytes = rawKeyBytes;
			this.rawValueBytes = rawValueBytes;
			this.deleted = false;
		}

		public void remove() {
			deleted = true;
			rawValueBytes = null;

			try {
				state.backend.db.delete(state.columnFamily, state.writeOptions, rawKeyBytes);
			} catch (RocksDBException e) {
				throw new RuntimeException("Error while removing data from RocksDB.", e);
			}
		}

		@Override
		public K getKey() {
			if (key == null) {
				key = keyDeserializer.apply(rawKeyBytes);
			}
			return key;
		}

		@Override
		public V getValue() {
			if (deleted) {
				return null;
			} else {
				if (value == null) {
					value = valueDeserializer.apply(rawValueBytes);
				}
				return value;
			}
		}

		@Override
		public V setValue(V value) {
			if (deleted) {
				throw new IllegalStateException("The value has already been deleted.");
			}

			V oldValue = getValue();
			try {
				rawValueBytes = valueSerializer.apply(value);
				state.backend.db.put(
								state.columnFamily, state.writeOptions, rawKeyBytes, rawValueBytes);
			} catch (RocksDBException e) {
				throw new RuntimeException("Error while putting data into RocksDB.", e);
			}

			return oldValue;
		}

		@Override
		public String toString() {
			return "RocksDBMapEntry("
							+ "key=" + Arrays.toString(rawKeyBytes)
							+ ", value=" + Arrays.toString(rawValueBytes)
							+ ")";
		}

	}

	private final AbstractRocksDBState state;

	/**
	 * The prefix bytes of the key being accessed. All entries under the same key
	 * has the same prefix, hence we can stop the iterating once coming across an
	 * entry with a different prefix.
	 */
	private final byte[] keyPrefixBytes;

	/**
	 * True if all entries have been accessed or the iterator has come across an
	 * entry with a different prefix.
	 */
	private boolean expired = false;

	/** A in-memory cache for the entries in the rocksdb. */
	private final ArrayList<RocksDBPrefixEntry<K, V>> cacheEntries = new ArrayList<>();
	private int cacheIndex = 0;

	RocksDBPrefixIterator(
					final AbstractRocksDBState state,
					final byte[] keyPrefixBytes) {

		this.state = state;
		this.keyPrefixBytes = keyPrefixBytes;
	}

	@Override
	public boolean hasNext() {
		loadCache();

		return (cacheIndex < cacheEntries.size());
	}

	@Override
	public void remove() {
		if (cacheIndex == 0 || cacheIndex > cacheEntries.size()) {
			throw new IllegalStateException(
							"The remove operation must be called after an valid next operation.");
		}

		RocksDBPrefixEntry<K, V> lastEntry = cacheEntries.get(cacheIndex - 1);
		lastEntry.remove();
	}

	@Override
	public Map.Entry<K, V> next() {
		loadCache();

		if (cacheIndex == cacheEntries.size()) {
			if (!expired) {
				throw new IllegalStateException();
			}

			return null;
		}

		RocksDBPrefixEntry entry = cacheEntries.get(cacheIndex);
		cacheIndex++;

		return entry;
	}

	private void loadCache() {
		if (cacheIndex > cacheEntries.size()) {
			throw new IllegalStateException();
		}

		// Load cache entries only when the cache is empty and there still exist unread entries
		if (cacheIndex < cacheEntries.size() || expired) {
			return;
		}

		try (RocksIterator iterator = state.backend.db.newIterator(state.columnFamily)) {

			/*
			* The iteration starts from the prefix bytes at the first loading. The cache then is
			* reloaded when the next entry to return is the last one in the cache. At that time,
			* we will start the iterating from the last returned entry.
			*/
			RocksDBPrefixEntry lastEntry = cacheEntries.isEmpty() ? null : cacheEntries.get(cacheEntries.size() - 1);
			byte[] startBytes = (lastEntry == null ? keyPrefixBytes : lastEntry.rawKeyBytes);
			int numEntries = (lastEntry == null ? CACHE_SIZE_BASE : Math.min(cacheEntries.size() * 2, CACHE_SIZE_LIMIT));

			cacheEntries.clear();
			cacheIndex = 0;

			iterator.seek(startBytes);

			/*
			* If the last returned entry is not deleted, it will be the first entry in the
			* iterating. Skip it to avoid redundant access in such cases.
			*/
			if (lastEntry != null && !lastEntry.deleted) {
				iterator.next();
			}

			while (true) {
				if (!iterator.isValid() || !underSameKey(iterator.key())) {
					expired = true;
					break;
				}

				if (cacheEntries.size() >= numEntries) {
					break;
				}

				RocksDBPrefixEntry<K, V> entry = new RocksDBPrefixEntry<>(
								iterator.key(), iterator.value(), this::deserializeKey,
								this::deserializeValue, this::serializeValue);
				// skip any entry that doesn't have suffix
				if (entry.rawKeyBytes.length > this.keyPrefixBytes.length) {
					cacheEntries.add(entry);
				}

				iterator.next();
			}
		}
	}

	private boolean underSameKey(byte[] rawKeyBytes) {
		if (rawKeyBytes.length < keyPrefixBytes.length) {
			return false;
		}

		for (int i = 0; i < keyPrefixBytes.length; ++i) {
			if (rawKeyBytes[i] != keyPrefixBytes[i]) {
				return false;
			}
		}

		return true;
	}

	/** Deserialize key from raw bytes. */
	abstract K deserializeKey(byte[] bytes);

	/** Deserialize value from raw bytes. */
	abstract V deserializeValue(byte[] bytes);

	/** Serialize value to bytes. */
	abstract byte[] serializeValue(V value);

}
