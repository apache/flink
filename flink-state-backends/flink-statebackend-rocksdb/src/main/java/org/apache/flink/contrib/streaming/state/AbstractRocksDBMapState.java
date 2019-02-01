/*
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

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Base class for {@link State} implementations that store state in a RocksDB database.
 *
 * <p>{@link RocksDBStateBackend} must ensure that we set the
 * {@link org.rocksdb.StringAppendOperator} on the column family that we use for our state since
 * we use the {@code merge()} call.
 *
 * @param <K>  The type of the key.
 * @param <N>  The type of the namespace.
 * @param <UK> The type of the keys in the map state.
 * @param <UV> The type of the user values.
 * @param <SV> The type of the user values in the map state value part.
 */
abstract class AbstractRocksDBMapState<K, N, UK, UV, SV>
	extends AbstractRocksDBState<K, N, Map<UK, SV>> {

	private static final Logger LOG = LoggerFactory.getLogger(AbstractRocksDBMapState.class);

	/**
	 * Creates a new {@code AbstractRocksDBMapState}.
	 *
	 * @param columnFamily        The RocksDB column family that this state is associated to.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param saveSerializer      The serializer for the state.
	 * @param defaultValue        The default value for the state.
	 * @param backend             The backend for which this state is bind to.
	 */
	protected AbstractRocksDBMapState(
		ColumnFamilyHandle columnFamily,
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<Map<UK, SV>> saveSerializer,
		Map<UK, SV> defaultValue,
		RocksDBKeyedStateBackend<K> backend) {
		super(columnFamily, namespaceSerializer, saveSerializer, defaultValue, backend);
	}

	@Override
	public TypeSerializer<K> getKeySerializer() {
		return backend.getKeySerializer();
	}

	@Override
	public TypeSerializer<N> getNamespaceSerializer() {
		return namespaceSerializer;
	}

	@Override
	public TypeSerializer<Map<UK, SV>> getValueSerializer() {
		return valueSerializer;
	}

	@Override
	public void clear() {
		try {
			try (RocksIteratorWrapper iterator = RocksDBKeyedStateBackend.getRocksIterator(backend.db, columnFamily);
				WriteBatch writeBatch = new WriteBatch(128)) {

				final byte[] keyPrefixBytes = serializeCurrentKeyWithGroupAndNamespace();
				iterator.seek(keyPrefixBytes);

				while (iterator.isValid()) {
					byte[] keyBytes = iterator.key();
					if (startWithKeyPrefix(keyPrefixBytes, keyBytes)) {
						writeBatch.remove(columnFamily, keyBytes);
					} else {
						break;
					}
					iterator.next();
				}

				backend.db.write(writeOptions, writeBatch);
			}
		} catch (Exception e) {
			LOG.warn("Error while cleaning the state.", e);
		}
	}

	private boolean startWithKeyPrefix(byte[] keyPrefixBytes, byte[] rawKeyBytes) {
		if (rawKeyBytes.length < keyPrefixBytes.length) {
			return false;
		}

		for (int i = keyPrefixBytes.length; --i >= backend.getKeyGroupPrefixBytes(); ) {
			if (rawKeyBytes[i] != keyPrefixBytes[i]) {
				return false;
			}
		}

		return true;
	}

	// ------------------------------------------------------------------------
	//  Internal Classes
	// ------------------------------------------------------------------------

	/**
	 * A map entry in AbstractRocksDBMapState.
	 */
	public class RocksDBBaseMapEntry implements Map.Entry<UK, UV> {

		protected final RocksDB db;

		/**
		 * The raw bytes of the key stored in RocksDB. Each user key is stored in RocksDB
		 * with the format #KeyGroup#Key#Namespace#UserKey.
		 */
		private final byte[] rawKeyBytes;

		/**
		 * The raw bytes of the value stored in RocksDB.
		 */
		private byte[] rawValueBytes;

		/**
		 * True if the entry has been deleted.
		 */
		protected boolean deleted;

		/**
		 * The user key and value. The deserialization is performed lazily, i.e. the key
		 * and the value is deserialized only when they are accessed.
		 */
		protected UK userKey;

		protected UV userValue;

		/**
		 * The offset of User Key offset in raw key bytes.
		 */
		private final int userKeyOffset;

		RocksDBBaseMapEntry(
			@Nonnull final RocksDB db,
			@Nonnegative final int userKeyOffset,
			@Nonnull final byte[] rawKeyBytes,
			@Nonnull final byte[] rawValueBytes) {
			this.db = db;

			this.userKeyOffset = userKeyOffset;

			this.rawKeyBytes = rawKeyBytes;
			this.rawValueBytes = rawValueBytes;
			this.deleted = false;
		}

		public byte[] getRawKeyBytes() {
			return rawKeyBytes;
		}

		public byte[] getRawValueBytes() {
			return rawValueBytes;
		}

		public int getUserKeyOffset() {
			return userKeyOffset;
		}

		public boolean isDeleted() {
			return deleted;
		}

		public void remove() {
			throw new UnsupportedOperationException("remove");
		}

		@Override
		public UK getKey() {
			throw new UnsupportedOperationException("getKey");
		}

		@Override
		public UV setValue(UV value) {
			throw new UnsupportedOperationException("setValue");
		}

		public UV getValue() {
			if (deleted) {
				return null;
			} else {
				return userValue;
			}
		}
	}

	/**
	 * An auxiliary utility to scan all entries under the given key.
	 */
	protected abstract class RocksDBKVIterator<T> implements Iterator<T> {
		private static final int CACHE_SIZE_LIMIT = 128;

		/**
		 * The db where data resides.
		 */
		private final RocksDB db;

		/**
		 * The prefix bytes of the key being accessed. All entries under the same key
		 * have the same prefix, hence we can stop iterating once coming across an
		 * entry with a different prefix.
		 */
		@Nonnull
		protected final byte[] metaKeyBytes;

		/**
		 * True if all entries have been accessed or the iterator has come across an
		 * entry with a different prefix.
		 */
		private boolean expired = false;

		/**
		 * A in-memory cache for the entries in the rocksdb.
		 */
		protected ArrayList<RocksDBBaseMapEntry> cacheEntries = new ArrayList<>();

		/**
		 * The entry pointing to the current position which is last returned by calling {@link #nextEntry()}.
		 */
		protected RocksDBBaseMapEntry currentEntry;
		protected int cacheIndex = 0;

		RocksDBKVIterator(
			final RocksDB db,
			final byte[] keyPrefixBytes) {

			this.db = db;
			this.metaKeyBytes = keyPrefixBytes;
		}

		@Override
		public boolean hasNext() {
			loadCache();

			return (cacheIndex < cacheEntries.size());
		}

		@Override
		public void remove() {
			if (currentEntry == null || currentEntry.isDeleted()) {
				throw new IllegalStateException("The remove operation must be called after a valid next operation.");
			}

			currentEntry.remove();
		}

		final RocksDBBaseMapEntry nextEntry() {
			loadCache();

			if (cacheIndex == cacheEntries.size()) {
				if (!expired) {
					throw new IllegalStateException();
				}

				return null;
			}

			this.currentEntry = cacheEntries.get(cacheIndex);
			cacheIndex++;

			return currentEntry;
		}

		protected abstract List<RocksDBBaseMapEntry> deserializeKV(
			@Nonnull final RocksDB db,
			@Nonnegative final int userKeyOffset,
			@Nonnull final byte[] rawKeyBytes,
			@Nonnull final byte[] rawValueBytes);

		protected boolean isEndOfIterator(RocksIteratorWrapper iterator) {
			return !iterator.isValid() || !startWithKeyPrefix(metaKeyBytes, iterator.key());
		}

		protected void cacheSeek(RocksIteratorWrapper iterator) {
			/*
			 * The iteration starts from the prefix bytes at the first loading. After #nextEntry() is called,
			 * the currentEntry points to the last returned entry, and at that time, we will start
			 * the iterating from currentEntry if reloading cache is needed.
			 */
			byte[] startBytes = (currentEntry == null ? metaKeyBytes : currentEntry.rawKeyBytes);

			cacheEntries.clear();
			cacheIndex = 0;

			iterator.seek(startBytes);
		}

		protected void cacheNext(RocksIteratorWrapper iterator) {
			iterator.next();
		}

		private void loadCache() {
			if (cacheIndex > cacheEntries.size()) {
				throw new IllegalStateException();
			}

			// Load cache entries only when the cache is empty and there still exist unread entries
			if (cacheIndex < cacheEntries.size() || expired) {
				return;
			}

			// use try-with-resources to ensure RocksIterator can be release even some runtime exception
			// occurred in the below code block.
			try (RocksIteratorWrapper iterator = RocksDBKeyedStateBackend.getRocksIterator(db, columnFamily)) {

				cacheSeek(iterator);

				/*
				 * If the entry pointing to the current position is not removed, it will be the first entry in the
				 * new iterating. Skip it to avoid redundant access in such cases.
				 */
				if (currentEntry != null && !currentEntry.deleted) {
					cacheNext(iterator);
				}

				while (true) {
					if (isEndOfIterator(iterator)) {
						expired = true;
						break;
					}

					if (cacheEntries.size() >= CACHE_SIZE_LIMIT) {
						break;
					}

					cacheEntries.addAll(deserializeKV(db,
						metaKeyBytes.length,
						iterator.key(),
						iterator.value()));

					cacheNext(iterator);
				}
			}
		}
	}
}
