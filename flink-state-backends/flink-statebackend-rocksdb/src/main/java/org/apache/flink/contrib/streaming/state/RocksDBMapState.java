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

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.util.Preconditions;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

/**
 * {@link MapState} implementation that stores state in RocksDB.
 *
 * <p>{@link RocksDBStateBackend} must ensure that we set the
 * {@link org.rocksdb.StringAppendOperator} on the column family that we use for our state since
 * we use the {@code merge()} call.
 *
 * @param <K>  The type of the key.
 * @param <N>  The type of the namespace.
 * @param <UK> The type of the keys in the map state.
 * @param <UV> The type of the values in the map state.
 */
public class RocksDBMapState<K, N, UK, UV>
	extends AbstractRocksDBState<K, N, MapState<UK, UV>, MapStateDescriptor<UK, UV>, Map<UK, UV>>
	implements InternalMapState<N, UK, UV> {

	private static final Logger LOG = LoggerFactory.getLogger(RocksDBMapState.class);

	/** Serializer for the keys and values. */
	private final TypeSerializer<UK> userKeySerializer;
	private final TypeSerializer<UV> userValueSerializer;

	/** The offset of User Key offset in raw key bytes. */
	private int userKeyOffset;

	/**
	 * Creates a new {@code RocksDBMapState}.
	 *
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param stateDesc The state identifier for the state.
	 */
	public RocksDBMapState(ColumnFamilyHandle columnFamily,
			TypeSerializer<N> namespaceSerializer,
			MapStateDescriptor<UK, UV> stateDesc,
			RocksDBKeyedStateBackend<K> backend) {

		super(columnFamily, namespaceSerializer, stateDesc, backend);

		this.userKeySerializer = stateDesc.getKeySerializer();
		this.userValueSerializer = stateDesc.getValueSerializer();
	}

	// ------------------------------------------------------------------------
	//  MapState Implementation
	// ------------------------------------------------------------------------

	@Override
	public UV get(UK userKey) throws IOException, RocksDBException {
		byte[] rawKeyBytes = serializeUserKeyWithCurrentKeyAndNamespace(userKey);
		byte[] rawValueBytes = backend.db.get(columnFamily, rawKeyBytes);

		return (rawValueBytes == null ? null : deserializeUserValue(rawValueBytes));
	}

	@Override
	public void put(UK userKey, UV userValue) throws IOException, RocksDBException {

		byte[] rawKeyBytes = serializeUserKeyWithCurrentKeyAndNamespace(userKey);
		byte[] rawValueBytes = serializeUserValue(userValue);

		backend.db.put(columnFamily, writeOptions, rawKeyBytes, rawValueBytes);
	}

	@Override
	public void putAll(Map<UK, UV> map) throws IOException, RocksDBException {
		if (map == null) {
			return;
		}

		for (Map.Entry<UK, UV> entry : map.entrySet()) {
			put(entry.getKey(), entry.getValue());
		}
	}

	@Override
	public void remove(UK userKey) throws IOException, RocksDBException {
		byte[] rawKeyBytes = serializeUserKeyWithCurrentKeyAndNamespace(userKey);

		backend.db.delete(columnFamily, writeOptions, rawKeyBytes);
	}

	@Override
	public boolean contains(UK userKey) throws IOException, RocksDBException {
		byte[] rawKeyBytes = serializeUserKeyWithCurrentKeyAndNamespace(userKey);
		byte[] rawValueBytes = backend.db.get(columnFamily, rawKeyBytes);

		return (rawValueBytes != null);
	}

	@Override
	public Iterable<Map.Entry<UK, UV>> entries() throws IOException, RocksDBException {
		final Iterator<Map.Entry<UK, UV>> iterator = iterator();

		// Return null to make the behavior consistent with other states.
		if (!iterator.hasNext()) {
			return null;
		} else {
			return new Iterable<Map.Entry<UK, UV>>() {
				@Override
				public Iterator<Map.Entry<UK, UV>> iterator() {
					return iterator;
				}
			};
		}
	}

	@Override
	public Iterable<UK> keys() throws IOException, RocksDBException {
		final byte[] prefixBytes = serializeCurrentKeyAndNamespace();

		return new Iterable<UK>() {
			@Override
			public Iterator<UK> iterator() {
				return new RocksDBMapIterator<UK>(backend.db, prefixBytes) {
					@Override
					public UK next() {
						RocksDBMapEntry entry = nextEntry();
						return (entry == null ? null : entry.getKey());
					}
				};
			}
		};
	}

	@Override
	public Iterable<UV> values() throws IOException, RocksDBException {
		final byte[] prefixBytes = serializeCurrentKeyAndNamespace();

		return new Iterable<UV>() {
			@Override
			public Iterator<UV> iterator() {
				return new RocksDBMapIterator<UV>(backend.db, prefixBytes) {
					@Override
					public UV next() {
						RocksDBMapEntry entry = nextEntry();
						return (entry == null ? null : entry.getValue());
					}
				};
			}
		};
	}

	@Override
	public Iterator<Map.Entry<UK, UV>> iterator() throws IOException, RocksDBException {
		final byte[] prefixBytes = serializeCurrentKeyAndNamespace();

		return new RocksDBMapIterator<Map.Entry<UK, UV>>(backend.db, prefixBytes) {
			@Override
			public Map.Entry<UK, UV> next() {
				return nextEntry();
			}
		};
	}

	@Override
	public void clear() {
		try {
			Iterator<Map.Entry<UK, UV>> iterator = iterator();

			while (iterator.hasNext()) {
				iterator.next();
				iterator.remove();
			}
		} catch (Exception e) {
			LOG.warn("Error while cleaning the state.", e);
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public byte[] getSerializedValue(byte[] serializedKeyAndNamespace) throws Exception {
		Preconditions.checkNotNull(serializedKeyAndNamespace, "Serialized key and namespace");

		//TODO make KvStateSerializer key-group aware to save this round trip and key-group computation
		Tuple2<K, N> des = KvStateSerializer.deserializeKeyAndNamespace(
				serializedKeyAndNamespace,
				backend.getKeySerializer(),
				namespaceSerializer);

		int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(des.f0, backend.getNumberOfKeyGroups());

		ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos(128);
		DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);
		writeKeyWithGroupAndNamespace(keyGroup, des.f0, des.f1, outputStream, outputView);
		final byte[] keyPrefixBytes = outputStream.toByteArray();

		final Iterator<Map.Entry<UK, UV>> iterator = new RocksDBMapIterator<Map.Entry<UK, UV>>(backend.db, keyPrefixBytes) {
			@Override
			public Map.Entry<UK, UV> next() {
				return nextEntry();
			}
		};

		// Return null to make the behavior consistent with other backends
		if (!iterator.hasNext()) {
			return null;
		}

		return KvStateSerializer.serializeMap(new Iterable<Map.Entry<UK, UV>>() {
			@Override
			public Iterator<Map.Entry<UK, UV>> iterator() {
				return iterator;
			}
		}, userKeySerializer, userValueSerializer);
	}

	// ------------------------------------------------------------------------
	//  Serialization Methods
	// ------------------------------------------------------------------------

	private byte[] serializeCurrentKeyAndNamespace() throws IOException {
		writeCurrentKeyWithGroupAndNamespace();
		userKeyOffset = keySerializationStream.getPosition();

		return keySerializationStream.toByteArray();
	}

	private byte[] serializeUserKeyWithCurrentKeyAndNamespace(UK userKey) throws IOException {
		serializeCurrentKeyAndNamespace();
		userKeySerializer.serialize(userKey, keySerializationDataOutputView);

		return keySerializationStream.toByteArray();
	}

	private byte[] serializeUserValue(UV userValue) throws IOException {
		keySerializationStream.reset();

		if (userValue == null) {
			keySerializationDataOutputView.writeBoolean(true);
		} else {
			keySerializationDataOutputView.writeBoolean(false);
			userValueSerializer.serialize(userValue, keySerializationDataOutputView);
		}

		return keySerializationStream.toByteArray();
	}

	private UK deserializeUserKey(byte[] rawKeyBytes) throws IOException {
		ByteArrayInputStreamWithPos bais = new ByteArrayInputStreamWithPos(rawKeyBytes);
		DataInputViewStreamWrapper in = new DataInputViewStreamWrapper(bais);

		in.skipBytes(userKeyOffset);

		return userKeySerializer.deserialize(in);
	}

	private UV deserializeUserValue(byte[] rawValueBytes) throws IOException {
		ByteArrayInputStreamWithPos bais = new ByteArrayInputStreamWithPos(rawValueBytes);
		DataInputViewStreamWrapper in = new DataInputViewStreamWrapper(bais);

		boolean isNull = in.readBoolean();

		return isNull ? null : userValueSerializer.deserialize(in);
	}

	// ------------------------------------------------------------------------
	//  Internal Classes
	// ------------------------------------------------------------------------

	/** A map entry in RocksDBMapState. */
	private class RocksDBMapEntry implements Map.Entry<UK, UV> {
		private final RocksDB db;

		/** The raw bytes of the key stored in RocksDB. Each user key is stored in RocksDB
		 * with the format #KeyGroup#Key#Namespace#UserKey. */
		private final byte[] rawKeyBytes;

		/** The raw bytes of the value stored in RocksDB. */
		private byte[] rawValueBytes;

		/** True if the entry has been deleted. */
		private boolean deleted;

		/** The user key and value. The deserialization is performed lazily, i.e. the key
		 * and the value is deserialized only when they are accessed. */
		private UK userKey = null;
		private UV userValue = null;

		RocksDBMapEntry(
			@Nonnull final RocksDB db,
			@Nonnull final byte[] rawKeyBytes,
			@Nonnull final byte[] rawValueBytes) {
			this.db = db;

			this.rawKeyBytes = rawKeyBytes;
			this.rawValueBytes = rawValueBytes;
			this.deleted = false;
		}

		public void remove() {
			deleted = true;
			rawValueBytes = null;

			try {
				db.delete(columnFamily, writeOptions, rawKeyBytes);
			} catch (RocksDBException e) {
				throw new RuntimeException("Error while removing data from RocksDB.", e);
			}
		}

		@Override
		public UK getKey() {
			if (userKey == null) {
				try {
					userKey = deserializeUserKey(rawKeyBytes);
				} catch (IOException e) {
					throw new RuntimeException("Error while deserializing the user key.");
				}
			}

			return userKey;
		}

		@Override
		public UV getValue() {
			if (deleted) {
				return null;
			} else {
				if (userValue == null) {
					try {
						userValue = deserializeUserValue(rawValueBytes);
					} catch (IOException e) {
						throw new RuntimeException("Error while deserializing the user value.");
					}
				}

				return userValue;
			}
		}

		@Override
		public UV setValue(UV value) {
			if (deleted) {
				throw new IllegalStateException("The value has already been deleted.");
			}

			UV oldValue = getValue();

			try {
				userValue = value;
				rawValueBytes = serializeUserValue(value);

				db.put(columnFamily, writeOptions, rawKeyBytes, rawValueBytes);
			} catch (IOException | RocksDBException e) {
				throw new RuntimeException("Error while putting data into RocksDB.", e);
			}

			return oldValue;
		}
	}

	/** An auxiliary utility to scan all entries under the given key. */
	private abstract class RocksDBMapIterator<T> implements Iterator<T> {

		static final int CACHE_SIZE_LIMIT = 128;

		/** The db where data resides. */
		private final RocksDB db;

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
		private ArrayList<RocksDBMapEntry> cacheEntries = new ArrayList<>();
		private int cacheIndex = 0;

		RocksDBMapIterator(final RocksDB db, final byte[] keyPrefixBytes) {
			this.db = db;
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
				throw new IllegalStateException("The remove operation must be called after an valid next operation.");
			}

			RocksDBMapEntry lastEntry = cacheEntries.get(cacheIndex - 1);
			lastEntry.remove();
		}

		final RocksDBMapEntry nextEntry() {
			loadCache();

			if (cacheIndex == cacheEntries.size()) {
				if (!expired) {
					throw new IllegalStateException();
				}

				return null;
			}

			RocksDBMapEntry entry = cacheEntries.get(cacheIndex);
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

			RocksIterator iterator = db.newIterator(columnFamily);

			/*
			 * The iteration starts from the prefix bytes at the first loading. The cache then is
			 * reloaded when the next entry to return is the last one in the cache. At that time,
			 * we will start the iterating from the last returned entry.
 			 */
			RocksDBMapEntry lastEntry = cacheEntries.size() == 0 ? null : cacheEntries.get(cacheEntries.size() - 1);
			byte[] startBytes = (lastEntry == null ? keyPrefixBytes : lastEntry.rawKeyBytes);

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

				if (cacheEntries.size() >= CACHE_SIZE_LIMIT) {
					break;
				}

				RocksDBMapEntry entry = new RocksDBMapEntry(db, iterator.key(), iterator.value());
				cacheEntries.add(entry);

				iterator.next();
			}

			iterator.close();
		}

		private boolean underSameKey(byte[] rawKeyBytes) {
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
	}
}
