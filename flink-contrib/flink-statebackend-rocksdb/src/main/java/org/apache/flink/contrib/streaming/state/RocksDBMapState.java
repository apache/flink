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
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

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
	final TypeSerializer<UK> userKeySerializer;
	final TypeSerializer<UV> userValueSerializer;

	/**
	 * We disable writes to the write-ahead-log here. We can't have these in the base class
	 * because JNI segfaults for some reason if they are.
	 */
	final WriteOptions writeOptions;

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

		writeOptions = new WriteOptions();
		writeOptions.setDisableWAL(true);
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

		backend.db.remove(columnFamily, writeOptions, rawKeyBytes);
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
				return new RocksDBMapIterator<>(prefixBytes, Map.Entry::getKey);
			}
		};
	}

	@Override
	public Iterable<UV> values() throws IOException, RocksDBException {
		final byte[] prefixBytes = serializeCurrentKeyAndNamespace();

		return new Iterable<UV>() {
			@Override
			public Iterator<UV> iterator() {
				return new RocksDBMapIterator<>(prefixBytes, Map.Entry::getValue);
			}
		};
	}

	@Override
	public Iterator<Map.Entry<UK, UV>> iterator() throws IOException, RocksDBException {
		final byte[] prefixBytes = serializeCurrentKeyAndNamespace();

		return new RocksDBMapIterator<>(prefixBytes, e -> e);
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

		final Iterator<Map.Entry<UK, UV>> iterator = new RocksDBMapIterator<>(keyPrefixBytes, e -> e);

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

		return keySerializationStream.toByteArray();
	}

	private byte[] serializeUserKeyWithCurrentKeyAndNamespace(UK userKey) throws IOException {
		writeCurrentKeyWithGroupAndNamespace();
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

		readKeyWithGroupAndNamespace(bais, in);

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

	/** An auxiliary utility to scan all entries under the given key. */
	private final class RocksDBMapIterator<T> implements Iterator<T> {

		private final RocksDBPrefixIterator<UK, UV> rocksIter;
		private final Function<Map.Entry<UK, UV>, T> map;

		<K, V> RocksDBMapIterator(
						byte[] keyPrefixBytes,
						Function<Map.Entry<UK, UV>, T> map) {

			rocksIter = new RocksDBPrefixIterator<UK, UV>(RocksDBMapState.this, keyPrefixBytes) {

				@Override
				UK deserializeKey(byte[] bytes) {
					try {
						return deserializeUserKey(bytes);
					} catch (IOException ex) {
						throw new RuntimeException(ex);
					}
				}

				@Override
				UV deserializeValue(byte[] bytes) {
					try {
						return deserializeUserValue(bytes);
					} catch (IOException ex) {
						throw new RuntimeException(ex);
					}
				}

				@Override
				byte[] serializeValue(UV value) {
					try {
						return serializeUserValue(value);
					} catch (IOException ex) {
						throw new RuntimeException(ex);
					}
				}

			};

			this.map = map;
		}

		@Override
		public boolean hasNext() {
			return rocksIter.hasNext();
		}

		@Override
		public void remove() {
			rocksIter.remove();
		}

		@Override
		public T next() {
			return map.apply(rocksIter.next());
		}

	}
}
