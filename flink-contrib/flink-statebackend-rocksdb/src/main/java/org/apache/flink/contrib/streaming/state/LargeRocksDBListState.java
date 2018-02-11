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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.internal.InternalListState;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;


/**
 * {@link ListState} implementation that stores state in RocksDB using
 * {@link RocksDBMapState}.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the values in the list state.
 */
public class LargeRocksDBListState<K, N, V>
	extends AbstractRocksDBListState<K, N, V>
	implements InternalListState<N, V> {

	private static final Logger LOG = LoggerFactory.getLogger(LargeRocksDBListState.class);

	private static final TypeSerializer<Integer> INDEX_SERIALIZER = IntSerializer.INSTANCE;

	/**
	 * Creates a new {@code LargeRocksDBListState}.
	 *
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param stateDesc The state identifier for the state. This contains name
	 *                     and can create a default state value.
	 * @param backend the rocksdb backend
	 */
	public LargeRocksDBListState(ColumnFamilyHandle columnFamily,
			TypeSerializer<N> namespaceSerializer,
			ListStateDescriptor<V> stateDesc,
			RocksDBKeyedStateBackend<K> backend) {

		super(columnFamily, namespaceSerializer, stateDesc, backend);
	}

	@Override
	public Iterable<V> get() {
		try {
			keySerializationStream.reset();
			writeCurrentKeyWithGroupAndNamespace();
			int count = getIndexWithNamespaceWritten(keySerializationStream.toByteArray());
			if (count == 0) {
				// required by contract and tests
				return null;
			}

			final byte[] keyPrefixBytes = keySerializationStream.toByteArray();
			return () -> new RocksDBListIterator(keyPrefixBytes);
		} catch (IOException | RocksDBException ex) {
			throw new RuntimeException(ex);
		}
	}

	@Override
	public void add(V value) throws IOException {
		if (value != null) {
			int index = getAndIncrementIndex();
			try {
				putAt(index, value);
			} catch (Exception e) {
				throw new RuntimeException("Error while adding data to RocksDB", e);
			}
		}
	}

	@Override
	public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
		for (N source : sources) {
			this.setCurrentNamespace(source);
			Iterable<V> values = get();
			if (values != null) {
				this.setCurrentNamespace(target);
				for (V v : values) {
					add(v);
				}
				this.setCurrentNamespace(source);
				clear();
			}
		}
	}

	@Override
	public byte[] getSerializedValue(byte[] serializedKeyAndNamespace) throws Exception {
		// serialize the list as single byte blob
		try (ByteArrayInputStreamWithPos bais = new ByteArrayInputStreamWithPos(serializedKeyAndNamespace);
				DataInputViewStreamWrapper in = new DataInputViewStreamWrapper(bais)) {

			Tuple3<Integer, K, N> ns = readKeyWithGroupAndNamespace(bais, in);
			setCurrentNamespace(ns.f2);
		}

		Iterable<V> values = get();
		if (values == null) {
			return null;
		}
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
				DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(baos)) {
			byte[] separator = null;
			for (V v : values) {
				if (separator != null) {
					out.write(separator);
				} else {
					separator = new byte[] { 0 };
				}
				this.valueSerializer.serialize(v, out);
			}
			baos.flush();
			return baos.toByteArray();
		}
	}

	@Override
	public void clear() {
		try {
			Iterable<V> iter = get();
			if (iter != null) {
				Iterator<V> it = iter.iterator();
				while (it.hasNext()) {
					it.next();
					it.remove();
				}
			}
			removeIndex();
		} catch (Exception e) {
			LOG.warn("Error while cleaning the state.", e);
		}

	}

	@Override
	public void update(List<V> list) throws Exception {
		clear();
		addAll(list);
	}

	@Override
	public void addAll(List<V> list) throws Exception {
		if (list != null) {
			for (V value : list) {
				add(value);
			}
		}
	}

	private byte[] serializeIndexWithNamespace(int index) throws IOException {
		writeCurrentKeyWithGroupAndNamespace();
		INDEX_SERIALIZER.serialize(index, keySerializationDataOutputView);
		return keySerializationStream.toByteArray();
	}

	private void putAt(int index, V value) throws RocksDBException, IOException {
		byte[] rawKeyBytes = serializeIndexWithNamespace(index);

		keySerializationStream.reset();
		DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(keySerializationStream);
		valueSerializer.serialize(value, out);
		byte[] rawValueBytes = keySerializationStream.toByteArray();
		backend.db.put(columnFamily, writeOptions, rawKeyBytes, rawValueBytes);
	}

	private int getIndexWithNamespaceWritten(byte[] key) throws IOException, RocksDBException {
		byte[] valueBytes = backend.db.get(columnFamily, key);
		if (valueBytes == null) {
			return 0;
		}
		return INDEX_SERIALIZER.deserialize(
				new DataInputViewStreamWrapper(new ByteArrayInputStream(valueBytes)));
	}

	private int getAndIncrementIndex() {
		try {
			writeCurrentKeyWithGroupAndNamespace();
			byte[] key = keySerializationStream.toByteArray();
			int index = getIndexWithNamespaceWritten(key);
			keySerializationStream.reset();
			DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(keySerializationStream);
			INDEX_SERIALIZER.serialize(++index, out);
			out.flush();
			backend.db.put(columnFamily, writeOptions, key, keySerializationStream.toByteArray());
			return index;
		} catch (IOException | RocksDBException e) {
			throw new RuntimeException("Error while retrieving data from RocksDB.", e);
		}
	}

	private void removeAt(int index) throws IOException, RocksDBException {
		byte[] rawKeyBytes = serializeIndexWithNamespace(index);

		backend.db.delete(columnFamily, writeOptions, rawKeyBytes);
	}

	private void removeIndex() throws IOException, RocksDBException {
		writeCurrentKeyWithGroupAndNamespace();
		byte[] key = keySerializationStream.toByteArray();
		backend.db.delete(columnFamily, writeOptions, key);
	}

	// ------------------------------------------------------------------------
	//  Internal Classes
	// ------------------------------------------------------------------------

	private final class RocksDBListIterator implements Iterator<V> {

		private final RocksDBPrefixIterator<byte[], V> iter;

		private RocksDBListIterator(byte[] keyPrefixBytes) {
			this.iter = new RocksDBPrefixIterator<byte[], V>(LargeRocksDBListState.this, keyPrefixBytes) {
				@Override
				byte[] deserializeKey(byte[] bytes) {
					return bytes;
				}

				@Override
				V deserializeValue(byte[] bytes) {
					ByteArrayInputStreamWithPos bais = new ByteArrayInputStreamWithPos(bytes);
					DataInputViewStreamWrapper in = new DataInputViewStreamWrapper(bais);
					try {
						return valueSerializer.deserialize(in);
					} catch (IOException ex) {
						throw new RuntimeException(ex);
					}
				}

				@Override
				byte[] serializeValue(V value) {
					try {
						keySerializationStream.reset();
						valueSerializer.serialize(value, keySerializationDataOutputView);
						return keySerializationStream.toByteArray();
					} catch (IOException ex) {
						throw new RuntimeException(ex);
					}
				}

			};
		}

		@Override
		public boolean hasNext() {
			return iter.hasNext();
		}

		@Override
		public V next() {
			return iter.next().getValue();
		}

		@Override
		public void remove() {
			iter.remove();
		}

	}
}
