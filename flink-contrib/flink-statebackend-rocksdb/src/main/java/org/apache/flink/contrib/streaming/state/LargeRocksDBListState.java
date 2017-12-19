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
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.internal.InternalListState;

import org.apache.flink.shaded.guava18.com.google.common.base.Function;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterators;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;


/**
 * {@link ListState} implementation that stores state in RocksDB using
 * {@link RocksDBMapState}.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the values in the list state.
 */
public class LargeRocksDBListState<K, N, V>
	extends RocksDBMapState<K, N, Integer, V>
	implements InternalListState<N, V> {

	/** State for current list index. */
	private final RocksDBValueState<K, N, Integer> indexState;

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

		super(columnFamily, namespaceSerializer, new MapStateDescriptor<Integer, V>(
						stateDesc.getName() + "::map", new IntSerializer(), stateDesc.getElementSerializer()),
						backend);
		this.indexState = new RocksDBValueState(columnFamily, namespaceSerializer,
						new ValueStateDescriptor<>(stateDesc.getName() + "::index",
										userKeySerializer), backend);
	}

	@Override
	public Iterable<V> get() {
		try {
			Iterator<Map.Entry<Integer, V>> i = this.iterator();
			if (!i.hasNext()) {
				// required by contract and tests
				return null;
			}
			return new Iterable<V>() {
				@Override
				public Iterator<V> iterator() {
					return Iterators.transform(
						i, new Function<Map.Entry<Integer, V>, V>() {
								@Override
								public V apply(Map.Entry<Integer, V> f) {
									return f.getValue();
								}
							});
				}
			};
		} catch (IOException | RocksDBException ex) {
			throw new RuntimeException(ex);
		}
	}

	@Override
	public void add(V value) throws IOException {
		Integer index = this.indexState.value();
		if (index == null) {
			index = 0;
		}
		try {
			put(index++, value);
			this.indexState.update(index);
		} catch (Exception e) {
			throw new RuntimeException("Error while adding data to RocksDB", e);
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
				this.userValueSerializer.serialize(v, out);
			}
			baos.flush();
			return baos.toByteArray();
		}
	}

	@Override
	public void setCurrentNamespace(N namespace) {
		super.setCurrentNamespace(namespace);
		this.indexState.setCurrentNamespace(namespace);
	}

	@Override
	public void clear() {
		super.clear();
		this.indexState.clear();
	}

}
