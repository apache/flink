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

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.internal.InternalFoldingState;

import org.rocksdb.ColumnFamilyHandle;

/**
 * {@link FoldingState} implementation that stores state in RocksDB.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <T> The type of the values that can be folded into the state.
 * @param <ACC> The type of the value in the folding state.
 *
 * @deprecated will be removed in a future version
 */
@Deprecated
class RocksDBFoldingState<K, N, T, ACC>
	extends AbstractRocksDBAppendingState<K, N, T, ACC, ACC>
	implements InternalFoldingState<K, N, T, ACC> {

	/** User-specified fold function. */
	private final FoldFunction<T, ACC> foldFunction;

	/**
	 * Creates a new {@code RocksDBFoldingState}.
	 *
	 * @param columnFamily The RocksDB column family that this state is associated to.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param valueSerializer The serializer for the state.
	 * @param defaultValue The default value for the state.
	 * @param foldFunction The fold function used for folding state.
	 * @param backend The backend for which this state is bind to.
	 */
	private RocksDBFoldingState(
		ColumnFamilyHandle columnFamily,
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<ACC> valueSerializer,
		ACC defaultValue,
		FoldFunction<T, ACC> foldFunction,
		RocksDBKeyedStateBackend<K> backend) {

		super(columnFamily, namespaceSerializer, valueSerializer, defaultValue, backend);

		this.foldFunction = foldFunction;
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
	public TypeSerializer<ACC> getValueSerializer() {
		return valueSerializer;
	}

	@Override
	public ACC get() {
		return getInternal();
	}

	@Override
	public void add(T value) throws Exception {
		byte[] key = getKeyBytes();
		ACC accumulator = getInternal(key);
		accumulator = accumulator == null ? getDefaultValue() : accumulator;
		accumulator = foldFunction.fold(accumulator, value);
		updateInternal(key, accumulator);
	}

	@SuppressWarnings("unchecked")
	static <K, N, SV, S extends State, IS extends S> IS create(
		StateDescriptor<S, SV> stateDesc,
		Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>> registerResult,
		RocksDBKeyedStateBackend<K> backend) {
		return (IS) new RocksDBFoldingState<>(
			registerResult.f0,
			registerResult.f1.getNamespaceSerializer(),
			registerResult.f1.getStateSerializer(),
			stateDesc.getDefaultValue(),
			((FoldingStateDescriptor<?, SV>) stateDesc).getFoldFunction(),
			backend);
	}
}
