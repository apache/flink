/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.state.api.input.BroadcastStateInputFormat;
import org.apache.flink.state.api.input.ListStateInputFormat;
import org.apache.flink.state.api.input.UnionStateInputFormat;

/**
 * An existing savepoint.
 */
@PublicEvolving
@SuppressWarnings("WeakerAccess")
public class ExistingSavepoint {
	private final ExecutionEnvironment env;

	private final String existingSavepoint;

	private final StateBackend stateBackend;

	ExistingSavepoint(ExecutionEnvironment env, String path, StateBackend stateBackend) {
		this.env = env;
		this.existingSavepoint = path;
		this.stateBackend = stateBackend;
	}

	/**
	 * Read operator {@code ListState} from a {@code Savepoint}.
	 * @param uid The uid of the operator.
	 * @param name The (unique) name for the state.
	 * @param typeInfo The type of the elements in the state.
	 * @param <T> The type of the values that are in the list state.
	 * @return A {@code DataSet} representing the elements in state.
	 */
	public <T> DataSet<T> readListState(String uid, String name, TypeInformation<T> typeInfo) {
		ListStateDescriptor<T> descriptor = new ListStateDescriptor<>(name, typeInfo);
		ListStateInputFormat<T> inputFormat = new ListStateInputFormat<>(existingSavepoint, uid, descriptor);
		return env.createInput(inputFormat, typeInfo);
	}

	/**
	 * Read operator {@code ListState} from a {@code Savepoint} when a
	 * custom serializer was used; e.g., a different serializer than the
	 * one returned by {@code TypeInformation#createSerializer}.
	 * @param uid The uid of the operator.
	 * @param name The (unique) name for the state.
	 * @param typeInfo The type of the elements in the state.
	 * @param serializer The serializer used to write the elements into state.
	 * @param <T> The type of the values that are in the list state.
	 * @return A {@code DataSet} representing the elements in state.
	 */
	public <T> DataSet<T> readListState(
		String uid,
		String name,
		TypeInformation<T> typeInfo,
		TypeSerializer<T> serializer) {

		ListStateDescriptor<T> descriptor = new ListStateDescriptor<>(name, serializer);
		ListStateInputFormat<T> inputFormat = new ListStateInputFormat<>(existingSavepoint, uid, descriptor);
		return env.createInput(inputFormat, typeInfo);
	}

	/**
	 * Read operator {@code UnionState} from a {@code Savepoint}.
	 * @param uid The uid of the operator.
	 * @param name The (unique) name for the state.
	 * @param typeInfo The type of the elements in the state.
	 * @param <T> The type of the values that are in the union state.
	 * @return A {@code DataSet} representing the elements in state.
	 */
	public <T> DataSet<T> readUnionState(String uid, String name, TypeInformation<T> typeInfo) {
		ListStateDescriptor<T> descriptor = new ListStateDescriptor<>(name, typeInfo);
		UnionStateInputFormat<T> inputFormat = new UnionStateInputFormat<>(existingSavepoint, uid, descriptor);
		return env.createInput(inputFormat, typeInfo);
	}

	/**
	 * Read operator {@code UnionState} from a {@code Savepoint} when a
	 * custom serializer was used; e.g., a different serializer than the
	 * one returned by {@code TypeInformation#createSerializer}.
	 * @param uid The uid of the operator.
	 * @param name The (unique) name for the state.
	 * @param typeInfo The type of the elements in the state.
	 * @param serializer The serializer used to write the elements into state.
	 * @param <T> The type of the values that are in the union state.
	 * @return A {@code DataSet} representing the elements in state.
	 */
	public <T> DataSet<T> readUnionState(
		String uid,
		String name,
		TypeInformation<T> typeInfo,
		TypeSerializer<T> serializer) {

		ListStateDescriptor<T> descriptor = new ListStateDescriptor<>(name, serializer);
		UnionStateInputFormat<T> inputFormat = new UnionStateInputFormat<>(existingSavepoint, uid, descriptor);
		return env.createInput(inputFormat, typeInfo);
	}

	/**
	 * Read operator {@code BroadcastState} from a {@code Savepoint}.
	 * @param uid The uid of the operator.
	 * @param name The (unique) name for the state.
	 * @param keyTypeInfo The type information for the keys in the state.
	 * @param valueTypeInfo The type information for the values in the state.
	 * @param <K> The type of keys in state.
	 * @param <V> The type of values in state.
	 * @return A {@code DataSet} of key-value pairs from state.
	 */
	public <K, V> DataSet<Tuple2<K, V>> readBroadcastState(
		String uid,
		String name,
		TypeInformation<K> keyTypeInfo,
		TypeInformation<V> valueTypeInfo) {

		MapStateDescriptor<K, V> descriptor = new MapStateDescriptor<>(name, keyTypeInfo, valueTypeInfo);
		BroadcastStateInputFormat<K, V> inputFormat = new BroadcastStateInputFormat<>(existingSavepoint, uid, descriptor);
		return env.createInput(inputFormat, new TupleTypeInfo<>(keyTypeInfo, valueTypeInfo));
	}

	/**
	 * Read operator {@code BroadcastState} from a {@code Savepoint}
	 * when a custom serializer was used; e.g., a different serializer
	 * than the one returned by {@code TypeInformation#createSerializer}.
	 * @param uid The uid of the operator.
	 * @param name The (unique) name for the state.
	 * @param keyTypeInfo The type information for the keys in the state.
	 * @param valueTypeInfo The type information for the values in the state.
	 * @param keySerializer The type serializer used to write keys into the state.
	 * @param valueSerializer The type serializer used to write values into the state.
	 * @param <K> The type of keys in state.
	 * @param <V> The type of values in state.
	 * @return A {@code DataSet} of key-value pairs from state.
	 */
	public <K, V> DataSet<Tuple2<K, V>> readBroadcastState(
		String uid,
		String name,
		TypeInformation<K> keyTypeInfo,
		TypeInformation<V> valueTypeInfo,
		TypeSerializer<K> keySerializer,
		TypeSerializer<V> valueSerializer) {

		MapStateDescriptor<K, V> descriptor = new MapStateDescriptor<>(name, keySerializer, valueSerializer);
		BroadcastStateInputFormat<K, V> inputFormat = new BroadcastStateInputFormat<>(existingSavepoint, uid, descriptor);
		return env.createInput(inputFormat, new TupleTypeInfo<>(keyTypeInfo, valueTypeInfo));
	}
}
