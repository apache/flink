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

package org.apache.flink.runtime.state.subkeyed;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.InternalStateType;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Objects;

/**
 * The descriptor for {@link SubKeyedState}.
 *
 * @param <K> The stateType of the keys in the state.
 * @param <N> The stateType of the namespaces in the state.
 * @param <V> The stateType of the values in the state.
 * @param <S> The stateType of the state described by the descriptor
 */
public abstract class SubKeyedStateDescriptor<K, N, V, S extends SubKeyedState<K, N, V>> implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * The name of the state.
	 */
	private final String name;

	/**
	 * The serializer for the keys in the state.
	 */
	private final TypeSerializer<K> keySerializer;

	/**
	 * The serializer for the namespaces in the state.
	 */
	private final TypeSerializer<N> namespaceSerializer;

	/**
	 * The serializer for the values in the state.
	 */
	private final TypeSerializer<V> valueSerializer;

	/**
	 * The stateType of the values in the state.
	 */
	private final InternalStateType stateType;

	/**
	 * Constructor for global states with given name and the serializers for
	 * the keys, the namespaces and the values in the state.
	 *
	 * @param name The name of the state.
	 * @param keySerializer The serializer for the keys in the state.
	 * @param namespaceSerializer The serializer for the namespaces in the state.
	 * @param valueSerializer The serializer for the values in the state.
	 */
	public SubKeyedStateDescriptor(
		final String name,
		final InternalStateType stateType,
		final TypeSerializer<K> keySerializer,
		final TypeSerializer<N> namespaceSerializer,
		final TypeSerializer<V> valueSerializer
	) {
		Preconditions.checkNotNull(name);
		Preconditions.checkNotNull(stateType);
		Preconditions.checkNotNull(keySerializer);
		Preconditions.checkNotNull(namespaceSerializer);
		Preconditions.checkNotNull(valueSerializer);

		this.name = name;
		this.stateType = stateType;
		this.keySerializer = keySerializer;
		this.namespaceSerializer = namespaceSerializer;
		this.valueSerializer = valueSerializer;
	}

	/**
	 * Creates the state described by the descriptor with the given binder.
	 *
	 * @param stateBinder The binder with which to create the state.
	 * @return The state described by the descriptor.
	 */
	public abstract S bind(SubKeyedStateBinder stateBinder) throws Exception ;

	//--------------------------------------------------------------------------

	/**
	 * Returns the name of the state.
	 *
	 * @return The name of the state.
	 */
	public String getName() {
		return name;
	}

	/**
	 * Returns the state descriptor stateType.
	 *
	 * @return The state descriptor stateType.
	 */
	public InternalStateType getStateType() {
		return stateType;
	}

	/**
	 * Returns the serializer for the keys in the state.
	 *
	 * @return The serializer for the keys in the state.
	 */
	public TypeSerializer<K> getKeySerializer() {
		return keySerializer;
	}

	/**
	 * Returns the serializer for the namespaces in the state.
	 */
	public TypeSerializer<N> getNamespaceSerializer() {
		return namespaceSerializer;
	}

	/**
	 * Returns the serializer for the values in the state.
	 *
	 * @return The serializer for the values in the state.
	 */
	public TypeSerializer<V> getValueSerializer() {
		return valueSerializer;
	}

	//--------------------------------------------------------------------------

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		SubKeyedStateDescriptor<?, ?, ?, ?> that = (SubKeyedStateDescriptor<?, ?, ?, ?>) o;

		return Objects.equals(name, that.name) &&
			Objects.equals(keySerializer, that.keySerializer) &&
			Objects.equals(namespaceSerializer, that.namespaceSerializer) &&
			Objects.equals(valueSerializer, that.valueSerializer);
	}

	@Override
	public int hashCode() {
		int result = Objects.hashCode(name);
		result = 31 * result + Objects.hashCode(keySerializer);
		result = 31 * result + Objects.hashCode(namespaceSerializer);
		result = 31 * result + Objects.hashCode(valueSerializer);
		return result;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + "{" +
			"name=" + name +
			", keySerializer=" + keySerializer +
			", namespaceSerializer=" + namespaceSerializer +
			", valueSerializer=" + valueSerializer +
			"}";
	}
}

