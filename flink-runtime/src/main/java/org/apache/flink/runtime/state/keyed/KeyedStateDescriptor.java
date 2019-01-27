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

package org.apache.flink.runtime.state.keyed;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.InternalStateType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/**
 * The descriptor for both local and global {@link KeyedState}.
 *
 * @param <K> The stateType of the keys in the state3.
 * @param <V> The stateType of the values in the state.
 * @param <S> The stateType of the state described by the descriptor
 */
public abstract class KeyedStateDescriptor<K, V, S extends KeyedState<K, V>> implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * The name of the state.
	 */
	private final String name;

	/**
	 * The serializer for the keys in the state.
	 */
	private TypeSerializer<K> keySerializer;

	/**
	 * The serializer for the values in the state.
	 */
	private TypeSerializer<V> valueSerializer;

	/**
	 * The stateType of the values in the state.
	 */
	private final InternalStateType stateType;

	/** Name for queries against state created from this StateDescriptor. */
	@Nullable
	private String queryableStateName;

	/**
	 * Constructor for global states with given name and the serializers for
	 * the keys and the values in the state.
	 *
	 * @param name The name of the state.
	 * @param keySerializer The serializer for the keys in the state.
	 * @param valueSerializer The serializer for the values in the state.
	 */
	KeyedStateDescriptor(
		final String name,
		final InternalStateType stateType,
		final TypeSerializer<K> keySerializer,
		final TypeSerializer<V> valueSerializer
	) {
		Preconditions.checkNotNull(name);
		Preconditions.checkNotNull(stateType);
		Preconditions.checkNotNull(keySerializer);
		Preconditions.checkNotNull(valueSerializer);

		this.name = name;
		this.stateType = stateType;
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
	}

	/**
	 * Creates the state described by the descriptor with the given binder.
	 *
	 * @param stateBinder The binder with which to create the state.
	 * @return The state described by the descriptor.
	 */
	public abstract S bind(KeyedStateBinder stateBinder) throws Exception;

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
	 * Returns the serializer for the values in the state.
	 *
	 * @return The serializer for the values in the state.
	 */
	public TypeSerializer<V> getValueSerializer() {
		return valueSerializer;
	}

	public void setKeySerializer(TypeSerializer<K> keySerializer) {
		this.keySerializer = keySerializer;
	}

	public void setValueSerializer(TypeSerializer<V> valueSerializer) {
		this.valueSerializer = valueSerializer;
	}

	/**
	 * Returns whether the state created from this descriptor is queryable.
	 *
	 * @return <code>true</code> if state is queryable, <code>false</code>
	 * otherwise.
	 */
	public boolean isQueryable() {
		return queryableStateName != null;
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

		KeyedStateDescriptor<?, ?, ?> that = (KeyedStateDescriptor<?, ?, ?>) o;

		return Objects.equals(name, that.name) &&
			Objects.equals(keySerializer, that.keySerializer) &&
			Objects.equals(valueSerializer, that.valueSerializer);
	}

	@Override
	public int hashCode() {
		int result = Objects.hashCode(name);
		result = 31 * result + Objects.hashCode(keySerializer);
		result = 31 * result + Objects.hashCode(valueSerializer);
		return result;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + "{" +
			"name=" + name +
			", keySerializer=" + keySerializer +
			", valueSerializer=" + valueSerializer +
			"}";
	}
}
