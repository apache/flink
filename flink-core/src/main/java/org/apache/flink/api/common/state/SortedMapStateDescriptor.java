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

package org.apache.flink.api.common.state;

import org.apache.flink.api.common.functions.Comparator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.SortedMapSerializer;
import org.apache.flink.api.java.typeutils.SortedMapTypeInfo;
import org.apache.flink.util.Preconditions;

import java.util.Objects;
import java.util.SortedMap;

/**
 * The descriptor for {@link SortedMapState}.
 *
 *<p>Notice: If we use stateBackend which would store keys in bytes format, please ensure the comparator and serializer are both ordered.
 * e.g. string "abc" must before "bd" when serialized to bytes if we expect them in lexicographical order.
 *
 * @param <K> Type of the keys in the state.
 * @param <V> Type of the values in the state.
 */
public final class SortedMapStateDescriptor<K, V> extends StateDescriptor<SortedMapState<K, V>, SortedMap<K, V>> {

	private static final long serialVersionUID = 1L;

	/**
	 * Create a new {@code SortedMapStateDescriptor} with given name, the
	 * comparator for the keys, and the serializers for the keys and the values
	 * in the state.
	 *
	 * @param name The name of the state.
	 * @param comparator The comparator for the keys in the state.
	 * @param keySerializer The serializer for the keys in the state.
	 * @param valueSerializer The serializer for the values in the state.
	 */
	public SortedMapStateDescriptor(
		String name,
		Comparator<K> comparator,
		TypeSerializer<K> keySerializer,
		TypeSerializer<V> valueSerializer
	) {
		super(name, new SortedMapSerializer<>(comparator, keySerializer,
			valueSerializer), null);
	}

	/**
	 * Create a new {@code SortedMapStateDescriptor} with given name, the
	 * comparator for the keys, and the type information for the keys and the
	 * values in the state.
	 *
	 * @param name The name of the state.
	 * @param comparator The comparator for the keys in the state.
	 * @param keyTypeInformation The type information for the keys in the state.
	 * @param valueTypeInformation The type information for the values in the state.
	 */
	public SortedMapStateDescriptor(
		String name,
		Comparator<K> comparator,
		TypeInformation<K> keyTypeInformation,
		TypeInformation<V> valueTypeInformation
	) {
		super(name, new SortedMapTypeInfo<>(keyTypeInformation,
			valueTypeInformation, comparator), null);
	}


	/**
	 * Create a new {@code SortedMapStateDescriptor} with the given name and the
	 * classes of the keys and the values.
	 *
	 * <p>If this constructor fails (because it is not possible to describe the
	 * type information via the classes), consider using the
	 * {@link #SortedMapStateDescriptor(String, Comparator, TypeInformation, TypeInformation)}
	 * constructor.
	 *
	 * @param name The name of the {@code MapStateDescriptor}.
	 * @param keyClass The class of the type of keys in the state.
	 * @param valueClass The class of the type of values in the state.
	 */
	public SortedMapStateDescriptor(
		String name,
		Comparator<K> comparator,
		Class<K> keyClass,
		Class<V> valueClass
	) {
		super(name, new SortedMapTypeInfo<>(keyClass, valueClass, comparator), null);
	}

	@Override
	public SortedMapState<K, V> bind(StateBinder stateBinder) throws Exception {
		return stateBinder.createSortedMapState(this);
	}

	@Override
	public SortedMapSerializer<K, V> getSerializer() {
		TypeSerializer<SortedMap<K, V>> sortedMapSerializer =
			super.getSerializer();
		Preconditions.checkState(sortedMapSerializer instanceof SortedMapSerializer);
		return (SortedMapSerializer<K, V>) sortedMapSerializer;
	}

	public TypeInformation<SortedMap<K, V>> getTypeInformation() {
		return typeInfo;
	}

	/**
	 * Gets the serializer for the keys in the state.
	 *
	 * @return The serializer for the keys in the state.
	 */
	public TypeSerializer<K> getKeySerializer() {
		final TypeSerializer<SortedMap<K, V>> rawSerializer = getSerializer();
		if (!(rawSerializer instanceof SortedMapSerializer)) {
			throw new IllegalStateException("Unexpected serializer type.");
		}

		return ((SortedMapSerializer<K, V>) rawSerializer).getKeySerializer();
	}

	/**
	 * Gets the serializer for the values in the state.
	 *
	 * @return The serializer for the values in the state.
	 */
	public TypeSerializer<V> getValueSerializer() {
		final TypeSerializer<SortedMap<K, V>> rawSerializer = getSerializer();
		if (!(rawSerializer instanceof SortedMapSerializer)) {
			throw new IllegalStateException("Unexpected serializer type.");
		}

		return ((SortedMapSerializer<K, V>) rawSerializer).getValueSerializer();
	}

	@Override
	public Type getType() {
		return Type.SORTEDMAP;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		SortedMapStateDescriptor<?, ?> that = (SortedMapStateDescriptor<?, ?>) o;
		return name.equals(that.name) &&
			Objects.equals(typeInfo, that.getTypeInformation()) &&
			Objects.equals(serializer, that.getSerializer());
	}

	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + getClass().hashCode();
		result = 31 + result + getTypeInformation().hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "SortedMapStateDescriptor{" +
			"name= " + getName() +
			", typeSerializer=" + serializer +
			", typeInformation=" + typeInfo +
			"}";
	}
}


