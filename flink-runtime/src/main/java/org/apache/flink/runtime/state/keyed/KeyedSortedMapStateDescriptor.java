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

import org.apache.flink.api.common.functions.Comparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.SortedMapSerializer;
import org.apache.flink.runtime.state.InternalStateType;
import org.apache.flink.util.Preconditions;

import java.util.SortedMap;

/**
 * The descriptor for {@link KeyedSortedMapState}.
 *
 * @param <K> Type of the keys in the state.
 * @param <MK> Type of the map keys in the state.
 * @param <MV> Type of the map values in the state.
 */
public final class KeyedSortedMapStateDescriptor<K, MK, MV>
	extends KeyedStateDescriptor<K, SortedMap<MK, MV>, KeyedSortedMapState<K, MK, MV>> {

	private static final long serialVersionUID = 1L;

	/**
	 * Constructor for global state descriptors with given name, the comparator
	 * for the maps and the serializers for the keys, the map keys and the map
	 * values.
	 *
	 * @param name The name of the state.
	 * @param keySerializer The serializer for the keys in the state.
	 * @param mapComparator The comparator for the map keys in the state.
	 * @param mapKeySerializer The serializer for the map keys in the state.
	 * @param mapValueSerializer The serializer for the map values in the state.
	 */
	public KeyedSortedMapStateDescriptor(
		final String name,
		final TypeSerializer<K> keySerializer,
		final Comparator<MK> mapComparator,
		final TypeSerializer<MK> mapKeySerializer,
		final TypeSerializer<MV> mapValueSerializer
	) {
		this(name, keySerializer, new SortedMapSerializer<>(mapComparator, mapKeySerializer, mapValueSerializer));
	}

	/**
	 * Constructor for global state descriptors with given name, the comparator
	 * and the serializer for the sorted maps in the state.
	 *
	 * @param name                The name of the state.
	 * @param keySerializer       The serializer for the keys in the state.
	 * @param sortedMapSerializer The serializer for the sorted maps in the state.
	 */
	public KeyedSortedMapStateDescriptor(
		final String name,
		final TypeSerializer<K> keySerializer,
		final SortedMapSerializer<MK, MV> sortedMapSerializer
	) {
		super(name, InternalStateType.KEYED_SORTEDMAP, keySerializer, sortedMapSerializer);
	}

	@Override
	public SortedMapSerializer<MK, MV> getValueSerializer() {
		TypeSerializer<SortedMap<MK, MV>> sortedMapSerializer = super.getValueSerializer();
		Preconditions.checkState(sortedMapSerializer instanceof SortedMapSerializer);
		return ((SortedMapSerializer<MK, MV>) sortedMapSerializer);
	}

	/**
	 * Returns the comparator for the map keys in the state.
	 *
	 * @return The comparator for the map keys in the state.
	 */
	public Comparator<MK> getMapKeyComparator() {
		return getValueSerializer().getComparator();
	}

	/**
	 * Returns the serializer for the map keys in the state.
	 *
	 * @return The serializer for the map keys in the state.
	 */
	public TypeSerializer<MK> getMapKeySerializer() {
		SortedMapSerializer<MK, MV> sortedMapSerializer = getValueSerializer();
		return sortedMapSerializer.getKeySerializer();
	}

	/**
	 * Returns the serializer for the map values in the state.
	 *
	 * @return The serializer for the map values in the state.
	 */
	public TypeSerializer<MV> getMapValueSerializer() {
		SortedMapSerializer<MK, MV> sortedMapSerializer = getValueSerializer();
		return sortedMapSerializer.getValueSerializer();
	}

	@Override
	public KeyedSortedMapState<K, MK, MV> bind(KeyedStateBinder stateBinder) throws Exception {
		return stateBinder.createKeyedSortedMapState(this);
	}
}

