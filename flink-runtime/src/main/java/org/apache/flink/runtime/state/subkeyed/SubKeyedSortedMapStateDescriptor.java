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

import org.apache.flink.api.common.functions.Comparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.SortedMapSerializer;
import org.apache.flink.runtime.state.InternalStateType;
import org.apache.flink.util.Preconditions;

import java.util.SortedMap;

/**
 * The descriptor for {@link SubKeyedSortedMapState}.
 *
 * @param <K> Type of the keys in the state.
 * @param <N> Type of the namespaces in the state.
 * @param <MK> Type of the map keys in the state.
 * @param <MV> Type of the map values in the state.
 */
public final class SubKeyedSortedMapStateDescriptor<K, N, MK, MV>
	extends SubKeyedStateDescriptor<K, N, SortedMap<MK, MV>, SubKeyedSortedMapState<K, N, MK, MV>> {

	private static final long serialVersionUID = 1L;

	/**
	 * Constructor for global states with given name, the comparator for the
	 * maps and the serializers for the keys, the namespaces, the map keys and the map values
	 * in the state.
	 *
	 * @param name The name of the state.
	 * @param keySerializer The serializer for the keys in the state.
	 * @param namespaceSerializer The serializer for the namespaces in the state.
	 * @param mapKeyComparator The comparator for the map keys in the state.
	 * @param mapKeySerializer The serializer for the map keys in the state.
	 * @param mapValueSerializer The serializer for the map values in the state.
	 */
	public SubKeyedSortedMapStateDescriptor(
		String name,
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		Comparator<MK> mapKeyComparator,
		TypeSerializer<MK> mapKeySerializer,
		TypeSerializer<MV> mapValueSerializer
	) {
		this(name, keySerializer, namespaceSerializer,
			new SortedMapSerializer<>(mapKeyComparator, mapKeySerializer, mapValueSerializer));
	}

	/**
	 * Constructor for global states with given name, the comparator for the
	 * maps and the serializers for the keys, the namespaces, the map keys and the map values
	 * in the state.
	 *
	 * @param name The name of the state.
	 * @param keySerializer The serializer for the keys in the state.
	 * @param namespaceSerializer The serializer for the namespaces in the state.
	 * @param sortedMapSerializer The serializer for the sorted maps in the state.
	 */
	public SubKeyedSortedMapStateDescriptor(
		String name,
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		SortedMapSerializer<MK, MV> sortedMapSerializer
	) {
		super(name, InternalStateType.SUBKEYED_SORTEDMAP, keySerializer, namespaceSerializer, sortedMapSerializer);
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
	public Comparator<MK> getComparator() {
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
	public SubKeyedSortedMapState<K, N, MK, MV> bind(SubKeyedStateBinder stateBinder) throws Exception {
		return stateBinder.createSubKeyedSortedMapState(this);
	}
}
