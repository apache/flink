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
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.runtime.state.InternalStateType;
import org.apache.flink.util.Preconditions;

import java.util.Map;

/**
 * The descriptor for {@link SubKeyedMapState}.
 *
 * @param <K> Type of the keys in the state.
 * @param <N> Type of the namespaces in the state.
 * @param <MK> Type of the map keys in the state.
 * @param <MV> Type of the map values in the state.
 */
public class SubKeyedMapStateDescriptor<K, N, MK, MV> extends SubKeyedStateDescriptor<K, N, Map<MK, MV>, SubKeyedMapState<K, N, MK, MV>> {

	private static final long serialVersionUID = 1L;

	/**
	 * Constructor for global states with given name and the serializers for the
	 * keys, the namespaces, the map keys and the map values in the state.
	 *
	 * @param name The name of the state.
	 * @param keySerializer The serializer for the keys in the state.
	 * @param namespaceSerializer The serializer for the namespaces in the state.
	 * @param mapKeySerializer The serializer for the map keys in the state.
	 * @param mapValueSerializer The serializer for the map values in the state.
	 */
	public SubKeyedMapStateDescriptor(
		String name,
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<MK> mapKeySerializer,
		TypeSerializer<MV> mapValueSerializer
	) {
		this(name, keySerializer, namespaceSerializer,
			new MapSerializer<>(mapKeySerializer, mapValueSerializer));
	}

	/**
	 * Constructor for global states with given name and the serializers for the
	 * keys, the namespaces, the map keys and the map values in the state.
	 *
	 * @param name The name of the state.
	 * @param keySerializer The serializer for the keys in the state.
	 * @param namespaceSerializer The serializer for the namespaces in the state.
	 * @param mapSerializer The serializer for the maps in the state.
	 */
	public SubKeyedMapStateDescriptor(
		String name,
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		MapSerializer<MK, MV> mapSerializer
	) {
		super(name, InternalStateType.SUBKEYED_MAP, keySerializer, namespaceSerializer, mapSerializer);
	}

	@Override
	public MapSerializer<MK, MV> getValueSerializer() {
		TypeSerializer<Map<MK, MV>> mapSerializer = super.getValueSerializer();
		Preconditions.checkState(mapSerializer instanceof MapSerializer);
		return (MapSerializer<MK, MV>) mapSerializer;
	}

	/**
	 * Returns the serializer for the map keys in the state.
	 *
	 * @return The serializer for the map keys in the state.
	 */
	public TypeSerializer<MK> getMapKeySerializer() {
		return getValueSerializer().getKeySerializer();
	}

	/**
	 * Returns the serializer for the map values in the state.
	 *
	 * @return The serializer for the map values in the state.
	 */
	public TypeSerializer<MV> getMapValueSerializer() {
		return getValueSerializer().getValueSerializer();
	}

	@Override
	public SubKeyedMapState<K, N, MK, MV> bind(SubKeyedStateBinder stateBinder) throws Exception {
		return stateBinder.createSubKeyedMapState(this);
	}
}
