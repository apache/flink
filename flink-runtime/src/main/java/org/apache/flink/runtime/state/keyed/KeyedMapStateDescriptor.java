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
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.runtime.state.InternalStateType;
import org.apache.flink.util.Preconditions;

import java.util.Map;

/**
 * The descriptor for {@link KeyedMapState}.
 *
 * @param <K> Type of the keys in the state.
 * @param <MK> Type of the map keys in the state.
 * @param <MV> Type of the map values in the state.
 */
public class KeyedMapStateDescriptor<K, MK, MV>
	extends KeyedStateDescriptor<K, Map<MK, MV>, KeyedMapState<K, MK, MV>> {

	private static final long serialVersionUID = 1L;

	/**
	 * Constructor for global state descriptors with given name and the
	 * serializers for the keys, the map keys and the map values in the state.
	 *
	 * @param name The name of the state.
	 * @param keySerializer The serializer for the keys in the state.
	 * @param mapKeySerializer The serializer for the map keys in the state.
	 * @param mapValueSerializer The serializer for the map values in the state.
	 */
	public KeyedMapStateDescriptor(
		final String name,
		final TypeSerializer<K> keySerializer,
		final TypeSerializer<MK> mapKeySerializer,
		final TypeSerializer<MV> mapValueSerializer
	) {
		this(name, keySerializer, new MapSerializer<>(mapKeySerializer, mapValueSerializer));
	}

	/**
	 * Constructor for global state descriptors with given name and the
	 * serializer for the keys and maps in the state.
	 *
	 * @param name The name of the state.
	 * @param keySerializer The serializer for the keys in the state.
	 * @param mapSerializer The serializer for the maps in the state.
	 */
	public KeyedMapStateDescriptor(
		final String name,
		final TypeSerializer<K> keySerializer,
		final MapSerializer<MK, MV> mapSerializer
	) {
		super(name, InternalStateType.KEYED_MAP, keySerializer, mapSerializer);
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
	public KeyedMapState<K, MK, MV> bind(KeyedStateBinder stateBinder) throws Exception {
		return stateBinder.createKeyedMapState(this);
	}
}
