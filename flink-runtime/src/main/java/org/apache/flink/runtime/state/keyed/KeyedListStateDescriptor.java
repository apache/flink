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
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.runtime.state.InternalStateType;
import org.apache.flink.util.Preconditions;

import java.util.List;

/**
 * The descriptor for {@link KeyedListState}.
 *
 * @param <K> Type of the keys in the state.
 * @param <E> Type of the elements in the state.
 */
public final class KeyedListStateDescriptor<K, E> extends KeyedStateDescriptor<K, List<E>, KeyedListState<K, E>> {

	private static final long serialVersionUID = 1L;

	/**
	 * Constructor with given name and the serializers for the keys and the
	 * elements in the state.
	 *
	 * @param name The name of the state.
	 * @param keySerializer The serializer for the keys in the state.
	 * @param elementSerializer The serializer for the elements in the state.
	 */
	public KeyedListStateDescriptor(
		final String name,
		final TypeSerializer<K> keySerializer,
		final TypeSerializer<E> elementSerializer
	) {
		this(name, keySerializer, new ListSerializer<>(elementSerializer));
	}

	/**
	 * Constructor with given name and the serializers for the keys and the
	 * lists in the state.
	 *
	 * @param name The name of the state.
	 * @param keySerializer The serializer for the keys in the state.
	 * @param listSerializer The serializer for the lists in the state.
	 */
	public KeyedListStateDescriptor(
		final String name,
		final TypeSerializer<K> keySerializer,
		final ListSerializer<E> listSerializer
	) {
		super(name, InternalStateType.KEYED_LIST, keySerializer, listSerializer);
	}

	@Override
	public ListSerializer<E> getValueSerializer() {
		TypeSerializer<List<E>> listSerializer = super.getValueSerializer();
		Preconditions.checkState(listSerializer instanceof ListSerializer);
		return (ListSerializer<E>) listSerializer;
	}

	/**
	 * Returns the serializer for the elements in the state.
	 *
	 * @return The serializer for the elements in the state.
	 */
	public TypeSerializer<E> getElementSerializer() {
		return getValueSerializer().getElementSerializer();
	}

	@Override
	public KeyedListState<K, E> bind(KeyedStateBinder stateBinder) throws Exception {
		return stateBinder.createKeyedListState(this);
	}
}
