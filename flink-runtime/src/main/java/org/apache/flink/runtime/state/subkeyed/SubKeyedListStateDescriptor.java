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
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.runtime.state.InternalStateType;
import org.apache.flink.util.Preconditions;

import java.util.List;

/**
 * The descriptor for {@link SubKeyedListState}.
 *
 * @param <K> Type of the keys in the state.
 * @param <N> Type of the namespaces in the state.
 * @param <E> Type of the elements in the state.
 */
public final class SubKeyedListStateDescriptor<K, N, E> extends SubKeyedStateDescriptor<K, N, List<E>, SubKeyedListState<K, N, E>> {

	private static final long serialVersionUID = 1L;

	/**
	 * Constructor for global states with given name and the serializers for the
	 * keys, the namespaces, and the elements in the state.
	 *
	 * @param name The name of the state.
	 * @param keySerializer The serializer for the keys in the state.
	 * @param namespaceSerializer The serializer for namespaces in the state.
	 * @param elementSerializer The serializer for the elements in the state.
	 */
	public SubKeyedListStateDescriptor(
		String name,
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<E> elementSerializer
	) {
		super(name, InternalStateType.SUBKEYED_LIST, keySerializer, namespaceSerializer,
			new ListSerializer<>(elementSerializer));
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
	public SubKeyedListState<K, N, E> bind(SubKeyedStateBinder stateBinder) throws Exception  {
		return stateBinder.createSubKeyedListState(this);
	}
}

