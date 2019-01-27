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

/**
 * Descriptor for {@link SubKeyedValueState}.
 *
 * @param <K> Type of the keys in the state.
 * @param <N> Type of the namespaces in the state.
 * @param <V> Type of the values in the state.
 */
public final class SubKeyedValueStateDescriptor<K, N, V> extends SubKeyedStateDescriptor<K, N, V, SubKeyedValueState<K, N, V>> {

	private static final long serialVersionUID = 1L;

	/**
	 * Constructor for global states with given name and the serializers for the
	 * keys, the namespaces and the values in the state.
	 *
	 * @param name The name of the state.
	 * @param keySerializer The serializer for the keys in the state.
	 * @param namespaceSerializer The serializer for the namespaces in the state.
	 * @param valueSerializer The serializer for the values in the state.
	 */
	public SubKeyedValueStateDescriptor(
		String name,
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<V> valueSerializer
	) {
		super(name, InternalStateType.SUBKEYED_VALUE, keySerializer, namespaceSerializer, valueSerializer);
	}

	@Override
	public SubKeyedValueState<K, N, V> bind(SubKeyedStateBinder stateBinder) throws Exception {
		return stateBinder.createSubKeyedValueState(this);
	}
}
