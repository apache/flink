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

import org.apache.flink.api.common.typeutils.SerializationException;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.AbstractInternalStateBackend;
import org.apache.flink.runtime.state.StateStorage;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * An implementation of {@link SubKeyedMapState} backed by a state storage.
 *
 * @param <K> Type of the keys in the state.
 * @param <N> Type of the namespaces in the state.
 * @param <MK> Type of the map keys in the state.
 * @param <MV> Type of the map values in the state.
 */
public final class SubKeyedMapStateImpl<K, N, MK, MV>
	extends AbstractSubKeyedMapStateImpl<K, N, MK, MV, Map<MK, MV>>
	implements SubKeyedMapState<K, N, MK, MV> {

	/**
	 * The descriptor of current state.
	 */
	private SubKeyedMapStateDescriptor stateDescriptor;

	/**
	 * Constructor with the state storage to store mappings.
	 *
	 * @param stateStorage The state storage where mappings are stored.
	 */
	public SubKeyedMapStateImpl(
		AbstractInternalStateBackend internalStateBackend,
		SubKeyedMapStateDescriptor descriptor,
		StateStorage stateStorage
	) {
		super(internalStateBackend, stateStorage);

		this.stateDescriptor = Preconditions.checkNotNull(descriptor);
		this.keySerializer = descriptor.getKeySerializer();
		this.namespaceSerializer = descriptor.getNamespaceSerializer();
		this.mapKeySerializer = descriptor.getMapKeySerializer();
		this.mapValueSerializer = descriptor.getMapValueSerializer();
		try {
			outputStream.reset();
			StringSerializer.INSTANCE.serialize(descriptor.getName(), outputView);
			stateNameByte = outputStream.toByteArray();
		} catch (Exception e) {
			throw new SerializationException(e);
		}
		this.stateNameForSerialize = stateStorage.supportMultiColumnFamilies() ? null : stateNameByte;
		this.serializedStateNameLength = stateNameForSerialize == null ? 0 : stateNameForSerialize.length;
	}

	@Override
	public SubKeyedMapStateDescriptor getDescriptor() {
		return stateDescriptor;
	}

	@Override
	Map<MK, MV> createMap() {
		return new HashMap<>();
	}
}
