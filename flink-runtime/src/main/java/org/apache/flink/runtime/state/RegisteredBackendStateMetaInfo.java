/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Compound meta information for a registered state in a keyed state backend. This combines all serializers and the
 * state name.
 *
 * @param <N> Type of namespace
 * @param <S> Type of state value
 */
public class RegisteredBackendStateMetaInfo<N, S> {

	private final StateDescriptor.Type stateType;
	private final String name;
	private final TypeSerializer<N> namespaceSerializer;
	private final TypeSerializer<S> stateSerializer;

	public RegisteredBackendStateMetaInfo(KeyedBackendSerializationProxy.StateMetaInfo<N, S> metaInfoProxy) {
		this.stateType = metaInfoProxy.getStateType();
		this.name = metaInfoProxy.getStateName();
		this.namespaceSerializer = metaInfoProxy.getNamespaceSerializerSerializationProxy().getTypeSerializer();
		this.stateSerializer = metaInfoProxy.getStateSerializerSerializationProxy().getTypeSerializer();
	}

	public RegisteredBackendStateMetaInfo(
			StateDescriptor.Type stateType,
			String name,
			TypeSerializer<N> namespaceSerializer,
			TypeSerializer<S> stateSerializer) {

		this.stateType = checkNotNull(stateType);
		this.name = checkNotNull(name);
		this.namespaceSerializer = namespaceSerializer;
		this.stateSerializer = stateSerializer;
	}

	public StateDescriptor.Type getStateType() {
		return stateType;
	}

	public String getName() {
		return name;
	}

	public TypeSerializer<N> getNamespaceSerializer() {
		return namespaceSerializer;
	}

	public TypeSerializer<S> getStateSerializer() {
		return stateSerializer;
	}

	public boolean isCompatibleWith(RegisteredBackendStateMetaInfo<?, ?> other) {

		if (this == other) {
			return true;
		}

		if (null == other) {
			return false;
		}

		if (!stateType.equals(StateDescriptor.Type.UNKNOWN)
				&& !other.stateType.equals(StateDescriptor.Type.UNKNOWN)
				&& !stateType.equals(other.stateType)) {
			return false;
		}

		if (!name.equals(other.getName())) {
			return false;
		}

		return ((namespaceSerializer == null && other.namespaceSerializer == null)
					|| namespaceSerializer == null || other.namespaceSerializer == null
					|| namespaceSerializer.isCompatibleWith(other.namespaceSerializer))
				&& stateSerializer.isCompatibleWith(other.stateSerializer);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		RegisteredBackendStateMetaInfo<?, ?> that = (RegisteredBackendStateMetaInfo<?, ?>) o;

		if (!stateType.equals(that.stateType)) {
			return false;
		}

		if (!getName().equals(that.getName())) {
			return false;
		}

		if (getNamespaceSerializer() != null ? !getNamespaceSerializer().equals(that.getNamespaceSerializer()) : that.getNamespaceSerializer() != null) {
			return false;
		}
		return getStateSerializer() != null ? getStateSerializer().equals(that.getStateSerializer()) : that.getStateSerializer() == null;
	}

	@Override
	public String toString() {
		return "RegisteredBackendStateMetaInfo{" +
				"stateType=" + stateType +
				", name='" + name + '\'' +
				", namespaceSerializer=" + namespaceSerializer +
				", stateSerializer=" + stateSerializer +
				'}';
	}

	@Override
	public int hashCode() {
		int result = getName().hashCode();
		result = 31 * result + getStateType().hashCode();
		result = 31 * result + (getNamespaceSerializer() != null ? getNamespaceSerializer().hashCode() : 0);
		result = 31 * result + (getStateSerializer() != null ? getStateSerializer().hashCode() : 0);
		return result;
	}
}
