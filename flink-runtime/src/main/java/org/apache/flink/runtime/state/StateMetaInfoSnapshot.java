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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.SortedMapSerializer;
import org.apache.flink.runtime.state.keyed.KeyedListStateDescriptor;
import org.apache.flink.runtime.state.keyed.KeyedMapStateDescriptor;
import org.apache.flink.runtime.state.keyed.KeyedSortedMapStateDescriptor;
import org.apache.flink.runtime.state.keyed.KeyedStateDescriptor;
import org.apache.flink.runtime.state.keyed.KeyedValueStateDescriptor;
import org.apache.flink.runtime.state.subkeyed.SubKeyedListStateDescriptor;
import org.apache.flink.runtime.state.subkeyed.SubKeyedMapStateDescriptor;
import org.apache.flink.runtime.state.subkeyed.SubKeyedSortedMapStateDescriptor;
import org.apache.flink.runtime.state.subkeyed.SubKeyedStateDescriptor;
import org.apache.flink.runtime.state.subkeyed.SubKeyedValueStateDescriptor;
import org.apache.flink.util.Preconditions;

import java.util.Objects;

public class StateMetaInfoSnapshot {
	private final InternalStateType stateType;
	private final String name;
	private final TypeSerializer keySerializer;
	private final TypeSerializer valueSerializer;
	protected final TypeSerializer namespaceSerializer;

	private final TypeSerializerConfigSnapshot keySerializerConfigSnapshot;
	private final TypeSerializerConfigSnapshot valueSerializerConfigSnapshot;
	private final TypeSerializerConfigSnapshot namespaceSerializerConfigSnapshot;

	public StateMetaInfoSnapshot(
		InternalStateType stateType,
		String name,
		TypeSerializer keySerializer,
		TypeSerializer valueSerializer,
		TypeSerializer namespaceSerializer,
		TypeSerializerConfigSnapshot keySerializerConfigSnapshot,
		TypeSerializerConfigSnapshot valueSerializerConfigSnapshot,
		TypeSerializerConfigSnapshot namespaceSerializerConfigSnapshot) {

		this.stateType = Preconditions.checkNotNull(stateType);
		this.name = Preconditions.checkNotNull(name);
		this.keySerializer = Preconditions.checkNotNull(keySerializer);
		this.valueSerializer = Preconditions.checkNotNull(valueSerializer);
		this.namespaceSerializer = Preconditions.checkNotNull(namespaceSerializer);
		this.keySerializerConfigSnapshot = Preconditions.checkNotNull(keySerializerConfigSnapshot);
		this.valueSerializerConfigSnapshot = Preconditions.checkNotNull(valueSerializerConfigSnapshot);
		this.namespaceSerializerConfigSnapshot = Preconditions.checkNotNull(namespaceSerializerConfigSnapshot);
	}

	public InternalStateType getStateType() {
		return stateType;
	}

	public String getName() {
		return name;
	}

	public TypeSerializer getKeySerializer() {
		return keySerializer;
	}

	public TypeSerializer getValueSerializer() {
		return valueSerializer;
	}

	public TypeSerializerConfigSnapshot getKeySerializerConfigSnapshot() {
		return keySerializerConfigSnapshot;
	}

	public TypeSerializerConfigSnapshot getValueSerializerConfigSnapshot() {
		return valueSerializerConfigSnapshot;
	}

	public TypeSerializer getNamespaceSerializer() {
		return namespaceSerializer;
	}

	public TypeSerializerConfigSnapshot getNamespaceSerializerConfigSnapshot() {
		return namespaceSerializerConfigSnapshot;
	}

	@Override
	public int hashCode() {
		int result = getName().hashCode();
		result = 31 * result + getStateType().hashCode();
		result = 31 * result + getKeySerializer().hashCode();
		result = 31 * result + getValueSerializer().hashCode();
		result = 31 * result + getNamespaceSerializer().hashCode();
		result = 31 * result + getKeySerializerConfigSnapshot().hashCode();
		result = 31 * result + getValueSerializerConfigSnapshot().hashCode();
		result = 31 * result + getNamespaceSerializerConfigSnapshot().hashCode();
		return result;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		StateMetaInfoSnapshot that = (StateMetaInfoSnapshot) o;

		if (!stateType.equals(that.stateType)) {
			return false;
		}

		if (!getName().equals(that.getName())) {
			return false;
		}

		return getKeySerializer().equals(that.getKeySerializer())
			&& getValueSerializer().equals(that.getValueSerializer())
			&& Objects.equals(getNamespaceSerializer(), that.getNamespaceSerializer())
			&& getKeySerializerConfigSnapshot().equals(that.getKeySerializerConfigSnapshot())
			&& getValueSerializerConfigSnapshot().equals(that.getValueSerializerConfigSnapshot())
			&& Objects.equals(getNamespaceSerializerConfigSnapshot(), that.getNamespaceSerializerConfigSnapshot());
	}

	public KeyedStateDescriptor createKeyedStateDescriptor() {
		Preconditions.checkState(this.stateType.isKeyedState(), "Expected keyed state meta snapshot.");

		String name = this.getName();
		TypeSerializer keySerializer = this.getKeySerializer();
		TypeSerializer valueSerializer = this.getValueSerializer();
		switch (this.getStateType()) {
			case KEYED_VALUE:
				return new KeyedValueStateDescriptor(name, keySerializer, valueSerializer);
			case KEYED_LIST:
				return new KeyedListStateDescriptor(name, keySerializer, (ListSerializer) valueSerializer);
			case KEYED_MAP:
				return new KeyedMapStateDescriptor(name, keySerializer, (MapSerializer) valueSerializer);
			case KEYED_SORTEDMAP:
				return new KeyedSortedMapStateDescriptor(name, keySerializer, (SortedMapSerializer) valueSerializer);
		}
		throw new IllegalStateException("Unknown internal state type for " + this.getStateType());
	}

	public SubKeyedStateDescriptor createSubKeyedStateDescriptor() {
		Preconditions.checkState(!this.stateType.isKeyedState(),
			"Expected subKeyed state meta snapshot.");

		String name = this.getName();
		TypeSerializer keySerializer = this.getKeySerializer();
		TypeSerializer valueSerializer = this.getValueSerializer();
		TypeSerializer namespaceSerializer = this.getNamespaceSerializer();
		switch (this.getStateType()) {
			case SUBKEYED_VALUE:
				return new SubKeyedValueStateDescriptor(name, keySerializer, namespaceSerializer, valueSerializer);
			case SUBKEYED_LIST:
				TypeSerializer elementSerializer = ((ListSerializer) valueSerializer).getElementSerializer();
				return new SubKeyedListStateDescriptor(name, keySerializer, namespaceSerializer, elementSerializer);
			case SUBKEYED_MAP:
				return new SubKeyedMapStateDescriptor(name, keySerializer, namespaceSerializer, (MapSerializer) valueSerializer);
			case SUBKEYED_SORTEDMAP:
				return new SubKeyedSortedMapStateDescriptor(name, keySerializer, namespaceSerializer, (SortedMapSerializer) valueSerializer);
		}
		throw new IllegalStateException("Unknown internal state type for " + this.getStateType());
	}
}
