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
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class RegisteredBroadcastBackendStateMetaInfo<K, V> extends RegisteredStateMetaInfoBase {

	/** The mode how elements in this state are assigned to tasks during restore. */
	private final OperatorStateHandle.Mode assignmentMode;

	/** The type serializer for the keys in the map state. */
	private final TypeSerializer<K> keySerializer;

	/** The type serializer for the values in the map state. */
	private final TypeSerializer<V> valueSerializer;

	public RegisteredBroadcastBackendStateMetaInfo(
			final String name,
			final OperatorStateHandle.Mode assignmentMode,
			final TypeSerializer<K> keySerializer,
			final TypeSerializer<V> valueSerializer) {

		super(name);
		Preconditions.checkArgument(assignmentMode != null && assignmentMode == OperatorStateHandle.Mode.BROADCAST);
		this.assignmentMode = assignmentMode;
		this.keySerializer = Preconditions.checkNotNull(keySerializer);
		this.valueSerializer = Preconditions.checkNotNull(valueSerializer);
	}

	public RegisteredBroadcastBackendStateMetaInfo(RegisteredBroadcastBackendStateMetaInfo<K, V> copy) {
		this(
			Preconditions.checkNotNull(copy).name,
			copy.assignmentMode,
			copy.keySerializer.duplicate(),
			copy.valueSerializer.duplicate());
	}

	@SuppressWarnings("unchecked")
	public RegisteredBroadcastBackendStateMetaInfo(StateMetaInfoSnapshot snapshot) {
		this(
			snapshot.getName(),
			OperatorStateHandle.Mode.valueOf(
				snapshot.getOption(StateMetaInfoSnapshot.CommonOptionsKeys.OPERATOR_STATE_DISTRIBUTION_MODE)),
			(TypeSerializer<K>) snapshot.getTypeSerializer(StateMetaInfoSnapshot.CommonSerializerKeys.KEY_SERIALIZER),
			(TypeSerializer<V>) snapshot.getTypeSerializer(StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER));
	}

	/**
	 * Creates a deep copy of the itself.
	 */
	public RegisteredBroadcastBackendStateMetaInfo<K, V> deepCopy() {
		return new RegisteredBroadcastBackendStateMetaInfo<>(this);
	}

	@Nonnull
	@Override
	public StateMetaInfoSnapshot snapshot() {
		Map<String, String> optionsMap = Collections.singletonMap(
			StateMetaInfoSnapshot.CommonOptionsKeys.OPERATOR_STATE_DISTRIBUTION_MODE.toString(),
			assignmentMode.toString());
		Map<String, TypeSerializer<?>> serializerMap = new HashMap<>(2);
		Map<String, TypeSerializerConfigSnapshot> serializerConfigSnapshotsMap = new HashMap<>(2);
		String keySerializerKey = StateMetaInfoSnapshot.CommonSerializerKeys.KEY_SERIALIZER.toString();
		String valueSerializerKey = StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER.toString();
		serializerMap.put(keySerializerKey, keySerializer.duplicate());
		serializerConfigSnapshotsMap.put(keySerializerKey, keySerializer.snapshotConfiguration());
		serializerMap.put(valueSerializerKey, valueSerializer.duplicate());
		serializerConfigSnapshotsMap.put(valueSerializerKey, valueSerializer.snapshotConfiguration());

		return new StateMetaInfoSnapshot(name, optionsMap, serializerConfigSnapshotsMap, serializerMap);
	}

	public TypeSerializer<K> getKeySerializer() {
		return keySerializer;
	}

	public TypeSerializer<V> getValueSerializer() {
		return valueSerializer;
	}

	public OperatorStateHandle.Mode getAssignmentMode() {
		return assignmentMode;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}

		if (!(obj instanceof RegisteredBroadcastBackendStateMetaInfo)) {
			return false;
		}

		final RegisteredBroadcastBackendStateMetaInfo other =
				(RegisteredBroadcastBackendStateMetaInfo) obj;

		return Objects.equals(name, other.getName())
				&& Objects.equals(assignmentMode, other.getAssignmentMode())
				&& Objects.equals(keySerializer, other.getKeySerializer())
				&& Objects.equals(valueSerializer, other.getValueSerializer());
	}

	@Override
	public int hashCode() {
		int result = name.hashCode();
		result = 31 * result + assignmentMode.hashCode();
		result = 31 * result + keySerializer.hashCode();
		result = 31 * result + valueSerializer.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "RegisteredBroadcastBackendStateMetaInfo{" +
				"name='" + name + '\'' +
				", keySerializer=" + keySerializer +
				", valueSerializer=" + valueSerializer +
				", assignmentMode=" + assignmentMode +
				'}';
	}
}
