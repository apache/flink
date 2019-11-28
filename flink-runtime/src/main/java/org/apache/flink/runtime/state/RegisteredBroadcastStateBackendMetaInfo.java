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
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class RegisteredBroadcastStateBackendMetaInfo<K, V> extends RegisteredStateMetaInfoBase {

	/** The mode how elements in this state are assigned to tasks during restore. */
	@Nonnull
	private final OperatorStateHandle.Mode assignmentMode;

	/** The type serializer for the keys in the map state. */
	@Nonnull
	private final StateSerializerProvider<K> keySerializerProvider;

	/** The type serializer for the values in the map state. */
	@Nonnull
	private final StateSerializerProvider<V> valueSerializerProvider;

	public RegisteredBroadcastStateBackendMetaInfo(
			@Nonnull final String name,
			@Nonnull final OperatorStateHandle.Mode assignmentMode,
			@Nonnull final TypeSerializer<K> keySerializer,
			@Nonnull final TypeSerializer<V> valueSerializer) {

		this(
			name,
			assignmentMode,
			StateSerializerProvider.fromNewRegisteredSerializer(keySerializer),
			StateSerializerProvider.fromNewRegisteredSerializer(valueSerializer));
	}

	public RegisteredBroadcastStateBackendMetaInfo(@Nonnull RegisteredBroadcastStateBackendMetaInfo<K, V> copy) {
		this(
			Preconditions.checkNotNull(copy).name,
			copy.assignmentMode,
			copy.getKeySerializer().duplicate(),
			copy.getValueSerializer().duplicate());
	}

	@SuppressWarnings("unchecked")
	public RegisteredBroadcastStateBackendMetaInfo(@Nonnull StateMetaInfoSnapshot snapshot) {
		this(
			snapshot.getName(),
			OperatorStateHandle.Mode.valueOf(
				snapshot.getOption(StateMetaInfoSnapshot.CommonOptionsKeys.OPERATOR_STATE_DISTRIBUTION_MODE)),
			StateSerializerProvider.fromPreviousSerializerSnapshot(
				(TypeSerializerSnapshot<K>) Preconditions.checkNotNull(
					snapshot.getTypeSerializerSnapshot(StateMetaInfoSnapshot.CommonSerializerKeys.KEY_SERIALIZER))),
			StateSerializerProvider.fromPreviousSerializerSnapshot(
				(TypeSerializerSnapshot<V>) Preconditions.checkNotNull(
					snapshot.getTypeSerializerSnapshot(StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER))));

		Preconditions.checkState(StateMetaInfoSnapshot.BackendStateType.BROADCAST == snapshot.getBackendStateType());
	}

	/**
	 * Creates a deep copy of the itself.
	 */
	@Nonnull
	public RegisteredBroadcastStateBackendMetaInfo<K, V> deepCopy() {
		return new RegisteredBroadcastStateBackendMetaInfo<>(this);
	}

	private RegisteredBroadcastStateBackendMetaInfo(
		@Nonnull final String name,
		@Nonnull final OperatorStateHandle.Mode assignmentMode,
		@Nonnull final StateSerializerProvider<K> keySerializerProvider,
		@Nonnull final StateSerializerProvider<V> valueSerializerProvider) {

		super(name);
		Preconditions.checkArgument(assignmentMode == OperatorStateHandle.Mode.BROADCAST);
		this.assignmentMode = assignmentMode;
		this.keySerializerProvider = keySerializerProvider;
		this.valueSerializerProvider = valueSerializerProvider;
	}

	@Nonnull
	@Override
	public StateMetaInfoSnapshot snapshot() {
		return computeSnapshot();
	}

	@Nonnull
	public TypeSerializer<K> getKeySerializer() {
		return keySerializerProvider.currentSchemaSerializer();
	}

	@Nonnull
	public TypeSerializerSchemaCompatibility<K> updateKeySerializer(TypeSerializer<K> newKeySerializer) {
		return keySerializerProvider.registerNewSerializerForRestoredState(newKeySerializer);
	}

	@Nullable
	public TypeSerializer<K> getPreviousKeySerializer() {
		return keySerializerProvider.previousSchemaSerializer();
	}

	@Nonnull
	public TypeSerializer<V> getValueSerializer() {
		return valueSerializerProvider.currentSchemaSerializer();
	}

	@Nonnull
	public TypeSerializerSchemaCompatibility<V> updateValueSerializer(TypeSerializer<V> newValueSerializer) {
		return valueSerializerProvider.registerNewSerializerForRestoredState(newValueSerializer);
	}

	@Nullable
	public TypeSerializer<V> getPreviousValueSerializer() {
		return valueSerializerProvider.previousSchemaSerializer();
	}

	@Nonnull
	public OperatorStateHandle.Mode getAssignmentMode() {
		return assignmentMode;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}

		if (!(obj instanceof RegisteredBroadcastStateBackendMetaInfo)) {
			return false;
		}

		final RegisteredBroadcastStateBackendMetaInfo other =
				(RegisteredBroadcastStateBackendMetaInfo) obj;

		return Objects.equals(name, other.getName())
				&& Objects.equals(assignmentMode, other.getAssignmentMode())
				&& Objects.equals(getKeySerializer(), other.getKeySerializer())
				&& Objects.equals(getValueSerializer(), other.getValueSerializer());
	}

	@Override
	public int hashCode() {
		int result = name.hashCode();
		result = 31 * result + assignmentMode.hashCode();
		result = 31 * result + getKeySerializer().hashCode();
		result = 31 * result + getValueSerializer().hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "RegisteredBroadcastBackendStateMetaInfo{" +
				"name='" + name + '\'' +
				", keySerializer=" + getKeySerializer() +
				", valueSerializer=" + getValueSerializer() +
				", assignmentMode=" + assignmentMode +
				'}';
	}

	@Nonnull
	private StateMetaInfoSnapshot computeSnapshot() {
		Map<String, String> optionsMap = Collections.singletonMap(
			StateMetaInfoSnapshot.CommonOptionsKeys.OPERATOR_STATE_DISTRIBUTION_MODE.toString(),
			assignmentMode.toString());
		Map<String, TypeSerializer<?>> serializerMap = new HashMap<>(2);
		Map<String, TypeSerializerSnapshot<?>> serializerConfigSnapshotsMap = new HashMap<>(2);
		String keySerializerKey = StateMetaInfoSnapshot.CommonSerializerKeys.KEY_SERIALIZER.toString();
		String valueSerializerKey = StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER.toString();

		TypeSerializer<K> keySerializer = getKeySerializer();
		serializerMap.put(keySerializerKey, keySerializer.duplicate());
		serializerConfigSnapshotsMap.put(keySerializerKey, keySerializer.snapshotConfiguration());

		TypeSerializer<V> valueSerializer = getValueSerializer();
		serializerMap.put(valueSerializerKey, valueSerializer.duplicate());
		serializerConfigSnapshotsMap.put(valueSerializerKey, valueSerializer.snapshotConfiguration());

		return new StateMetaInfoSnapshot(
			name,
			StateMetaInfoSnapshot.BackendStateType.BROADCAST,
			optionsMap,
			serializerConfigSnapshotsMap,
			serializerMap);
	}
}
