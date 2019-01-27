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
import org.apache.flink.util.Preconditions;

import java.util.Objects;

public class RegisteredBroadcastBackendStateMetaInfo<K, V> {

	/** The name of the state, as registered by the user. */
	private final String name;

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

		Preconditions.checkArgument(assignmentMode != null && assignmentMode == OperatorStateHandle.Mode.BROADCAST);

		this.name = Preconditions.checkNotNull(name);
		this.assignmentMode = assignmentMode;
		this.keySerializer = Preconditions.checkNotNull(keySerializer);
		this.valueSerializer = Preconditions.checkNotNull(valueSerializer);
	}

	public RegisteredBroadcastBackendStateMetaInfo(RegisteredBroadcastBackendStateMetaInfo<K, V> copy) {

		Preconditions.checkNotNull(copy);

		this.name = copy.name;
		this.assignmentMode = copy.assignmentMode;
		this.keySerializer = copy.keySerializer.duplicate();
		this.valueSerializer = copy.valueSerializer.duplicate();
	}

	/**
	 * Creates a deep copy of the itself.
	 */
	public RegisteredBroadcastBackendStateMetaInfo<K, V> deepCopy() {
		return new RegisteredBroadcastBackendStateMetaInfo<>(this);
	}

	public String getName() {
		return name;
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

	public RegisteredBroadcastBackendStateMetaInfo.Snapshot<K, V> snapshot() {
		return new RegisteredBroadcastBackendStateMetaInfo.Snapshot<>(
				name,
				assignmentMode,
				keySerializer.duplicate(),
				valueSerializer.duplicate(),
				keySerializer.snapshotConfiguration(),
				valueSerializer.snapshotConfiguration());
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

	/**
	 * A consistent snapshot of a {@link RegisteredOperatorBackendStateMetaInfo}.
	 */
	public static class Snapshot<K, V> {

		private String name;
		private OperatorStateHandle.Mode assignmentMode;
		private TypeSerializer<K> keySerializer;
		private TypeSerializer<V> valueSerializer;
		private TypeSerializerConfigSnapshot keySerializerConfigSnapshot;
		private TypeSerializerConfigSnapshot valueSerializerConfigSnapshot;

		/** Empty constructor used when restoring the state meta info snapshot. */
		Snapshot() {}

		private Snapshot(
				final String name,
				final OperatorStateHandle.Mode assignmentMode,
				final TypeSerializer<K> keySerializer,
				final TypeSerializer<V> valueSerializer,
				final TypeSerializerConfigSnapshot keySerializerConfigSnapshot,
				final TypeSerializerConfigSnapshot valueSerializerConfigSnapshot) {

			this.name = Preconditions.checkNotNull(name);
			this.assignmentMode = Preconditions.checkNotNull(assignmentMode);
			this.keySerializer = Preconditions.checkNotNull(keySerializer);
			this.valueSerializer = Preconditions.checkNotNull(valueSerializer);
			this.keySerializerConfigSnapshot = Preconditions.checkNotNull(keySerializerConfigSnapshot);
			this.valueSerializerConfigSnapshot = Preconditions.checkNotNull(valueSerializerConfigSnapshot);
		}

		public String getName() {
			return name;
		}

		void setName(String name) {
			this.name = name;
		}

		public OperatorStateHandle.Mode getAssignmentMode() {
			return assignmentMode;
		}

		void setAssignmentMode(OperatorStateHandle.Mode mode) {
			this.assignmentMode = mode;
		}

		public TypeSerializer<K> getKeySerializer() {
			return keySerializer;
		}

		void setKeySerializer(TypeSerializer<K> serializer) {
			this.keySerializer = serializer;
		}

		public TypeSerializer<V> getValueSerializer() {
			return valueSerializer;
		}

		void setValueSerializer(TypeSerializer<V> serializer) {
			this.valueSerializer = serializer;
		}

		public TypeSerializerConfigSnapshot getKeySerializerConfigSnapshot() {
			return keySerializerConfigSnapshot;
		}

		void setKeySerializerConfigSnapshot(TypeSerializerConfigSnapshot configSnapshot) {
			this.keySerializerConfigSnapshot = configSnapshot;
		}

		public TypeSerializerConfigSnapshot getValueSerializerConfigSnapshot() {
			return valueSerializerConfigSnapshot;
		}

		void setValueSerializerConfigSnapshot(TypeSerializerConfigSnapshot configSnapshot) {
			this.valueSerializerConfigSnapshot = configSnapshot;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == this) {
				return true;
			}

			if (!(obj instanceof RegisteredBroadcastBackendStateMetaInfo.Snapshot)) {
				return false;
			}

			RegisteredBroadcastBackendStateMetaInfo.Snapshot snapshot =
					(RegisteredBroadcastBackendStateMetaInfo.Snapshot) obj;

			return name.equals(snapshot.getName())
					&& assignmentMode.ordinal() == snapshot.getAssignmentMode().ordinal()
					&& Objects.equals(keySerializer, snapshot.getKeySerializer())
					&& Objects.equals(valueSerializer, snapshot.getValueSerializer())
					&& keySerializerConfigSnapshot.equals(snapshot.getKeySerializerConfigSnapshot())
					&& valueSerializerConfigSnapshot.equals(snapshot.getValueSerializerConfigSnapshot());
		}

		@Override
		public int hashCode() {
			int result = name.hashCode();
			result = 31 * result + assignmentMode.hashCode();
			result = 31 * result + ((keySerializer != null) ? keySerializer.hashCode() : 0);
			result = 31 * result + ((valueSerializer != null) ? valueSerializer.hashCode() : 0);
			result = 31 * result + keySerializerConfigSnapshot.hashCode();
			result = 31 * result + valueSerializerConfigSnapshot.hashCode();
			return result;
		}
	}
}
