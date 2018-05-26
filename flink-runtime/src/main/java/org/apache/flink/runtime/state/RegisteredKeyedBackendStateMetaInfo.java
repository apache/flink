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
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.CompatibilityUtil;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StateMigrationException;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Compound meta information for a registered state in a keyed state backend. This combines all serializers and the
 * state name.
 *
 * @param <N> Type of namespace
 * @param <S> Type of state value
 */
public class RegisteredKeyedBackendStateMetaInfo<N, S> {

	private final StateDescriptor.Type stateType;
	private final String name;
	private final TypeSerializer<N> namespaceSerializer;
	private final TypeSerializer<S> stateSerializer;

	public RegisteredKeyedBackendStateMetaInfo(
			StateDescriptor.Type stateType,
			String name,
			TypeSerializer<N> namespaceSerializer,
			TypeSerializer<S> stateSerializer) {

		this.stateType = checkNotNull(stateType);
		this.name = checkNotNull(name);
		this.namespaceSerializer = checkNotNull(namespaceSerializer);
		this.stateSerializer = checkNotNull(stateSerializer);
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

	public Snapshot<N, S> snapshot() {
		return new Snapshot<>(
			stateType,
			name,
			namespaceSerializer.duplicate(),
			stateSerializer.duplicate(),
			namespaceSerializer.snapshotConfiguration(),
			stateSerializer.snapshotConfiguration());
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		RegisteredKeyedBackendStateMetaInfo<?, ?> that = (RegisteredKeyedBackendStateMetaInfo<?, ?>) o;

		if (!stateType.equals(that.stateType)) {
			return false;
		}

		if (!getName().equals(that.getName())) {
			return false;
		}

		return getStateSerializer().equals(that.getStateSerializer())
				&& getNamespaceSerializer().equals(that.getNamespaceSerializer());
	}

	@Override
	public String toString() {
		return "RegisteredKeyedBackendStateMetaInfo{" +
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
		result = 31 * result + getNamespaceSerializer().hashCode();
		result = 31 * result + getStateSerializer().hashCode();
		return result;
	}

	/**
	 * A consistent snapshot of a {@link RegisteredKeyedBackendStateMetaInfo}.
	 */
	public static class Snapshot<N, S> {

		private StateDescriptor.Type stateType;
		private String name;
		private TypeSerializer<N> namespaceSerializer;
		private TypeSerializer<S> stateSerializer;
		private TypeSerializerConfigSnapshot namespaceSerializerConfigSnapshot;
		private TypeSerializerConfigSnapshot stateSerializerConfigSnapshot;

		/** Empty constructor used when restoring the state meta info snapshot. */
		Snapshot() {}

		private Snapshot(
				StateDescriptor.Type stateType,
				String name,
				TypeSerializer<N> namespaceSerializer,
				TypeSerializer<S> stateSerializer,
				TypeSerializerConfigSnapshot namespaceSerializerConfigSnapshot,
				TypeSerializerConfigSnapshot stateSerializerConfigSnapshot) {

			this.stateType = Preconditions.checkNotNull(stateType);
			this.name = Preconditions.checkNotNull(name);
			this.namespaceSerializer = Preconditions.checkNotNull(namespaceSerializer);
			this.stateSerializer = Preconditions.checkNotNull(stateSerializer);
			this.namespaceSerializerConfigSnapshot = Preconditions.checkNotNull(namespaceSerializerConfigSnapshot);
			this.stateSerializerConfigSnapshot = Preconditions.checkNotNull(stateSerializerConfigSnapshot);
		}

		public StateDescriptor.Type getStateType() {
			return stateType;
		}

		void setStateType(StateDescriptor.Type stateType) {
			this.stateType = stateType;
		}

		public String getName() {
			return name;
		}

		void setName(String name) {
			this.name = name;
		}

		public TypeSerializer<N> getNamespaceSerializer() {
			return namespaceSerializer;
		}

		void setNamespaceSerializer(TypeSerializer<N> namespaceSerializer) {
			this.namespaceSerializer = namespaceSerializer;
		}

		public TypeSerializer<S> getStateSerializer() {
			return stateSerializer;
		}

		void setStateSerializer(TypeSerializer<S> stateSerializer) {
			this.stateSerializer = stateSerializer;
		}

		public TypeSerializerConfigSnapshot getNamespaceSerializerConfigSnapshot() {
			return namespaceSerializerConfigSnapshot;
		}

		void setNamespaceSerializerConfigSnapshot(TypeSerializerConfigSnapshot namespaceSerializerConfigSnapshot) {
			this.namespaceSerializerConfigSnapshot = namespaceSerializerConfigSnapshot;
		}

		public TypeSerializerConfigSnapshot getStateSerializerConfigSnapshot() {
			return stateSerializerConfigSnapshot;
		}

		void setStateSerializerConfigSnapshot(TypeSerializerConfigSnapshot stateSerializerConfigSnapshot) {
			this.stateSerializerConfigSnapshot = stateSerializerConfigSnapshot;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}

			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			Snapshot<?, ?> that = (Snapshot<?, ?>) o;

			if (!stateType.equals(that.stateType)) {
				return false;
			}

			if (!getName().equals(that.getName())) {
				return false;
			}

			// need to check for nulls because serializer and config snapshots may be null on restore
			return Objects.equals(getStateSerializer(), that.getStateSerializer())
				&& Objects.equals(getNamespaceSerializer(), that.getNamespaceSerializer())
				&& Objects.equals(getNamespaceSerializerConfigSnapshot(), that.getNamespaceSerializerConfigSnapshot())
				&& Objects.equals(getStateSerializerConfigSnapshot(), that.getStateSerializerConfigSnapshot());
		}

		@Override
		public int hashCode() {
			// need to check for nulls because serializer and config snapshots may be null on restore
			int result = getName().hashCode();
			result = 31 * result + getStateType().hashCode();
			result = 31 * result + (getNamespaceSerializer() != null ? getNamespaceSerializer().hashCode() : 0);
			result = 31 * result + (getStateSerializer() != null ? getStateSerializer().hashCode() : 0);
			result = 31 * result + (getNamespaceSerializerConfigSnapshot() != null ? getNamespaceSerializerConfigSnapshot().hashCode() : 0);
			result = 31 * result + (getStateSerializerConfigSnapshot() != null ? getStateSerializerConfigSnapshot().hashCode() : 0);
			return result;
		}
	}

	/**
	 * Checks compatibility of a restored k/v state, with the new {@link StateDescriptor} provided to it.
	 * This checks that the descriptor specifies identical names and state types, as well as
	 * serializers that are compatible for the restored k/v state bytes.
	 */
	public static  <N, S> RegisteredKeyedBackendStateMetaInfo<N, S> resolveKvStateCompatibility(
		RegisteredKeyedBackendStateMetaInfo.Snapshot<N, S> restoredStateMetaInfoSnapshot,
		TypeSerializer<N> newNamespaceSerializer,
		StateDescriptor<?, S> newStateDescriptor) throws StateMigrationException {

		Preconditions.checkState(
			Objects.equals(newStateDescriptor.getName(), restoredStateMetaInfoSnapshot.getName()),
			"Incompatible state names. " +
				"Was [" + restoredStateMetaInfoSnapshot.getName() + "], " +
				"registered with [" + newStateDescriptor.getName() + "].");

		if (!Objects.equals(newStateDescriptor.getType(), StateDescriptor.Type.UNKNOWN)
			&& !Objects.equals(restoredStateMetaInfoSnapshot.getStateType(), StateDescriptor.Type.UNKNOWN)) {

			Preconditions.checkState(
				newStateDescriptor.getType() == restoredStateMetaInfoSnapshot.getStateType(),
				"Incompatible state types. " +
					"Was [" + restoredStateMetaInfoSnapshot.getStateType() + "], " +
					"registered with [" + newStateDescriptor.getType() + "].");
		}

		// check compatibility results to determine if state migration is required
		CompatibilityResult<N> namespaceCompatibility = CompatibilityUtil.resolveCompatibilityResult(
			restoredStateMetaInfoSnapshot.getNamespaceSerializer(),
			null,
			restoredStateMetaInfoSnapshot.getNamespaceSerializerConfigSnapshot(),
			newNamespaceSerializer);

		TypeSerializer<S> newStateSerializer = newStateDescriptor.getSerializer();
		CompatibilityResult<S> stateCompatibility = CompatibilityUtil.resolveCompatibilityResult(
			restoredStateMetaInfoSnapshot.getStateSerializer(),
			UnloadableDummyTypeSerializer.class,
			restoredStateMetaInfoSnapshot.getStateSerializerConfigSnapshot(),
			newStateSerializer);

		if (namespaceCompatibility.isRequiresMigration() || stateCompatibility.isRequiresMigration()) {
			// TODO state migration currently isn't possible.
			throw new StateMigrationException("State migration isn't supported, yet.");
		} else {
			return new RegisteredKeyedBackendStateMetaInfo<>(
				newStateDescriptor.getType(),
				newStateDescriptor.getName(),
				newNamespaceSerializer,
				newStateSerializer);
		}
	}
}
