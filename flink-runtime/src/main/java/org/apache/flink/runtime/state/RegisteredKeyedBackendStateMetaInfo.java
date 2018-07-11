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
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StateMigrationException;

import javax.annotation.Nonnull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Compound meta information for a registered state in a keyed state backend. This combines all serializers and the
 * state name.
 *
 * @param <N> Type of namespace
 * @param <S> Type of state value
 */
public class RegisteredKeyedBackendStateMetaInfo<N, S> extends RegisteredStateMetaInfoBase {

	private final StateDescriptor.Type stateType;
	private final TypeSerializer<N> namespaceSerializer;
	private final TypeSerializer<S> stateSerializer;

	@SuppressWarnings("unchecked")
	public RegisteredKeyedBackendStateMetaInfo(StateMetaInfoSnapshot snapshot) {
		this(
			StateDescriptor.Type.valueOf(snapshot.getOption(StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE)),
			snapshot.getName(),
			(TypeSerializer<N>) snapshot.getTypeSerializer(StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER),
			(TypeSerializer<S>) snapshot.getTypeSerializer(StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER));
	}

	public RegisteredKeyedBackendStateMetaInfo(
			StateDescriptor.Type stateType,
			String name,
			TypeSerializer<N> namespaceSerializer,
			TypeSerializer<S> stateSerializer) {

		super(name);
		this.stateType = checkNotNull(stateType);
		this.namespaceSerializer = checkNotNull(namespaceSerializer);
		this.stateSerializer = checkNotNull(stateSerializer);
	}

	public StateDescriptor.Type getStateType() {
		return stateType;
	}

	public TypeSerializer<N> getNamespaceSerializer() {
		return namespaceSerializer;
	}

	public TypeSerializer<S> getStateSerializer() {
		return stateSerializer;
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
	 * Checks compatibility of a restored k/v state, with the new {@link StateDescriptor} provided to it.
	 * This checks that the descriptor specifies identical names and state types, as well as
	 * serializers that are compatible for the restored k/v state bytes.
	 */
	public static <N, S> RegisteredKeyedBackendStateMetaInfo<N, S> resolveKvStateCompatibility(
		StateMetaInfoSnapshot restoredStateMetaInfoSnapshot,
		TypeSerializer<N> newNamespaceSerializer,
		StateDescriptor<?, S> newStateDescriptor) throws StateMigrationException {

		Preconditions.checkState(
			Objects.equals(newStateDescriptor.getName(), restoredStateMetaInfoSnapshot.getName()),
			"Incompatible state names. " +
				"Was [" + restoredStateMetaInfoSnapshot.getName() + "], " +
				"registered with [" + newStateDescriptor.getName() + "].");

		final StateDescriptor.Type restoredType =
			StateDescriptor.Type.valueOf(
				restoredStateMetaInfoSnapshot.getOption(
					StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE));

		if (!Objects.equals(newStateDescriptor.getType(), StateDescriptor.Type.UNKNOWN)
			&& !Objects.equals(restoredType, StateDescriptor.Type.UNKNOWN)) {

			Preconditions.checkState(
				newStateDescriptor.getType() == restoredType,
				"Incompatible state types. " +
					"Was [" + restoredType + "], " +
					"registered with [" + newStateDescriptor.getType() + "].");
		}

		// check compatibility results to determine if state migration is required
		CompatibilityResult<N> namespaceCompatibility = CompatibilityUtil.resolveCompatibilityResult(
			restoredStateMetaInfoSnapshot.getTypeSerializer(StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER),
			null,
			restoredStateMetaInfoSnapshot.getTypeSerializerConfigSnapshot(
				StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER),
			newNamespaceSerializer);

		TypeSerializer<S> newStateSerializer = newStateDescriptor.getSerializer();
		CompatibilityResult<S> stateCompatibility = CompatibilityUtil.resolveCompatibilityResult(
			restoredStateMetaInfoSnapshot.getTypeSerializer(
				StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER),
			UnloadableDummyTypeSerializer.class,
			restoredStateMetaInfoSnapshot.getTypeSerializerConfigSnapshot(
				StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER),
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

	@Nonnull
	@Override
	public StateMetaInfoSnapshot snapshot() {
		Map<String, String> optionsMap = Collections.singletonMap(
			StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE.toString(),
			stateType.toString());
		Map<String, TypeSerializer<?>> serializerMap = new HashMap<>(2);
		Map<String, TypeSerializerConfigSnapshot> serializerConfigSnapshotsMap = new HashMap<>(2);
		String namespaceSerializerKey = StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER.toString();
		String valueSerializerKey = StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER.toString();
		serializerMap.put(namespaceSerializerKey, namespaceSerializer.duplicate());
		serializerConfigSnapshotsMap.put(namespaceSerializerKey, namespaceSerializer.snapshotConfiguration());
		serializerMap.put(valueSerializerKey, stateSerializer.duplicate());
		serializerConfigSnapshotsMap.put(valueSerializerKey, stateSerializer.snapshotConfiguration());

		return new StateMetaInfoSnapshot(name, optionsMap, serializerConfigSnapshotsMap, serializerMap);
	}
}
