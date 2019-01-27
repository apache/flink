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

import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.CompatibilityUtil;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.SortedMapSerializer;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.keyed.KeyedStateDescriptor;
import org.apache.flink.runtime.state.subkeyed.SubKeyedStateDescriptor;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StateMigrationException;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Compound meta information for a registered keyed/subKeyed State in a internal state backend.
 * This combines all serializers and the state name.
 */
public class RegisteredStateMetaInfo {

	private final InternalStateType internalStateType;
	private final String name;
	private final TypeSerializer keySerializer;
	private final TypeSerializer valueSerializer;
	private final TypeSerializer namespaceSerializer;

	private RegisteredStateMetaInfo(
		InternalStateType internalStateType,
		String name,
		TypeSerializer keySerializer,
		TypeSerializer valueSerializer,
		TypeSerializer namespaceSerializer) {

		this.internalStateType = checkNotNull(internalStateType);
		Preconditions.checkState(!internalStateType.isKeyedState(), "Expected subKeyed state.");
		this.name = checkNotNull(name);
		this.keySerializer = checkNotNull(keySerializer);
		this.valueSerializer = checkNotNull(valueSerializer);
		checkValueSerializerType(internalStateType, valueSerializer);
		this.namespaceSerializer = checkNotNull(namespaceSerializer);
	}

	private RegisteredStateMetaInfo(
		InternalStateType internalStateType,
		String name,
		TypeSerializer keySerializer,
		TypeSerializer valueSerializer) {

		this.internalStateType = checkNotNull(internalStateType);
		Preconditions.checkState(internalStateType.isKeyedState(), "Expected keyed state.");
		this.name = checkNotNull(name);
		this.keySerializer = checkNotNull(keySerializer);
		this.valueSerializer = checkNotNull(valueSerializer);
		checkValueSerializerType(internalStateType, valueSerializer);
		this.namespaceSerializer = VoidNamespaceSerializer.INSTANCE;
	}

	private void checkValueSerializerType(InternalStateType internalStateType, TypeSerializer valueSerializer) {
		switch (internalStateType) {
			case KEYED_LIST:
			case SUBKEYED_LIST:
				Preconditions.checkState(valueSerializer instanceof ListSerializer,
					"Expected ListSerializer when creating " + internalStateType + " state type.");
				break;
			case KEYED_MAP:
			case SUBKEYED_MAP:
				Preconditions.checkState(valueSerializer instanceof MapSerializer,
					"Expected MapSerializer when creating "  + internalStateType + " state type.");
				break;
			case KEYED_SORTEDMAP:
			case SUBKEYED_SORTEDMAP:
				Preconditions.checkState(valueSerializer instanceof SortedMapSerializer,
					"Expected SortedMapSerializer when creating "  + internalStateType + " state type.");
				break;
			default: // nothing to do
		}
	}

	public StateMetaInfoSnapshot snapshot() {
		return new StateMetaInfoSnapshot(
			internalStateType,
			name,
			keySerializer.duplicate(),
			valueSerializer.duplicate(),
			namespaceSerializer.duplicate(),
			keySerializer.snapshotConfiguration(),
			valueSerializer.snapshotConfiguration(),
			namespaceSerializer.snapshotConfiguration());
	}

	public InternalStateType getStateType() {
		return internalStateType;
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

	public TypeSerializer getNamespaceSerializer() {
		return namespaceSerializer;
	}

	public static RegisteredStateMetaInfo createKeyedStateMetaInfo(
		InternalStateType stateType,
		String name,
		TypeSerializer keySerializer,
		TypeSerializer valueSerializer){

		return new RegisteredStateMetaInfo(stateType, name, keySerializer, valueSerializer);
	}

	public static RegisteredStateMetaInfo createSubKeyedStateMetaInfo(
		InternalStateType stateType,
		String name,
		TypeSerializer keySerializer,
		TypeSerializer valueSerializer,
		TypeSerializer namespaceSerializer){

		return new RegisteredStateMetaInfo(stateType, name, keySerializer, valueSerializer, namespaceSerializer);
	}

	public static RegisteredStateMetaInfo createKeyedStateMetaInfo(StateMetaInfoSnapshot stateMetaInfoSnapshot){

		return new RegisteredStateMetaInfo(
			stateMetaInfoSnapshot.getStateType(),
			stateMetaInfoSnapshot.getName(),
			stateMetaInfoSnapshot.getKeySerializer(),
			stateMetaInfoSnapshot.getValueSerializer());
	}

	public static RegisteredStateMetaInfo createSubKeyedStateMetaInfo(StateMetaInfoSnapshot stateMetaInfoSnapshot){

		return new RegisteredStateMetaInfo(
			stateMetaInfoSnapshot.getStateType(),
			stateMetaInfoSnapshot.getName(),
			stateMetaInfoSnapshot.getKeySerializer(),
			stateMetaInfoSnapshot.getValueSerializer(),
			stateMetaInfoSnapshot.getNamespaceSerializer());
	}

	@Override
	public int hashCode() {
		int result = getName().hashCode();
		result = 31 * result + getStateType().hashCode();
		result = 31 * result + getStateType().hashCode();
		result = 31 * result + getStateType().hashCode();
		result = 31 * result + getKeySerializer().hashCode();
		result = 31 * result + getValueSerializer().hashCode();
		result = 31 * result + getNamespaceSerializer().hashCode();
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

		RegisteredStateMetaInfo that = (RegisteredStateMetaInfo) o;

		return getName().equals(that.getName())
			&& getStateType().equals(that.getStateType())
			&& getStateType().equals(that.getStateType())
			&& getKeySerializer().equals(that.getKeySerializer())
			&& getValueSerializer().equals(that.getValueSerializer())
			&& Objects.equals(getNamespaceSerializer(), that.getNamespaceSerializer());
	}

	public static RegisteredStateMetaInfo resolveStateCompatibility(
		StateMetaInfoSnapshot restoreStateMetaInfoSnapshot,
		SubKeyedStateDescriptor newSubKeyedStateDescriptor) throws StateMigrationException {
		Preconditions.checkState(
			Objects.equals(newSubKeyedStateDescriptor.getName(), restoreStateMetaInfoSnapshot.getName()),
			"Incompatible state names. " +
				"Was [" + restoreStateMetaInfoSnapshot.getName() + "], " +
				"registered with [" + newSubKeyedStateDescriptor.getName() + "].");

		TypeSerializer newKeySerializer = newSubKeyedStateDescriptor.getKeySerializer();
		TypeSerializer newValueSerializer = newSubKeyedStateDescriptor.getValueSerializer();
		TypeSerializer newNamespaceSerializer = newSubKeyedStateDescriptor.getNamespaceSerializer();

		checkRequireMigration(Arrays.asList(
			Tuple4.of(restoreStateMetaInfoSnapshot.getKeySerializer(),
				UnloadableDummyTypeSerializer.class,
				restoreStateMetaInfoSnapshot.getKeySerializerConfigSnapshot(),
				newKeySerializer),

			Tuple4.of(restoreStateMetaInfoSnapshot.getValueSerializer(),
				UnloadableDummyTypeSerializer.class,
				restoreStateMetaInfoSnapshot.getValueSerializerConfigSnapshot(),
				newValueSerializer),

			Tuple4.of(restoreStateMetaInfoSnapshot.getNamespaceSerializer(),
				null,
				restoreStateMetaInfoSnapshot.getNamespaceSerializerConfigSnapshot(),
				newNamespaceSerializer)));

			return new RegisteredStateMetaInfo(
				newSubKeyedStateDescriptor.getStateType(),
				newSubKeyedStateDescriptor.getName(),
				newKeySerializer,
				newValueSerializer,
				newNamespaceSerializer);
	}

	public static RegisteredStateMetaInfo resolveStateCompatibility(
		StateMetaInfoSnapshot restoreStateMetaInfoSnapshot,
		KeyedStateDescriptor newKeyedStateDescriptor) throws StateMigrationException {

		if (!VoidNamespaceSerializer.INSTANCE.equals(restoreStateMetaInfoSnapshot.getNamespaceSerializer())) {
			throw new IllegalStateException("Expected Keyed state's meta info snapshot.");
		}

		Preconditions.checkState(
			Objects.equals(newKeyedStateDescriptor.getName(), restoreStateMetaInfoSnapshot.getName()),
			"Incompatible state names. " +
				"Was [" + restoreStateMetaInfoSnapshot.getName() + "], " +
				"registered with [" + newKeyedStateDescriptor.getName() + "].");

		TypeSerializer newKeySerializer = newKeyedStateDescriptor.getKeySerializer();
		TypeSerializer newValueSerializer = newKeyedStateDescriptor.getValueSerializer();

		checkRequireMigration(Arrays.asList(
			Tuple4.of(restoreStateMetaInfoSnapshot.getKeySerializer(),
				UnloadableDummyTypeSerializer.class,
				restoreStateMetaInfoSnapshot.getKeySerializerConfigSnapshot(),
				newKeySerializer),

			Tuple4.of(restoreStateMetaInfoSnapshot.getValueSerializer(),
				UnloadableDummyTypeSerializer.class,
				restoreStateMetaInfoSnapshot.getValueSerializerConfigSnapshot(),
				newValueSerializer)));

		return new RegisteredStateMetaInfo(
			newKeyedStateDescriptor.getStateType(),
			newKeyedStateDescriptor.getName(),
			newKeySerializer,
			newValueSerializer);
	}

	private static void checkRequireMigration(
		Collection<Tuple4<TypeSerializer, Class<?>, TypeSerializerConfigSnapshot, TypeSerializer>> serializersToCheck) throws StateMigrationException {

		for (Tuple4<TypeSerializer, Class<?>, TypeSerializerConfigSnapshot, TypeSerializer> tupleInfo : serializersToCheck) {
			TypeSerializerConfigSnapshot precedingSerializerConfigSnapshot = tupleInfo.f2;
			TypeSerializer newSerializer = tupleInfo.f3;
			// check compatibility results to determine if state migration is required
			CompatibilityResult compatibilityResult = CompatibilityUtil.resolveCompatibilityResult(
				tupleInfo.f0,
				tupleInfo.f1,
				precedingSerializerConfigSnapshot,
				newSerializer);
			if (compatibilityResult.isRequiresMigration()) {
				// TODO state migration currently isn't possible.
				throw new StateMigrationException("State migration isn't supported, yet." +
					" PrecedingSerializerConfigSnapshot: " + precedingSerializerConfigSnapshot +
					" needs migration with new serializer: " + newSerializer);
			}
		}
	}
}
