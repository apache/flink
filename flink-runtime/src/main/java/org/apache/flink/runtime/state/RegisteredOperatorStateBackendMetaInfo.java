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
import java.util.Map;

/**
 * Compound meta information for a registered state in an operator state backend.
 * This contains the state name, assignment mode, and state partition serializer.
 *
 * @param <S> Type of the state.
 */
public class RegisteredOperatorStateBackendMetaInfo<S> extends RegisteredStateMetaInfoBase {

	/**
	 * The mode how elements in this state are assigned to tasks during restore
	 */
	@Nonnull
	private final OperatorStateHandle.Mode assignmentMode;

	/**
	 * The type serializer for the elements in the state list
	 */
	@Nonnull
	private final StateSerializerProvider<S> partitionStateSerializerProvider;

	public RegisteredOperatorStateBackendMetaInfo(
			@Nonnull String name,
			@Nonnull TypeSerializer<S> partitionStateSerializer,
			@Nonnull OperatorStateHandle.Mode assignmentMode) {
		this(
			name,
			StateSerializerProvider.fromNewState(partitionStateSerializer),
			assignmentMode);
	}

	private RegisteredOperatorStateBackendMetaInfo(@Nonnull RegisteredOperatorStateBackendMetaInfo<S> copy) {
		this(
			Preconditions.checkNotNull(copy).name,
			copy.getPartitionStateSerializer().duplicate(),
			copy.assignmentMode);
	}

	@SuppressWarnings("unchecked")
	public RegisteredOperatorStateBackendMetaInfo(@Nonnull StateMetaInfoSnapshot snapshot) {
		this(
			snapshot.getName(),
			StateSerializerProvider.fromRestoredState(
				(TypeSerializerSnapshot<S>) Preconditions.checkNotNull(
					snapshot.getTypeSerializerSnapshot(StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER))),
			OperatorStateHandle.Mode.valueOf(
				snapshot.getOption(StateMetaInfoSnapshot.CommonOptionsKeys.OPERATOR_STATE_DISTRIBUTION_MODE)));

		Preconditions.checkState(StateMetaInfoSnapshot.BackendStateType.OPERATOR == snapshot.getBackendStateType());
	}

	private RegisteredOperatorStateBackendMetaInfo(
			@Nonnull String name,
			@Nonnull StateSerializerProvider<S> partitionStateSerializerProvider,
			@Nonnull OperatorStateHandle.Mode assignmentMode) {
		super(name);
		this.partitionStateSerializerProvider = partitionStateSerializerProvider;
		this.assignmentMode = assignmentMode;
	}

	/**
	 * Creates a deep copy of the itself.
	 */
	@Nonnull
	public RegisteredOperatorStateBackendMetaInfo<S> deepCopy() {
		return new RegisteredOperatorStateBackendMetaInfo<>(this);
	}

	@Nonnull
	@Override
	public StateMetaInfoSnapshot snapshot() {
		return computeSnapshot();
	}

	@Nonnull
	public OperatorStateHandle.Mode getAssignmentMode() {
		return assignmentMode;
	}

	@Nonnull
	public TypeSerializer<S> getPartitionStateSerializer() {
		return partitionStateSerializerProvider.currentSchemaSerializer();
	}

	@Nonnull
	public TypeSerializerSchemaCompatibility<S> updatePartitionStateSerializer(TypeSerializer<S> newPartitionStateSerializer) {
		return partitionStateSerializerProvider.registerNewSerializerForRestoredState(newPartitionStateSerializer);
	}

	@Nullable
	public TypeSerializer<S> getPreviousPartitionStateSerializer() {
		return partitionStateSerializerProvider.previousSchemaSerializer();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}

		if (obj == null) {
			return false;
		}

		return (obj instanceof RegisteredOperatorStateBackendMetaInfo)
			&& name.equals(((RegisteredOperatorStateBackendMetaInfo) obj).getName())
			&& assignmentMode.equals(((RegisteredOperatorStateBackendMetaInfo) obj).getAssignmentMode())
			&& getPartitionStateSerializer().equals(((RegisteredOperatorStateBackendMetaInfo) obj).getPartitionStateSerializer());
	}

	@Override
	public int hashCode() {
		int result = getName().hashCode();
		result = 31 * result + getAssignmentMode().hashCode();
		result = 31 * result + getPartitionStateSerializer().hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "RegisteredOperatorBackendStateMetaInfo{" +
			"name='" + name + "\'" +
			", assignmentMode=" + assignmentMode +
			", partitionStateSerializer=" + getPartitionStateSerializer() +
			'}';
	}

	@Nonnull
	private StateMetaInfoSnapshot computeSnapshot() {
		Map<String, String> optionsMap = Collections.singletonMap(
			StateMetaInfoSnapshot.CommonOptionsKeys.OPERATOR_STATE_DISTRIBUTION_MODE.toString(),
			assignmentMode.toString());
		String valueSerializerKey = StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER.toString();

		TypeSerializer<S> partitionStateSerializer = getPartitionStateSerializer();
		Map<String, TypeSerializer<?>> serializerMap =
			Collections.singletonMap(valueSerializerKey, partitionStateSerializer.duplicate());
		Map<String, TypeSerializerSnapshot<?>> serializerConfigSnapshotsMap =
			Collections.singletonMap(valueSerializerKey, partitionStateSerializer.snapshotConfiguration());

		return new StateMetaInfoSnapshot(
			name,
			StateMetaInfoSnapshot.BackendStateType.OPERATOR,
			optionsMap,
			serializerConfigSnapshotsMap,
			serializerMap);
	}
}
