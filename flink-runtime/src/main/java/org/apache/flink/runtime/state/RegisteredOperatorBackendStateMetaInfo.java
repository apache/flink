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

/**
 * Compound meta information for a registered state in an operator state backend.
 * This contains the state name, assignment mode, and state partition serializer.
 *
 * @param <S> Type of the state.
 */
public class RegisteredOperatorBackendStateMetaInfo<S> {

	/**
	 * The name of the state, as registered by the user
	 */
	private final String name;

	/**
	 * The mode how elements in this state are assigned to tasks during restore
	 */
	private final OperatorStateHandle.Mode assignmentMode;

	/**
	 * The type serializer for the elements in the state list
	 */
	private final TypeSerializer<S> partitionStateSerializer;

	public RegisteredOperatorBackendStateMetaInfo(
			String name,
			TypeSerializer<S> partitionStateSerializer,
			OperatorStateHandle.Mode assignmentMode) {

		this.name = Preconditions.checkNotNull(name);
		this.partitionStateSerializer = Preconditions.checkNotNull(partitionStateSerializer);
		this.assignmentMode = Preconditions.checkNotNull(assignmentMode);
	}

	private RegisteredOperatorBackendStateMetaInfo(RegisteredOperatorBackendStateMetaInfo<S> copy) {

		Preconditions.checkNotNull(copy);

		this.name = copy.name;
		this.partitionStateSerializer = copy.partitionStateSerializer.duplicate();
		this.assignmentMode = copy.assignmentMode;
	}

	/**
	 * Creates a deep copy of the itself.
	 */
	public RegisteredOperatorBackendStateMetaInfo<S> deepCopy() {
		return new RegisteredOperatorBackendStateMetaInfo<>(this);
	}

	public String getName() {
		return name;
	}

	public OperatorStateHandle.Mode getAssignmentMode() {
		return assignmentMode;
	}

	public TypeSerializer<S> getPartitionStateSerializer() {
		return partitionStateSerializer;
	}

	public Snapshot<S> snapshot() {
		return new Snapshot<>(
			name,
			assignmentMode,
			partitionStateSerializer.duplicate(),
			partitionStateSerializer.snapshotConfiguration());
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}

		if (obj == null) {
			return false;
		}

		return (obj instanceof RegisteredOperatorBackendStateMetaInfo)
			&& name.equals(((RegisteredOperatorBackendStateMetaInfo) obj).getName())
			&& assignmentMode.equals(((RegisteredOperatorBackendStateMetaInfo) obj).getAssignmentMode())
			&& partitionStateSerializer.equals(((RegisteredOperatorBackendStateMetaInfo) obj).getPartitionStateSerializer());
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
			", partitionStateSerializer=" + partitionStateSerializer +
			'}';
	}

	/**
	 * A consistent snapshot of a {@link RegisteredOperatorBackendStateMetaInfo}.
	 */
	public static class Snapshot<S> {

		private String name;
		private OperatorStateHandle.Mode assignmentMode;
		private TypeSerializer<S> partitionStateSerializer;
		private TypeSerializerConfigSnapshot partitionStateSerializerConfigSnapshot;

		/** Empty constructor used when restoring the state meta info snapshot. */
		Snapshot() {}

		private Snapshot(
				String name,
				OperatorStateHandle.Mode assignmentMode,
				TypeSerializer<S> partitionStateSerializer,
				TypeSerializerConfigSnapshot partitionStateSerializerConfigSnapshot) {

			this.name = Preconditions.checkNotNull(name);
			this.assignmentMode = Preconditions.checkNotNull(assignmentMode);
			this.partitionStateSerializer = Preconditions.checkNotNull(partitionStateSerializer);
			this.partitionStateSerializerConfigSnapshot = Preconditions.checkNotNull(partitionStateSerializerConfigSnapshot);
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

		void setAssignmentMode(OperatorStateHandle.Mode assignmentMode) {
			this.assignmentMode = assignmentMode;
		}

		public TypeSerializer<S> getPartitionStateSerializer() {
			return partitionStateSerializer;
		}

		void setPartitionStateSerializer(TypeSerializer<S> partitionStateSerializer) {
			this.partitionStateSerializer = partitionStateSerializer;
		}

		public TypeSerializerConfigSnapshot getPartitionStateSerializerConfigSnapshot() {
			return partitionStateSerializerConfigSnapshot;
		}

		void setPartitionStateSerializerConfigSnapshot(TypeSerializerConfigSnapshot partitionStateSerializerConfigSnapshot) {
			this.partitionStateSerializerConfigSnapshot = partitionStateSerializerConfigSnapshot;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == this) {
				return true;
			}

			if (obj == null) {
				return false;
			}

			if (!(obj instanceof Snapshot)) {
				return false;
			}

			Snapshot snapshot = (Snapshot)obj;

			// need to check for nulls because serializer and config snapshots may be null on restore
			return name.equals(snapshot.getName())
				&& assignmentMode.equals(snapshot.getAssignmentMode())
				&& Objects.equals(partitionStateSerializer, snapshot.getPartitionStateSerializer())
				&& Objects.equals(partitionStateSerializerConfigSnapshot, snapshot.getPartitionStateSerializerConfigSnapshot());
		}

		@Override
		public int hashCode() {
			// need to check for nulls because serializer and config snapshots may be null on restore
			int result = getName().hashCode();
			result = 31 * result + getAssignmentMode().hashCode();
			result = 31 * result + (getPartitionStateSerializer() != null ? getPartitionStateSerializer().hashCode() : 0);
			result = 31 * result + (getPartitionStateSerializerConfigSnapshot() != null ? getPartitionStateSerializerConfigSnapshot().hashCode() : 0);
			return result;
		}
	}
}
