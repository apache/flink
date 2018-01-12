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

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

/**
 * State handle for partitionable operator state. Besides being a {@link StreamStateHandle}, this also provides a
 * map that contains the offsets to the partitions of named states in the stream.
 */
public class OperatorStateHandle implements StreamStateHandle {

	/**
	 * The modes that determine how an {@link OperatorStateHandle} is assigned to tasks during restore.
	 */
	public enum Mode {
		SPLIT_DISTRIBUTE, // The operator state partitions in the state handle are split and distributed to one task each.
		BROADCAST // The operator state partitions are broadcast to all task.
	}

	private static final long serialVersionUID = 35876522969227335L;

	/**
	 * unique state name -> offsets for available partitions in the handle stream
	 */
	private final Map<String, StateMetaInfo> stateNameToPartitionOffsets;
	private final StreamStateHandle delegateStateHandle;

	public OperatorStateHandle(
			Map<String, StateMetaInfo> stateNameToPartitionOffsets,
			StreamStateHandle delegateStateHandle) {

		this.delegateStateHandle = Preconditions.checkNotNull(delegateStateHandle);
		this.stateNameToPartitionOffsets = Preconditions.checkNotNull(stateNameToPartitionOffsets);
	}

	public Map<String, StateMetaInfo> getStateNameToPartitionOffsets() {
		return stateNameToPartitionOffsets;
	}

	@Override
	public void discardState() throws Exception {
		delegateStateHandle.discardState();
	}

	@Override
	public long getStateSize() {
		return delegateStateHandle.getStateSize();
	}

	@Override
	public FSDataInputStream openInputStream() throws IOException {
		return delegateStateHandle.openInputStream();
	}

	public StreamStateHandle getDelegateStateHandle() {
		return delegateStateHandle;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (!(o instanceof OperatorStateHandle)) {
			return false;
		}

		OperatorStateHandle that = (OperatorStateHandle) o;

		if (stateNameToPartitionOffsets.size() != that.stateNameToPartitionOffsets.size()) {
			return false;
		}

		for (Map.Entry<String, StateMetaInfo> entry : stateNameToPartitionOffsets.entrySet()) {
			if (!entry.getValue().equals(that.stateNameToPartitionOffsets.get(entry.getKey()))) {
				return false;
			}
		}

		return delegateStateHandle.equals(that.delegateStateHandle);
	}

	@Override
	public int hashCode() {
		int result = delegateStateHandle.hashCode();
		for (Map.Entry<String, StateMetaInfo> entry : stateNameToPartitionOffsets.entrySet()) {

			int entryHash = entry.getKey().hashCode();
			if (entry.getValue() != null) {
				entryHash += entry.getValue().hashCode();
			}
			result = 31 * result + entryHash;
		}
		return result;
	}

	@Override
	public String toString() {
		return "OperatorStateHandle{" +
				"stateNameToPartitionOffsets=" + stateNameToPartitionOffsets +
				", delegateStateHandle=" + delegateStateHandle +
				'}';
	}

	public static class StateMetaInfo implements Serializable {

		private static final long serialVersionUID = 3593817615858941166L;

		private final long[] offsets;
		private final Mode distributionMode;

		public StateMetaInfo(long[] offsets, Mode distributionMode) {
			this.offsets = Preconditions.checkNotNull(offsets);
			this.distributionMode = Preconditions.checkNotNull(distributionMode);
		}

		public long[] getOffsets() {
			return offsets;
		}

		public Mode getDistributionMode() {
			return distributionMode;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			StateMetaInfo that = (StateMetaInfo) o;

			if (!Arrays.equals(getOffsets(), that.getOffsets())) {
				return false;
			}
			return getDistributionMode() == that.getDistributionMode();
		}

		@Override
		public int hashCode() {
			int result = Arrays.hashCode(getOffsets());
			result = 31 * result + getDistributionMode().hashCode();
			return result;
		}

		@Override
		public String toString() {
			return "StateMetaInfo{" +
					"offsets=" + Arrays.toString(offsets) +
					", distributionMode=" + distributionMode +
					'}';
		}
	}
}
