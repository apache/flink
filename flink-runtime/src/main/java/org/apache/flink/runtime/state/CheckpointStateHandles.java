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

import java.io.Serializable;
import java.util.List;

/**
 * Container state handles that contains all state handles from the different state types of a checkpointed state.
 * TODO This will be changed in the future if we get rid of chained state and instead connect state directly to individual operators in a chain.
 */
public class CheckpointStateHandles implements Serializable {

	private static final long serialVersionUID = 3252351989995L;

	private final ChainedStateHandle<StreamStateHandle> nonPartitionedStateHandles;

	private final ChainedStateHandle<OperatorStateHandle> partitioneableStateHandles;

	private final List<KeyGroupsStateHandle> keyGroupsStateHandle;

	public CheckpointStateHandles(
			ChainedStateHandle<StreamStateHandle> nonPartitionedStateHandles,
			ChainedStateHandle<OperatorStateHandle> partitioneableStateHandles,
			List<KeyGroupsStateHandle> keyGroupsStateHandle) {

		this.nonPartitionedStateHandles = nonPartitionedStateHandles;
		this.partitioneableStateHandles = partitioneableStateHandles;
		this.keyGroupsStateHandle = keyGroupsStateHandle;
	}

	public ChainedStateHandle<StreamStateHandle> getNonPartitionedStateHandles() {
		return nonPartitionedStateHandles;
	}

	public ChainedStateHandle<OperatorStateHandle> getPartitioneableStateHandles() {
		return partitioneableStateHandles;
	}

	public List<KeyGroupsStateHandle> getKeyGroupsStateHandle() {
		return keyGroupsStateHandle;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof CheckpointStateHandles)) {
			return false;
		}

		CheckpointStateHandles that = (CheckpointStateHandles) o;

		if (nonPartitionedStateHandles != null ?
				!nonPartitionedStateHandles.equals(that.nonPartitionedStateHandles)
				: that.nonPartitionedStateHandles != null) {
			return false;
		}

		if (partitioneableStateHandles != null ?
				!partitioneableStateHandles.equals(that.partitioneableStateHandles)
				: that.partitioneableStateHandles != null) {
			return false;
		}
		return keyGroupsStateHandle != null ?
				keyGroupsStateHandle.equals(that.keyGroupsStateHandle) : that.keyGroupsStateHandle == null;

	}

	@Override
	public int hashCode() {
		int result = nonPartitionedStateHandles != null ? nonPartitionedStateHandles.hashCode() : 0;
		result = 31 * result + (partitioneableStateHandles != null ? partitioneableStateHandles.hashCode() : 0);
		result = 31 * result + (keyGroupsStateHandle != null ? keyGroupsStateHandle.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return "CheckpointStateHandles{" +
				"nonPartitionedStateHandles=" + nonPartitionedStateHandles +
				", partitioneableStateHandles=" + partitioneableStateHandles +
				", keyGroupsStateHandle=" + keyGroupsStateHandle +
				'}';
	}
}
