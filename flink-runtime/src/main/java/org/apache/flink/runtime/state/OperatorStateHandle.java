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
import java.util.Arrays;
import java.util.Map;

/**
 * State handle for partitionable operator state. Besides being a {@link StreamStateHandle}, this also provides a
 * map that contains the offsets to the partitions of named states in the stream.
 */
public class OperatorStateHandle implements StreamStateHandle {

	private static final long serialVersionUID = 35876522969227335L;

	/** unique state name -> offsets for available partitions in the handle stream */
	private final Map<String, long[]> stateNameToPartitionOffsets;
	private final StreamStateHandle delegateStateHandle;

	public OperatorStateHandle(
			Map<String, long[]> stateNameToPartitionOffsets,
			StreamStateHandle delegateStateHandle) {

		this.delegateStateHandle = Preconditions.checkNotNull(delegateStateHandle);
		this.stateNameToPartitionOffsets = Preconditions.checkNotNull(stateNameToPartitionOffsets);
	}

	public Map<String, long[]> getStateNameToPartitionOffsets() {
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

		if(stateNameToPartitionOffsets.size() != that.stateNameToPartitionOffsets.size()) {
			return false;
		}

		for (Map.Entry<String, long[]> entry : stateNameToPartitionOffsets.entrySet()) {
			if (!Arrays.equals(entry.getValue(), that.stateNameToPartitionOffsets.get(entry.getKey()))) {
				return false;
			}
		}

		return delegateStateHandle.equals(that.delegateStateHandle);
	}

	@Override
	public int hashCode() {
		int result = delegateStateHandle.hashCode();
		for (Map.Entry<String, long[]> entry : stateNameToPartitionOffsets.entrySet()) {

			int entryHash = entry.getKey().hashCode();
			if (entry.getValue() != null) {
				entryHash += Arrays.hashCode(entry.getValue());
			}
			result = 31 * result + entryHash;
		}
		return result;
	}
}
