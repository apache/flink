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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.runtime.util.LongArrayList;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Checkpoint output stream that allows to write raw operator state in a partitioned way.
 */
@PublicEvolving
public final class OperatorStateCheckpointOutputStream
		extends NonClosingCheckpointOutputStream<OperatorStateHandle> {

	private LongArrayList partitionOffsets;
	private final long initialPosition;

	public OperatorStateCheckpointOutputStream(
			CheckpointStreamFactory.CheckpointStateOutputStream delegate) throws IOException {

		super(delegate);
		this.partitionOffsets = new LongArrayList(16);
		this.initialPosition = delegate.getPos();
	}

	/**
	 * User code can call this method to signal that it begins to write a new partition of operator state.
	 * Each previously written partition is considered final/immutable as soon as this method is called again.
	 */
	public void startNewPartition() throws IOException {
		partitionOffsets.add(delegate.getPos());
	}

	/**
	 * This method should not be public so as to not expose internals to user code.
	 */
	@Override
	OperatorStateHandle closeAndGetHandle() throws IOException {
		StreamStateHandle streamStateHandle = delegate.closeAndGetHandle();

		if (null == streamStateHandle) {
			return null;
		}

		if (partitionOffsets.isEmpty() && delegate.getPos() > initialPosition) {
			startNewPartition();
		}

		Map<String, OperatorStateHandle.StateMetaInfo> offsetsMap = new HashMap<>(1);

		OperatorStateHandle.StateMetaInfo metaInfo =
				new OperatorStateHandle.StateMetaInfo(
						partitionOffsets.toArray(),
					OperatorStateHandle.Mode.SPLIT_DISTRIBUTE);

		offsetsMap.put(DefaultOperatorStateBackend.DEFAULT_OPERATOR_STATE_NAME, metaInfo);

		return new OperatorStreamStateHandle(offsetsMap, streamStateHandle);
	}

	public int getNumberOfPartitions() {
		return partitionOffsets.size();
	}
}
