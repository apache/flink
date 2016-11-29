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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.concurrent.RunnableFuture;

/**
 * This class is a default implementation for StateSnapshotContext.
 */
public class StateSnapshotContextSynchronousImpl implements StateSnapshotContext {
	
	private final long checkpointId;
	private final long checkpointTimestamp;
	
	/** Factory for he checkpointing stream */
	private final CheckpointStreamFactory streamFactory;
	
	/** Key group range for the operator that created this context. Only for keyed operators */
	private final KeyGroupRange keyGroupRange;

	/**
	 * Registry for opened streams to participate in the lifecycle of the stream task. Hence, this registry should be 
	 * obtained from and managed by the stream task.
	 */
	private final CloseableRegistry closableRegistry;

	private KeyedStateCheckpointOutputStream keyedStateCheckpointOutputStream;
	private OperatorStateCheckpointOutputStream operatorStateCheckpointOutputStream;

	@VisibleForTesting
	public StateSnapshotContextSynchronousImpl(long checkpointId, long checkpointTimestamp) {
		this.checkpointId = checkpointId;
		this.checkpointTimestamp = checkpointTimestamp;
		this.streamFactory = null;
		this.keyGroupRange = KeyGroupRange.EMPTY_KEY_GROUP_RANGE;
		this.closableRegistry = null;
	}


	public StateSnapshotContextSynchronousImpl(
			long checkpointId,
			long checkpointTimestamp,
			CheckpointStreamFactory streamFactory,
			KeyGroupRange keyGroupRange,
			CloseableRegistry closableRegistry) {

		this.checkpointId = checkpointId;
		this.checkpointTimestamp = checkpointTimestamp;
		this.streamFactory = Preconditions.checkNotNull(streamFactory);
		this.keyGroupRange = Preconditions.checkNotNull(keyGroupRange);
		this.closableRegistry = Preconditions.checkNotNull(closableRegistry);
	}

	@Override
	public long getCheckpointId() {
		return checkpointId;
	}

	@Override
	public long getCheckpointTimestamp() {
		return checkpointTimestamp;
	}

	private CheckpointStreamFactory.CheckpointStateOutputStream openAndRegisterNewStream() throws Exception {
		CheckpointStreamFactory.CheckpointStateOutputStream cout =
				streamFactory.createCheckpointStateOutputStream(checkpointId, checkpointTimestamp);

		closableRegistry.registerClosable(cout);
		return cout;
	}

	@Override
	public KeyedStateCheckpointOutputStream getRawKeyedOperatorStateOutput() throws Exception {
		if (null == keyedStateCheckpointOutputStream) {
			Preconditions.checkState(keyGroupRange != KeyGroupRange.EMPTY_KEY_GROUP_RANGE, "Not a keyed operator");
			keyedStateCheckpointOutputStream = new KeyedStateCheckpointOutputStream(openAndRegisterNewStream(), keyGroupRange);
		}
		return keyedStateCheckpointOutputStream;
	}

	@Override
	public OperatorStateCheckpointOutputStream getRawOperatorStateOutput() throws Exception {
		if (null == operatorStateCheckpointOutputStream) {
			operatorStateCheckpointOutputStream = new OperatorStateCheckpointOutputStream(openAndRegisterNewStream());
		}
		return operatorStateCheckpointOutputStream;
	}

	public RunnableFuture<KeyGroupsStateHandle> getKeyedStateStreamFuture() throws IOException {
		return closeAndUnregisterStreamToObtainStateHandle(keyedStateCheckpointOutputStream);
	}

	public RunnableFuture<OperatorStateHandle> getOperatorStateStreamFuture() throws IOException {
		return closeAndUnregisterStreamToObtainStateHandle(operatorStateCheckpointOutputStream);
	}

	private <T extends StreamStateHandle> RunnableFuture<T> closeAndUnregisterStreamToObtainStateHandle(
			NonClosingCheckpointOutputStream<T> stream) throws IOException {
		if (null == stream) {
			return null;
		}

		closableRegistry.unregisterClosable(stream.getDelegate());

		// for now we only support synchronous writing
		return new DoneFuture<>(stream.closeAndGetHandle());
	}

}