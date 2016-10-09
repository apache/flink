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

package org.apache.flink.runtime.jobgraph.tasks;

import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;

import java.util.Collection;
import java.util.List;

/**
 * This interface must be implemented by any invokable that has recoverable state and participates
 * in checkpointing.
 */
public interface StatefulTask {

	/**
	 * Sets the initial state of the operator, upon recovery. The initial state is typically
	 * a snapshot of the state from a previous execution.
	 *
	 * TODO this should use @{@link org.apache.flink.runtime.state.CheckpointStateHandles} after redoing chained state.
	 *
	 * @param chainedState Handle for the chained operator states.
	 * @param keyGroupsState Handle for key group states.
	 */
	void setInitialState(
		ChainedStateHandle<StreamStateHandle> chainedState,
		List<KeyGroupsStateHandle> keyGroupsState,
		List<Collection<OperatorStateHandle>> partitionableOperatorState) throws Exception;

	/**
	 * This method is called to trigger a checkpoint, asynchronously by the checkpoint
	 * coordinator.
	 * 
	 * <p>This method is called for tasks that start the checkpoints by injecting the initial barriers,
	 * i.e., the source tasks. In contrast, checkpoints on downstream operators, which are the result of
	 * receiving checkpoint barriers, invoke the {@link #triggerCheckpointOnBarrier(CheckpointMetaData)}
	 * method.
	 *
	 * @param checkpointMetaData Meta data for about this checkpoint
	 *
	 * @return {@code false} if the checkpoint can not be carried out, {@code true} otherwise
	 */
	boolean triggerCheckpoint(CheckpointMetaData checkpointMetaData) throws Exception;

	/**
	 * This method is called when a checkpoint is triggered as a result of receiving checkpoint
	 * barriers on all input streams.
	 * 
	 * @param checkpointMetaData Meta data for about this checkpoint
	 * 
	 * @throws Exception Exceptions thrown as the result of triggering a checkpoint are forwarded.
	 */
	void triggerCheckpointOnBarrier(CheckpointMetaData checkpointMetaData) throws Exception;

	/**
	 * Invoked when a checkpoint has been completed, i.e., when the checkpoint coordinator has received
	 * the notification from all participating tasks.
	 *
	 * @param checkpointId The ID of the checkpoint that is complete..
	 * @throws Exception The notification method may forward its exceptions.
	 */
	void notifyCheckpointComplete(long checkpointId) throws Exception;
}
