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

import org.apache.flink.runtime.state.StateHandle;

/**
 * This interface must be implemented by any invokable that has recoverable state and participates
 * in checkpointing.
 */
public interface StatefulTask<T extends StateHandle<?>> {

	/**
	 * Sets the initial state of the operator, upon recovery. The initial state is typically
	 * a snapshot of the state from a previous execution.
	 * 
	 * @param stateHandle The handle to the state.
	 * @param recoveryTimestamp Global recovery timestamp.
	 */
	void setInitialState(T stateHandle, long recoveryTimestamp) throws Exception;

	/**
	 * This method is either called directly and asynchronously by the checkpoint
	 * coordinator (in the case of functions that are directly notified - usually
	 * the data sources), or called synchronously when all incoming channels have
	 * reported a checkpoint barrier.
	 *
	 * @param checkpointId The ID of the checkpoint, incrementing.
	 * @param timestamp The timestamp when the checkpoint was triggered at the JobManager.
	 *
	 * @return {@code false} if the checkpoint can not be carried out, {@code true} otherwise
	 */
	boolean triggerCheckpoint(long checkpointId, long timestamp) throws Exception;


	/**
	 * Invoked when a checkpoint has been completed, i.e., when the checkpoint coordinator has received
	 * the notification from all participating tasks.
	 *
	 * @param checkpointId The ID of the checkpoint that is complete..
	 * @throws Exception The notification method may forward its exceptions.
	 */
	void notifyCheckpointComplete(long checkpointId) throws Exception;
}
