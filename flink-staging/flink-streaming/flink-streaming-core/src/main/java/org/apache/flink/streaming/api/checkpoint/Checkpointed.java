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

package org.apache.flink.streaming.api.checkpoint;

import java.io.Serializable;

/**
 * This method must be implemented by functions that have state that needs to be
 * checkpointed. The functions get a call whenever a checkpoint should take place
 * and return a snapshot of their state, which will be checkpointed.
 * 
 * <p>This interface marks a function as <i>synchronously</i> checkpointed. While the
 * state is written, the function is not called, so the function needs not return a
 * copy of its state, but may return a reference to its state. Functions that can
 * continue to work and mutate the state, even while the state snapshot is being accessed,
 * can implement the {@link org.apache.flink.streaming.api.checkpoint.CheckpointedAsynchronously}
 * interface.</p>
 * 
 * @param <T> The type of the operator state.
 */
public interface Checkpointed<T extends Serializable> {

	/**
	 * Gets the current state of the function of operator. The state must reflect the result of all
	 * prior invocations to this function. 
	 * 
	 * @param checkpointId The ID of the checkpoint.
	 * @param checkpointTimestamp The timestamp of the checkpoint, as derived by
	 *                            System.currentTimeMillis() on the JobManager.
	 *                            
	 * @return A snapshot of the operator state.
	 * 
	 * @throws Exception Thrown if the creation of the state object failed. This causes the
	 *                   checkpoint to fail. The system may decide to fail the operation (and trigger
	 *                   recovery), or to discard this checkpoint attempt and to continue running
	 *                   and to try again with the next checkpoint attempt.
	 */
	T snapshotState(long checkpointId, long checkpointTimestamp) throws Exception;

	/**
	 * Restores the state of the function or operator to that of a previous checkpoint.
	 * This method is invoked when a function is executed as part of a recovery run.
	 * 	 * 
	 * @param state The state to be restored. 
	 */
	void restoreState(T state);
}
