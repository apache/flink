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

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

/**
 * This method must be implemented by functions that have state that needs to be
 * checkpointed. The functions get a call whenever a checkpoint should take place
 * and return a snapshot of their state, which will be checkpointed.
 * 
 * <h1>Deprecation and Replacement</h1>
 *
 * The short cut replacement for this interface is via {@link ListCheckpointed} and works
 * as shown in the example below. The {@code ListCheckpointed} interface returns a list of
 * elements (
 * 
 * 
 *
 * <pre>{@code
 * public class ExampleFunction<T> implements MapFunction<T, T>, ListCheckpointed<Integer> {
 *
 *     private int count;
 *
 *     public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
 *         return Collections.singletonList(this.count);
 *     }
 *
 *     public void restoreState(List<Integer> state) throws Exception {
 *         this.value = state.count.isEmpty() ? 0 : state.get(0);
 *     }
 *
 *     public T map(T value) {
 *         count++;
 *         return value;
 *     }
 * }
 * }</pre>
 * 
 * @param <T> The type of the operator state.
 * 
 * @deprecated Please use {@link ListCheckpointed} as illustrated above, or
 *             {@link CheckpointedFunction} for more control over the checkpointing process.
 */
@Deprecated
@PublicEvolving
public interface Checkpointed<T extends Serializable> extends CheckpointedRestoring<T> {

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
}
