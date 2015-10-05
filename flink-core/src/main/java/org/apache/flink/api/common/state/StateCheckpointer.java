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

package org.apache.flink.api.common.state;

import java.io.Serializable;

/**
 * Basic interface for creating {@link OperatorState} snapshots in stateful
 * streaming programs.
 * 
 * The user needs to implement the {@link #snapshotState(S, long, long)} and
 * {@link #restoreState(C)} methods that will be called to create and restore
 * state snapshots of the given states.
 * 
 * <p>
 * Note that the {@link OperatorState} is <i>synchronously</i> checkpointed.
 * While the state is written, the state cannot be accessed or modified so the
 * function needs not return a copy of its state, but may return a reference to
 * its state.
 * </p>
 * 
 * @param <S>
 *            Type of the operator state.
 * @param <C>
 *            Type of the snapshot that will be persisted.
 */
public interface StateCheckpointer<S, C extends Serializable> {

	/**
	 * Takes a snapshot of a given operator state. The snapshot returned will be
	 * persisted in the state backend for this job and restored upon failure.
	 * This method is called for all state partitions in case of partitioned
	 * state when creating a checkpoint.
	 * 
	 * @param state
	 *            The state for which the snapshot needs to be taken
	 * @param checkpointId
	 *            The ID of the checkpoint.
	 * @param checkpointTimestamp
	 *            The timestamp of the checkpoint, as derived by
	 *            System.currentTimeMillis() on the JobManager.
	 * 
	 * @return A snapshot of the operator state.
	 */
	C snapshotState(S state, long checkpointId, long checkpointTimestamp);

	/**
	 * Restores the operator states from a given snapshot. The restores state
	 * will be loaded back to the function. In case of partitioned state, each
	 * partition is restored independently.
	 * 
	 * @param stateSnapshot
	 *            The state snapshot that needs to be restored.
	 * @return The state corresponding to the snapshot.
	 */
	S restoreState(C stateSnapshot);
}
