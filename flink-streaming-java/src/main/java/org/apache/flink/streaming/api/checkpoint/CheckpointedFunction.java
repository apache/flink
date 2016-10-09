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

package org.apache.flink.streaming.api.checkpoint;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.state.OperatorStateStore;

/**
 *
 * Similar to @{@link Checkpointed}, this interface must be implemented by functions that have potentially
 * repartitionable state that needs to be checkpointed. Methods from this interface are called upon checkpointing and
 * restoring of state.
 *
 * On #initializeState the implementing class receives the {@link OperatorStateStore}
 * to store it's state. At least before each snapshot, all state persistent state must be stored in the state store.
 *
 * When the backend is received for initialization, the user registers states with the backend via
 * {@link org.apache.flink.api.common.state.StateDescriptor}. Then, all previously stored state is found in the
 * received {@link org.apache.flink.api.common.state.State} (currently only
 * {@link org.apache.flink.api.common.state.ListState} is supported.
 *
 * In #prepareSnapshot, the implementing class must ensure that all operator state is passed to the operator backend,
 * i.e. that the state was stored in the relevant {@link org.apache.flink.api.common.state.State} instances that
 * are requested on restore. Notice that users might want to clear and reinsert the complete state first if incremental
 * updates of the states are not possible.
 */
@PublicEvolving
public interface CheckpointedFunction {

	/**
	 *
	 * This method is called when state should be stored for a checkpoint. The state can be registered and written to
	 * the provided backend.
	 *
	 * @param checkpointId Id of the checkpoint to perform
	 * @param timestamp Timestamp of the checkpoint
	 * @throws Exception
	 */
	void prepareSnapshot(long checkpointId, long timestamp) throws Exception;

	/**
	 * This method is called when an operator is opened, so that the function can set the state backend to which it
	 * hands it's state on snapshot.
	 *
	 * @param stateStore the state store to which this function stores it's state
	 * @throws Exception
	 */
	void initializeState(OperatorStateStore stateStore) throws Exception;
}
