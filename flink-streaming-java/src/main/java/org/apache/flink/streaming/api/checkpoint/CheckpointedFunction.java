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
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;

/**
 *
 * Similar to @{@link Checkpointed}, this interface must be implemented by functions that have potentially
 * repartitionable state that needs to be checkpointed. Methods from this interface are called upon checkpointing and
 * initialization of state.
 *
 * On {@link #initializeState(FunctionInitializationContext)} the implementing class receives a
 * {@link FunctionInitializationContext} which provides access to the {@link OperatorStateStore} (all) and
 * {@link org.apache.flink.api.common.state.KeyedStateStore} (only for keyed operators). Those allow to register
 * managed operator / keyed  user states. Furthermore, the context provides information whether or the operator was
 * restored.
 *
 *
 * In {@link #snapshotState(FunctionSnapshotContext)} the implementing class must ensure that all operator / keyed state
 * is passed to user states that have been registered during initialization, so that it is visible to the system
 * backends for checkpointing.
 *
 */
@PublicEvolving
public interface CheckpointedFunction {

	/**
	 * This method is called when a snapshot for a checkpoint is requested. This acts as a hook to the function to
	 * ensure that all state is exposed by means previously offered through {@link FunctionInitializationContext} when
	 * the Function was initialized, or offered now by {@link FunctionSnapshotContext} itself.
	 *
	 * @param context the context for drawing a snapshot of the operator
	 * @throws Exception
	 */
	void snapshotState(FunctionSnapshotContext context) throws Exception;

	/**
	 * This method is called when an operator is initialized, so that the function can set up it's state through
	 * the provided context. Initialization typically includes registering user states through the state stores
	 * that the context offers.
	 *
	 * @param context the context for initializing the operator
	 * @throws Exception
	 */
	void initializeState(FunctionInitializationContext context) throws Exception;

}
