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

import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.jobgraph.OperatorID;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * This interface provides methods to report and retrieve state for a task.
 *
 * <p>When a checkpoint or savepoint is triggered on a task, it will create snapshots for all stream operator instances
 * it owns. All operator snapshots from the task are then reported via this interface. A typical implementation will
 * dispatch and forward the reported state information to interested parties such as the checkpoint coordinator or a
 * local state store.
 *
 * <p>This interface also offers the complementary method that provides access to previously saved state of operator
 * instances in the task for restore purposes.
 */
public interface TaskStateManager extends CheckpointListener {

	/**
	 * Report the state snapshots for the operator instances running in the owning task.
	 *
	 * @param checkpointMetaData meta data from the checkpoint request.
	 * @param checkpointMetrics task level metrics for the checkpoint.
	 * @param acknowledgedState the reported states from the owning task.
	 */
	void reportStateHandles(
		@Nonnull CheckpointMetaData checkpointMetaData,
		@Nonnull CheckpointMetrics checkpointMetrics,
		@Nullable TaskStateSnapshot acknowledgedState);

	/**
	 * Returns means to restore previously reported state of an operator running in the owning task.
	 *
	 * @param operatorID the id of the operator for which we request state.
	 * @return previous state for the operator. Null if no previous state exists.
	 */
	OperatorSubtaskState operatorStates(OperatorID operatorID);
}
