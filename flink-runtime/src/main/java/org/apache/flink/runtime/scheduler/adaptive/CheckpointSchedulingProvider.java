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

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointScheduling;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Provide {@link CheckpointCoordinator} related operations for the {@link StopWithSavepoint}
 * operation.
 */
class CheckpointSchedulingProvider implements CheckpointScheduling {
    private final ExecutionGraph executionGraph;

    CheckpointSchedulingProvider(ExecutionGraph executionGraph) {
        this.executionGraph = checkNotNull(executionGraph, "executionGraph must not be null");
    }

    @Override
    public void startCheckpointScheduler() {
        final CheckpointCoordinator coordinator = executionGraph.getCheckpointCoordinator();
        if (coordinator == null) {
            // for a streaming job, the checkpoint coordinator is always set (even if
            // periodic checkpoints are disabled). The only situation where it can be null is when
            // the job reached a terminal state.
            checkState(
                    executionGraph.getState().isTerminalState(),
                    "CheckpointCoordinator is only allowed to be null if we are in a terminal state.");
            return;
        }
        if (coordinator.isPeriodicCheckpointingConfigured()
                && !coordinator.isPeriodicCheckpointingStarted()) {
            coordinator.startCheckpointScheduler();
        }
    }

    @Override
    public void stopCheckpointScheduler() {
        final CheckpointCoordinator coordinator = executionGraph.getCheckpointCoordinator();
        if (coordinator != null) {
            coordinator.stopCheckpointScheduler();
        }
    }
}
