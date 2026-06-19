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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link CheckpointCoordinatorDeActivator} that tracks {@link Execution} states in addition to
 * {@link JobStatus} to avoid triggering checkpoints unnecessarily.
 */
public class ExecutionTrackingCheckpointCoordinatorDeActivator
        implements CheckpointCoordinatorDeActivator {
    private static final Logger LOG =
            LoggerFactory.getLogger(ExecutionTrackingCheckpointCoordinatorDeActivator.class);

    private final CheckpointCoordinator coordinator;
    private final Set<ExecutionAttemptID> pendingExecutions;
    @Nullable private JobStatus jobStatus;
    private boolean started;

    public ExecutionTrackingCheckpointCoordinatorDeActivator(CheckpointCoordinator coordinator) {
        this.coordinator = checkNotNull(coordinator);
        this.pendingExecutions = new HashSet<>();
        this.started = false;
    }

    @Override
    public void jobStatusChanges(JobID jobId, JobStatus newJobStatus, long timestamp) {
        this.jobStatus = newJobStatus;
        maybeStartOrStopCheckpointScheduler();
    }

    @Override
    public void onStateUpdate(
            ExecutionAttemptID execution, ExecutionState previousState, ExecutionState newState) {
        LOG.debug(
                "Received state update for execution {} from {} to {}",
                execution,
                previousState,
                newState);
        if (newState == ExecutionState.RUNNING || newState == ExecutionState.FINISHED) {
            pendingExecutions.remove(execution);
        } else {
            pendingExecutions.add(execution);
        }
        maybeStartOrStopCheckpointScheduler();
    }

    private void maybeStartOrStopCheckpointScheduler() {
        if (jobStatus == JobStatus.RUNNING && allTasksRunning()) {
            if (!started) {
                LOG.info("Starting checkpoint scheduler");
                started = true;
                coordinator.startCheckpointScheduler();
            } else {
                LOG.info(
                        "Checkpoint scheduler is already started, ignoring status change (some task has finished?)");
            }
        } else if (started) {
            LOG.info("Stopping checkpoint scheduler, current job status: {}", jobStatus);
            started = false;
            coordinator.stopCheckpointScheduler();
        } else {
            LOG.debug(
                    "Not Starting checkpoint scheduler, job status: {}, pending executions: {}",
                    jobStatus,
                    pendingExecutions);
        }
    }

    private boolean allTasksRunning() {
        return pendingExecutions.isEmpty();
    }
}
