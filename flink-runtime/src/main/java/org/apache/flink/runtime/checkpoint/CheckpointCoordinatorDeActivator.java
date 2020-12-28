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
import org.apache.flink.runtime.executiongraph.JobStatusListener;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This actor listens to changes in the JobStatus and activates or deactivates the periodic
 * checkpoint scheduler.
 */
public class CheckpointCoordinatorDeActivator implements JobStatusListener {

    private final CheckpointCoordinator coordinator;

    public CheckpointCoordinatorDeActivator(CheckpointCoordinator coordinator) {
        this.coordinator = checkNotNull(coordinator);
    }

    @Override
    public void jobStatusChanges(
            JobID jobId, JobStatus newJobStatus, long timestamp, Throwable error) {
        if (newJobStatus == JobStatus.RUNNING) {
            // start the checkpoint scheduler
            coordinator.startCheckpointScheduler();
        } else {
            // anything else should stop the trigger for now
            coordinator.stopCheckpointScheduler();
        }
    }
}
