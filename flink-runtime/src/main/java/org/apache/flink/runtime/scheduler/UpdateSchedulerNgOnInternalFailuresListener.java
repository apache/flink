/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;

import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Calls {@link JobMaster#updateTaskExecutionState(TaskExecutionState)} on task failure. Calls
 * {@link SchedulerNG#handleGlobalFailure(Throwable)} on global failures.
 */
public class UpdateSchedulerNgOnInternalFailuresListener implements InternalFailuresListener {

    SchedulerNG schedulerNg;
    private final JobMaster jobMaster;

    public UpdateSchedulerNgOnInternalFailuresListener(final JobMaster jobMaster) {
        this.jobMaster = jobMaster;
    }

    private UpdateSchedulerNgOnInternalFailuresListener(
            final JobMaster jobMaster, SchedulerNG scheduler) {
        this.jobMaster = jobMaster;
        this.schedulerNg = scheduler;
    }

    public UpdateSchedulerNgOnInternalFailuresListener withScheduler(SchedulerNG scheduler) {
        return new UpdateSchedulerNgOnInternalFailuresListener(
                this.jobMaster, checkNotNull(scheduler));
    }

    @Override
    public void notifyTaskFailure(
            final ExecutionAttemptID attemptId,
            final Throwable t,
            Map<String, String> labels,
            final boolean cancelTask,
            final boolean releasePartitions) {

        final TaskExecutionState state =
                new TaskExecutionState(attemptId, ExecutionState.FAILED, t).withLabels(labels);
        if (jobMaster != null) {
            jobMaster.updateTaskExecutionState(state);
        } else {
            // TODO only temporary to make tests pass
            schedulerNg.updateTaskExecutionState(
                    new TaskExecutionStateTransition(state, cancelTask, releasePartitions));
        }
    }

    @Override
    public void notifyGlobalFailure(Throwable t) {
        schedulerNg.handleGlobalFailure(t);
    }
}
