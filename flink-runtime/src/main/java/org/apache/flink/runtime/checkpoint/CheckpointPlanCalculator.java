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
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Computes the tasks to trigger, wait or commit for each checkpoint. */
public class CheckpointPlanCalculator {
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointPlanCalculator.class);

    private final JobID jobId;

    private final List<ExecutionVertex> tasksToTrigger;

    private final List<ExecutionVertex> tasksToWait;

    private final List<ExecutionVertex> tasksToCommitTo;

    public CheckpointPlanCalculator(
            JobID jobId,
            List<ExecutionVertex> tasksToTrigger,
            List<ExecutionVertex> tasksToWait,
            List<ExecutionVertex> tasksToCommitTo) {

        this.jobId = jobId;
        this.tasksToTrigger = Collections.unmodifiableList(tasksToTrigger);
        this.tasksToWait = Collections.unmodifiableList(tasksToWait);
        this.tasksToCommitTo = Collections.unmodifiableList(tasksToCommitTo);
    }

    public CheckpointPlan calculateCheckpointPlan() throws CheckpointException {
        return new CheckpointPlan(
                Collections.unmodifiableList(getTriggerExecutions()),
                Collections.unmodifiableMap(getAckTasks()),
                tasksToCommitTo);
    }

    /**
     * Check if all tasks that we need to trigger are running. If not, abort the checkpoint.
     *
     * @return the executions need to be triggered.
     * @throws CheckpointException the exception fails checking
     */
    private List<Execution> getTriggerExecutions() throws CheckpointException {
        List<Execution> executionsToTrigger = new ArrayList<>(tasksToTrigger.size());
        for (ExecutionVertex executionVertex : tasksToTrigger) {
            Execution ee = executionVertex.getCurrentExecutionAttempt();
            if (ee == null) {
                LOG.info(
                        "Checkpoint triggering task {} of job {} is not being executed at the moment. Aborting checkpoint.",
                        executionVertex.getTaskNameWithSubtaskIndex(),
                        executionVertex.getJobId());
                throw new CheckpointException(
                        CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING);
            } else if (ee.getState() == ExecutionState.RUNNING) {
                executionsToTrigger.add(ee);
            } else {
                LOG.info(
                        "Checkpoint triggering task {} of job {} is not in state {} but {} instead. Aborting checkpoint.",
                        executionVertex.getTaskNameWithSubtaskIndex(),
                        jobId,
                        ExecutionState.RUNNING,
                        ee.getState());
                throw new CheckpointException(
                        CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING);
            }
        }

        return executionsToTrigger;
    }

    /**
     * Check if all tasks that need to acknowledge the checkpoint are running. If not, abort the
     * checkpoint
     *
     * @return the execution vertices which should give an ack response
     * @throws CheckpointException the exception fails checking
     */
    private Map<ExecutionAttemptID, ExecutionVertex> getAckTasks() throws CheckpointException {
        Map<ExecutionAttemptID, ExecutionVertex> ackTasks = new HashMap<>(tasksToWait.size());

        for (ExecutionVertex ev : tasksToWait) {
            Execution ee = ev.getCurrentExecutionAttempt();
            if (ee != null) {
                ackTasks.put(ee.getAttemptId(), ev);
            } else {
                LOG.info(
                        "Checkpoint acknowledging task {} of job {} is not being executed at the moment. Aborting checkpoint.",
                        ev.getTaskNameWithSubtaskIndex(),
                        jobId);
                throw new CheckpointException(
                        CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING);
            }
        }
        return ackTasks;
    }
}
