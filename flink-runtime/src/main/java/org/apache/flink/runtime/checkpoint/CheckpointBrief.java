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

import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;

import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The brief of one checkpoint, indicating which tasks to trigger, waiting for acknowledge or commit
 * for one specific checkpoint.
 */
public class CheckpointBrief {

    /** Tasks who need to be sent a message when a checkpoint is started. */
    private final List<Execution> tasksToTrigger;

    /** Tasks who need to acknowledge a checkpoint before it succeeds. */
    private final Map<ExecutionAttemptID, ExecutionVertex> tasksToWait;

    /**
     * Tasks that are still running when taking the checkpoint, these need to be sent a message when
     * the checkpoint is confirmed.
     */
    private final List<ExecutionVertex> tasksToCommitTo;

    /** Tasks that have already been finished when taking the checkpoint. */
    private final List<Execution> finishedTasks;

    /** The job vertices whose tasks are all finished when taking the checkpoint. */
    private final List<ExecutionJobVertex> fullyFinishedJobVertex;

    public CheckpointBrief(
            List<Execution> tasksToTrigger,
            Map<ExecutionAttemptID, ExecutionVertex> tasksToWait,
            List<ExecutionVertex> tasksToCommitTo,
            List<Execution> finishedTasks,
            List<ExecutionJobVertex> fullyFinishedJobVertex) {

        this.tasksToTrigger = checkNotNull(tasksToTrigger);
        this.tasksToWait = checkNotNull(tasksToWait);
        this.tasksToCommitTo = checkNotNull(tasksToCommitTo);
        this.finishedTasks = checkNotNull(finishedTasks);
        this.fullyFinishedJobVertex = checkNotNull(fullyFinishedJobVertex);
    }

    public List<Execution> getTasksToTrigger() {
        return tasksToTrigger;
    }

    public Map<ExecutionAttemptID, ExecutionVertex> getTasksToWait() {
        return tasksToWait;
    }

    public List<ExecutionVertex> getTasksToCommitTo() {
        return tasksToCommitTo;
    }

    public List<Execution> getFinishedTasks() {
        return finishedTasks;
    }

    public List<ExecutionJobVertex> getFullyFinishedJobVertex() {
        return fullyFinishedJobVertex;
    }
}
