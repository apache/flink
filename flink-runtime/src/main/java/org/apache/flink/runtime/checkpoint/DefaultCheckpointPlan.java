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
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The default implementation of he {@link CheckpointPlan}. */
public class DefaultCheckpointPlan implements CheckpointPlan {

    private final List<Execution> tasksToTrigger;

    private final List<Execution> tasksToWaitFor;

    private final List<ExecutionVertex> tasksToCommitTo;

    private final List<Execution> finishedTasks;

    private final List<ExecutionJobVertex> fullyFinishedJobVertex;

    private final boolean mayHaveFinishedTasks;

    DefaultCheckpointPlan(
            List<Execution> tasksToTrigger,
            List<Execution> tasksToWaitFor,
            List<ExecutionVertex> tasksToCommitTo,
            List<Execution> finishedTasks,
            List<ExecutionJobVertex> fullyFinishedJobVertex,
            boolean mayHaveFinishedTasks) {

        this.tasksToTrigger = checkNotNull(tasksToTrigger);
        this.tasksToWaitFor = checkNotNull(tasksToWaitFor);
        this.tasksToCommitTo = checkNotNull(tasksToCommitTo);
        this.finishedTasks = checkNotNull(finishedTasks);
        this.fullyFinishedJobVertex = checkNotNull(fullyFinishedJobVertex);
        this.mayHaveFinishedTasks = mayHaveFinishedTasks;
    }

    @Override
    public List<Execution> getTasksToTrigger() {
        return tasksToTrigger;
    }

    @Override
    public List<Execution> getTasksToWaitFor() {
        return tasksToWaitFor;
    }

    @Override
    public List<ExecutionVertex> getTasksToCommitTo() {
        return tasksToCommitTo;
    }

    @Override
    public List<Execution> getFinishedTasks() {
        return finishedTasks;
    }

    @Override
    public List<ExecutionJobVertex> getFullyFinishedJobVertex() {
        return fullyFinishedJobVertex;
    }

    @Override
    public boolean mayHaveFinishedTasks() {
        return mayHaveFinishedTasks;
    }
}
