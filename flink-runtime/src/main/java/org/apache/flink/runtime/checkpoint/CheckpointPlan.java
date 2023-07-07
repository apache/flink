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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;

import java.util.Collection;
import java.util.List;

/**
 * The plan of one checkpoint, indicating which tasks to trigger, waiting for acknowledge or commit
 * for one specific checkpoint.
 */
public interface CheckpointPlan extends Plan, FinishedTaskStateProvider {

    /** Returns tasks who need to acknowledge a checkpoint before it succeeds. */
    List<Execution> getTasksToWaitFor();

    /**
     * Returns tasks that are still running when taking the checkpoint, these need to be sent a
     * message when the checkpoint is confirmed.
     */
    List<ExecutionVertex> getTasksToCommitTo();

    /** Returns tasks that have already been finished when taking the checkpoint. */
    List<Execution> getFinishedTasks();

    /** Returns the job vertices whose tasks are all finished when taking the checkpoint. */
    @VisibleForTesting
    Collection<ExecutionJobVertex> getFullyFinishedJobVertex();

    /** Returns whether we support checkpoints after some tasks finished. */
    boolean mayHaveFinishedTasks();
}
