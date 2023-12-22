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

package org.apache.flink.api.connector.sink2;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.TaskInfo;

import java.util.OptionalLong;

/**
 * Common interface which exposes runtime info for creating {@link SinkWriter} and {@link Committer}
 * objects.
 */
@Internal
public interface InitContext {
    /**
     * The first checkpoint id when an application is started and not recovered from a previously
     * taken checkpoint or savepoint.
     */
    long INITIAL_CHECKPOINT_ID = 1;

    /**
     * Get the id of task where the committer is running.
     *
     * @deprecated This method is deprecated since Flink 1.19. All metadata about the task should be
     *     provided uniformly by {@link #getTaskInfo()}.
     * @see <a
     *     href="https://cwiki.apache.org/confluence/display/FLINK/FLIP-382%3A+Unify+the+Provision+of+Diverse+Metadata+for+Context-like+APIs">
     *     FLIP-382: Unify the Provision of Diverse Metadata for Context-like APIs </a>
     */
    @Deprecated
    default int getSubtaskId() {
        return getTaskInfo().getIndexOfThisSubtask();
    }

    /**
     * Get the number of parallel committer tasks.
     *
     * @deprecated This method is deprecated since Flink 1.19. All metadata about the task should be
     *     provided uniformly by {@link #getTaskInfo()}.
     * @see <a
     *     href="https://cwiki.apache.org/confluence/display/FLINK/FLIP-382%3A+Unify+the+Provision+of+Diverse+Metadata+for+Context-like+APIs">
     *     FLIP-382: Unify the Provision of Diverse Metadata for Context-like APIs </a>
     */
    @Deprecated
    default int getNumberOfParallelSubtasks() {
        return getTaskInfo().getNumberOfParallelSubtasks();
    }

    /**
     * Gets the attempt number of this parallel subtask. First attempt is numbered 0.
     *
     * @return Attempt number of the subtask.
     * @deprecated This method is deprecated since Flink 1.19. All metadata about the task should be
     *     provided uniformly by {@link #getTaskInfo()}.
     * @see <a
     *     href="https://cwiki.apache.org/confluence/display/FLINK/FLIP-382%3A+Unify+the+Provision+of+Diverse+Metadata+for+Context-like+APIs">
     *     FLIP-382: Unify the Provision of Diverse Metadata for Context-like APIs </a>
     */
    @Deprecated
    default int getAttemptNumber() {
        return getTaskInfo().getAttemptNumber();
    }

    /**
     * Returns id of the restored checkpoint, if state was restored from the snapshot of a previous
     * execution.
     */
    OptionalLong getRestoredCheckpointId();

    /**
     * The ID of the current job. Note that Job ID can change in particular upon manual restart. The
     * returned ID should NOT be used for any job management tasks.
     *
     * @deprecated This method is deprecated since Flink 1.19. All metadata about the job should be
     *     provided uniformly by {@link #getJobInfo()}.
     * @see <a
     *     href="https://cwiki.apache.org/confluence/display/FLINK/FLIP-382%3A+Unify+the+Provision+of+Diverse+Metadata+for+Context-like+APIs">
     *     FLIP-382: Unify the Provision of Diverse Metadata for Context-like APIs </a>
     */
    @Deprecated
    default JobID getJobId() {
        return getJobInfo().getJobId();
    }

    /**
     * Get the meta information of current job.
     *
     * @return the job meta information.
     */
    @PublicEvolving
    JobInfo getJobInfo();

    /**
     * Get the meta information of current task.
     *
     * @return the task meta information.
     */
    @PublicEvolving
    TaskInfo getTaskInfo();
}
