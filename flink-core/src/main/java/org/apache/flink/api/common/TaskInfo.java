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

package org.apache.flink.api.common;

import org.apache.flink.annotation.PublicEvolving;

/** The {@link TaskInfo} represents the meta information of current task. */
@PublicEvolving
public interface TaskInfo {

    /**
     * Gets the task name.
     *
     * @return The task name.
     */
    String getTaskName();

    /**
     * Gets the max parallelism aka the max number of subtasks.
     *
     * @return The max parallelism.
     */
    int getMaxNumberOfParallelSubtasks();

    /**
     * Gets the number of this parallel subtask. The numbering starts from 0 and goes up to
     * parallelism-1 (parallelism as returned by {@link #getNumberOfParallelSubtasks()}).
     *
     * @return The index of the parallel subtask.
     */
    int getIndexOfThisSubtask();

    /**
     * Gets the parallelism with which the parallel task runs.
     *
     * @return The parallelism with which the parallel task runs.
     */
    int getNumberOfParallelSubtasks();

    /**
     * Gets the attempt number of this parallel subtask. First attempt is numbered 0. The attempt
     * number corresponds to the number of times this task has been restarted(after
     * failure/cancellation) since the job was initially started.
     *
     * @return The attempt number of the subtask.
     */
    int getAttemptNumber();

    /**
     * Returns the name of the task, appended with the subtask indicator, such as "MyTask (3/6)#1",
     * where 3 would be ({@link #getIndexOfThisSubtask()} + 1), and 6 would be {@link
     * #getNumberOfParallelSubtasks()}, and 1 would be {@link #getAttemptNumber()}.
     *
     * @return The name of the task, with subtask indicator.
     */
    String getTaskNameWithSubtasks();

    /**
     * Returns the allocation id for where this task is executed.
     *
     * @return The allocation id for where this task is executed.
     */
    String getAllocationIDAsString();
}
