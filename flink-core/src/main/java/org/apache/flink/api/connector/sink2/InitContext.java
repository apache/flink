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
     * Returns id of the restored checkpoint, if state was restored from the snapshot of a previous
     * execution.
     */
    OptionalLong getRestoredCheckpointId();

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
