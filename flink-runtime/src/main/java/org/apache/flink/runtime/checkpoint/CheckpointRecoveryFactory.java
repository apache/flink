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

/** A factory for per Job checkpoint recovery components. */
public interface CheckpointRecoveryFactory {

    /**
     * Creates a RECOVERED {@link CompletedCheckpointStore} instance for a job. In this context,
     * RECOVERED means, that if we already have completed checkpoints from previous runs, we should
     * use them as the initial state.
     *
     * @param jobId Job ID to recover checkpoints for
     * @param maxNumberOfCheckpointsToRetain Maximum number of checkpoints to retain
     * @param userClassLoader User code class loader of the job
     * @return {@link CompletedCheckpointStore} instance for the job
     */
    CompletedCheckpointStore createRecoveredCompletedCheckpointStore(
            JobID jobId, int maxNumberOfCheckpointsToRetain, ClassLoader userClassLoader)
            throws Exception;

    /**
     * Creates a {@link CheckpointIDCounter} instance for a job.
     *
     * @param jobId Job ID to recover checkpoints for
     * @return {@link CheckpointIDCounter} instance for the job
     */
    CheckpointIDCounter createCheckpointIDCounter(JobID jobId) throws Exception;
}
