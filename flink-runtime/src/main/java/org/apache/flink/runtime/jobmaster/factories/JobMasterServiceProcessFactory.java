/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.factories;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobmaster.JobMasterServiceProcess;

import javax.annotation.Nullable;

import java.util.UUID;

/** Factory for the {@link JobMasterServiceProcess}. */
public interface JobMasterServiceProcessFactory {

    /**
     * Create a new {@link JobMasterServiceProcess} for the given leaderSessionId.
     *
     * @param leaderSessionId leaderSessionId for which to create a {@link JobMasterServiceProcess}
     * @return the newly created {@link JobMasterServiceProcess}
     */
    JobMasterServiceProcess create(UUID leaderSessionId);

    /**
     * Gets the {@link JobID} of the job for which this factory creates {@link
     * JobMasterServiceProcess}.
     */
    JobID getJobId();

    /**
     * Creates an {@link ArchivedExecutionGraph} for the job for which this factory creates {@link
     * JobMasterServiceProcess} with the given jobStatus and failure cause.
     *
     * @param jobStatus jobStatus which the {@link ArchivedExecutionGraph} should have
     * @param cause cause which the {@link ArchivedExecutionGraph} should be initialized with; null
     *     iff no failure cause
     * @return created {@link ArchivedExecutionGraph}
     */
    ArchivedExecutionGraph createArchivedExecutionGraph(
            JobStatus jobStatus, @Nullable Throwable cause);
}
