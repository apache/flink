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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.JobsOverview;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;

/** Interface for a {@link ArchivedExecutionGraph} store. */
public interface ArchivedExecutionGraphStore extends Closeable {

    /**
     * Returns the current number of stored {@link ArchivedExecutionGraph}.
     *
     * @return Current number of stored {@link ArchivedExecutionGraph}
     */
    int size();

    /**
     * Get the {@link ArchivedExecutionGraph} for the given job id. Null if it isn't stored.
     *
     * @param jobId identifying the serializable execution graph to retrieve
     * @return The stored serializable execution graph or null
     */
    @Nullable
    ArchivedExecutionGraph get(JobID jobId);

    /**
     * Store the given {@link ArchivedExecutionGraph} in the store.
     *
     * @param archivedExecutionGraph to store
     * @throws IOException if the serializable execution graph could not be stored in the store
     */
    void put(ArchivedExecutionGraph archivedExecutionGraph) throws IOException;

    /**
     * Return the {@link JobsOverview} for all stored/past jobs.
     *
     * @return Jobs overview for all stored/past jobs
     */
    JobsOverview getStoredJobsOverview();

    /**
     * Return the collection of {@link JobDetails} of all currently stored jobs.
     *
     * @return Collection of job details of all currently stored jobs
     */
    Collection<JobDetails> getAvailableJobDetails();

    /**
     * Return the {@link JobDetails}} for the given job.
     *
     * @param jobId identifying the job for which to retrieve the {@link JobDetails}
     * @return {@link JobDetails} of the requested job or null if the job is not available
     */
    @Nullable
    JobDetails getAvailableJobDetails(JobID jobId);
}
