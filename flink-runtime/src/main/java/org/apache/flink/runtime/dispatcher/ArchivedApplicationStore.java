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

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.application.ArchivedApplication;
import org.apache.flink.runtime.messages.webmonitor.ApplicationDetails;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.JobsOverview;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Optional;

/** Interface for a {@link ArchivedApplication} store. */
public interface ArchivedApplicationStore extends Closeable {

    /**
     * Returns the current number of stored {@link ArchivedApplication} instances.
     *
     * @return Current number of stored {@link ArchivedApplication} instances
     */
    int size();

    /**
     * Get the {@link ArchivedApplication} for the given application id. Empty if it isn't stored.
     *
     * @param applicationId identifying the serializable application to retrieve
     * @return The stored serializable application or empty
     */
    Optional<ArchivedApplication> get(ApplicationID applicationId);

    /**
     * Store the given {@link ArchivedApplication} in the store.
     *
     * @param archivedApplication to store
     * @throws IOException if the serializable archived application could not be stored in the store
     */
    void put(ArchivedApplication archivedApplication) throws IOException;

    /**
     * Return the collection of {@link ApplicationDetails} of all currently stored applications.
     *
     * @return Collection of application details of all currently stored applications
     */
    Collection<ApplicationDetails> getApplicationDetails();

    /**
     * Get the {@link ExecutionGraphInfo} for the given job id. Empty if it isn't stored.
     *
     * @param jobId identifying the serializable execution graph to retrieve
     * @return The stored serializable execution graph or empty
     */
    Optional<ExecutionGraphInfo> getExecutionGraphInfo(JobID jobId);

    /**
     * Return the collection of {@link JobDetails} of all currently stored jobs.
     *
     * @return Collection of job details of all currently stored jobs
     */
    Collection<JobDetails> getJobDetails();

    /**
     * Return the {@link JobsOverview} for all stored/past jobs.
     *
     * @return Jobs overview for all stored/past jobs
     */
    JobsOverview getJobsOverview();
}
