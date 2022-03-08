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
import org.apache.flink.runtime.dispatcher.cleanup.LocallyCleanableResource;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;

import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.Set;

/** {@code JobManagerRunner} collects running jobs represented by {@link JobManagerRunner}. */
public interface JobManagerRunnerRegistry extends LocallyCleanableResource {

    /**
     * Checks whether a {@link JobManagerRunner} is registered under the given {@link JobID}.
     *
     * @param jobId The {@code JobID} to check.
     * @return {@code true}, if a {@code JobManagerRunner} is registered; {@code false} otherwise.
     */
    boolean isRegistered(JobID jobId);

    /** Registers the given {@link JobManagerRunner} instance. */
    void register(JobManagerRunner jobManagerRunner);

    /**
     * Returns the {@link JobManagerRunner} for the given {@code JobID}.
     *
     * @throws NoSuchElementException if the passed {@code JobID} does not belong to a registered
     *     {@code JobManagerRunner}.
     * @see #isRegistered(JobID)
     */
    JobManagerRunner get(JobID jobId);

    /** Returns the number of {@link JobManagerRunner} instances currently being registered. */
    int size();

    /** Returns {@link JobID} instances of registered {@link JobManagerRunner} instances. */
    Set<JobID> getRunningJobIds();

    /** Returns the registered {@link JobManagerRunner} instances. */
    Collection<JobManagerRunner> getJobManagerRunners();

    /**
     * Unregistered the {@link JobManagerRunner} with the given {@code JobID}. {@code null} is
     * returned if there's no {@code JobManagerRunner} registered for the given {@link JobID}.
     */
    JobManagerRunner unregister(JobID jobId);
}
