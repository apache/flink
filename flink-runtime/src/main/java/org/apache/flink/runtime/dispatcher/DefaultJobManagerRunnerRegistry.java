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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * {@code DefaultJobManagerRunnerRegistry} is the default implementation of the {@link
 * JobManagerRunnerRegistry} interface.
 */
public class DefaultJobManagerRunnerRegistry implements JobManagerRunnerRegistry {

    @VisibleForTesting final Map<JobID, JobManagerRunner> jobManagerRunners;

    public DefaultJobManagerRunnerRegistry(int initialCapacity) {
        Preconditions.checkArgument(initialCapacity > 0);
        jobManagerRunners = CollectionUtil.newHashMapWithExpectedSize(initialCapacity);
    }

    @Override
    public boolean isRegistered(JobID jobId) {
        return jobManagerRunners.containsKey(jobId);
    }

    @Override
    public void register(JobManagerRunner jobManagerRunner) {
        Preconditions.checkArgument(
                !isRegistered(jobManagerRunner.getJobID()),
                "A job with the ID %s is already registered.",
                jobManagerRunner.getJobID());
        this.jobManagerRunners.put(jobManagerRunner.getJobID(), jobManagerRunner);
    }

    @Override
    public JobManagerRunner get(JobID jobId) {
        assertJobRegistered(jobId);
        return this.jobManagerRunners.get(jobId);
    }

    @Override
    public int size() {
        return this.jobManagerRunners.size();
    }

    @Override
    public Set<JobID> getRunningJobIds() {
        return new HashSet<>(this.jobManagerRunners.keySet());
    }

    @Override
    public Collection<JobManagerRunner> getJobManagerRunners() {
        return new ArrayList<>(this.jobManagerRunners.values());
    }

    @Override
    public CompletableFuture<Void> localCleanupAsync(JobID jobId, Executor unusedExecutor) {
        if (isRegistered(jobId)) {
            return unregister(jobId).closeAsync();
        }

        return FutureUtils.completedVoidFuture();
    }

    @Override
    public JobManagerRunner unregister(JobID jobId) {
        assertJobRegistered(jobId);
        return this.jobManagerRunners.remove(jobId);
    }

    private void assertJobRegistered(JobID jobId) {
        if (!isRegistered(jobId)) {
            throw new NoSuchElementException(
                    "There is no running job registered for the job ID " + jobId);
        }
    }
}
