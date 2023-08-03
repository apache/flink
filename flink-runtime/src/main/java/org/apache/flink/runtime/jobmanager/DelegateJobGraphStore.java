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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.apache.flink.runtime.executiongraph.failover.flip1.FailoverStrategyFactoryLoader.isJobRestartDisable;

/** Delegate for job graph store with filter for job without restart strategy. */
public class DelegateJobGraphStore implements JobGraphStore {
    private final JobGraphStore delegate;
    private final Set<JobID> filterJobs;

    public DelegateJobGraphStore(JobGraphStore delegate) {
        this.delegate = delegate;
        this.filterJobs = new HashSet<>();
    }

    @Override
    public void start(JobGraphListener jobGraphListener) throws Exception {
        this.delegate.start(jobGraphListener);
    }

    @Override
    public void stop() throws Exception {
        this.delegate.stop();
    }

    @Override
    public void putJobGraph(JobGraph jobGraph) throws Exception {
        if (isJobRestartDisable(jobGraph.getJobConfiguration())) {
            this.filterJobs.add(jobGraph.getJobID());
        } else {
            this.delegate.putJobGraph(jobGraph);
        }
    }

    @Nullable
    @Override
    public JobGraph recoverJobGraph(JobID jobId) throws Exception {
        return filterJob(jobId) ? null : this.delegate.recoverJobGraph(jobId);
    }

    private boolean filterJob(JobID jobId) {
        return filterJobs.contains(jobId);
    }

    @Override
    public Collection<JobID> getJobIds() throws Exception {
        return this.delegate.getJobIds();
    }

    @VisibleForTesting
    Set<JobID> getFilterJobs() {
        return this.filterJobs;
    }

    @Override
    public void putJobResourceRequirements(
            JobID jobId, JobResourceRequirements jobResourceRequirements) throws Exception {
        if (filterJob(jobId)) {
            throw new IllegalStateException(
                    "Cannot update resource requirements for the job without restart strategy.");
        }

        this.delegate.putJobResourceRequirements(jobId, jobResourceRequirements);
    }

    @Override
    public CompletableFuture<Void> localCleanupAsync(JobID jobId, Executor executor) {
        if (filterJob(jobId)) {
            filterJobs.remove(jobId);
            return CompletableFuture.completedFuture(null);
        } else {
            return this.delegate.localCleanupAsync(jobId, executor);
        }
    }

    @Override
    public CompletableFuture<Void> globalCleanupAsync(JobID jobId, Executor executor) {
        if (filterJob(jobId)) {
            filterJobs.remove(jobId);
            return CompletableFuture.completedFuture(null);
        } else {
            return this.delegate.globalCleanupAsync(jobId, executor);
        }
    }
}
