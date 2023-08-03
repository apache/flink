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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.testutils.TestingJobGraphStore;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for delegate job graph store. */
class DelegateJobGraphStoreTest {
    @Test
    void testJobGraphForDelegateStore() throws Exception {
        JobGraph jobGraph1 = JobGraphTestUtils.emptyJobGraph();
        jobGraph1.getJobConfiguration().set(RestartStrategyOptions.RESTART_STRATEGY, "fixeddelay");
        JobGraph jobGraph2 = JobGraphTestUtils.emptyJobGraph();
        JobGraph jobGraph3 = JobGraphTestUtils.emptyJobGraph();

        CompletableFuture<JobID> jobResourceRequirementsFuture = new CompletableFuture<>();
        CompletableFuture<JobID> localCleanupFuture = new CompletableFuture<>();
        CompletableFuture<JobID> globalCleanupFuture = new CompletableFuture<>();
        CompletableFuture<JobID> stopFuture = new CompletableFuture<>();

        JobGraphStore store =
                TestingJobGraphStore.newBuilder()
                        .setPutJobResourceRequirementsConsumer(
                                (job, requirements) ->
                                        jobResourceRequirementsFuture.complete(job.getJobID()))
                        .setLocalCleanupFunction(
                                (jobId, executor) -> {
                                    localCleanupFuture.complete(jobId);
                                    return CompletableFuture.completedFuture(null);
                                })
                        .setGlobalCleanupFunction(
                                (jobId, executor) -> {
                                    globalCleanupFuture.complete(jobId);
                                    return CompletableFuture.completedFuture(null);
                                })
                        .setStopRunnable(() -> stopFuture.complete(jobGraph1.getJobID()))
                        .withAutomaticStart()
                        .build();
        DelegateJobGraphStore delegate = new DelegateJobGraphStore(store);

        // Add job1 and job2 to store.
        delegate.putJobGraph(jobGraph1);
        delegate.putJobGraph(jobGraph2);
        delegate.putJobGraph(jobGraph3);

        // Only add jobs without restart strategy in filter jobs.
        assertThat(delegate.getFilterJobs())
                .isEqualTo(
                        new HashSet<>(Arrays.asList(jobGraph2.getJobID(), jobGraph3.getJobID())));

        // Only get stored job ids for recovery.
        assertThat(delegate.getJobIds()).isEqualTo(Collections.singleton(jobGraph1.getJobID()));
        assertThat(store.getJobIds()).isEqualTo(Collections.singleton(jobGraph1.getJobID()));
        assertThat(
                        Objects.requireNonNull(delegate.recoverJobGraph(jobGraph1.getJobID()))
                                .getJobID())
                .isEqualTo(jobGraph1.getJobID());
        assertThat(delegate.recoverJobGraph(jobGraph2.getJobID())).isNull();

        delegate.putJobResourceRequirements(
                jobGraph1.getJobID(), new JobResourceRequirements(Collections.emptyMap()));
        assertThat(jobResourceRequirementsFuture.get(10, TimeUnit.SECONDS))
                .isEqualTo(jobGraph1.getJobID());
        // Update resource requirements for the job without restart strategy will throw exception.
        assertThatThrownBy(
                        () ->
                                delegate.putJobResourceRequirements(
                                        jobGraph2.getJobID(),
                                        new JobResourceRequirements(Collections.emptyMap())))
                .hasMessage(
                        "Cannot update resource requirements for the job without restart strategy.");

        delegate.localCleanupAsync(jobGraph1.getJobID(), null);
        assertThat(localCleanupFuture.get(10, TimeUnit.SECONDS)).isEqualTo(jobGraph1.getJobID());
        delegate.globalCleanupAsync(jobGraph1.getJobID(), null);
        assertThat(globalCleanupFuture.get(10, TimeUnit.SECONDS)).isEqualTo(jobGraph1.getJobID());

        // Clean filter jobs for local and global cleanup
        delegate.localCleanupAsync(jobGraph2.getJobID(), null);
        delegate.globalCleanupAsync(jobGraph3.getJobID(), null);
        assertThat(delegate.getFilterJobs().isEmpty()).isTrue();

        delegate.stop();
        assertThat(stopFuture.get(10, TimeUnit.SECONDS)).isEqualTo(jobGraph1.getJobID());
    }
}
