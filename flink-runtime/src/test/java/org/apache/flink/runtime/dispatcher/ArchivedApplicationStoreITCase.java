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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.application.SingleJobApplication;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.util.Collection;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for the {@link ArchivedApplicationStore}. */
class ArchivedApplicationStoreITCase {

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    @TempDir File temporaryFolder;

    @ParameterizedTest
    @EnumSource(value = JobManagerOptions.ArchivedApplicationStoreType.class)
    void testArchivedApplicationStore(
            JobManagerOptions.ArchivedApplicationStoreType archivedApplicationStoreType)
            throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(
                JobManagerOptions.COMPLETED_APPLICATION_STORE_TYPE, archivedApplicationStoreType);
        try (final MiniCluster miniCluster =
                new ArchivedApplicationStoreTestUtils.PersistingMiniCluster(
                        new MiniClusterConfiguration.Builder()
                                .withRandomPorts()
                                .setConfiguration(configuration)
                                .build(),
                        temporaryFolder,
                        new ScheduledExecutorServiceAdapter(EXECUTOR_RESOURCE.getExecutor()))) {
            miniCluster.start();

            final JobGraph jobGraph = JobGraphTestUtils.singleNoOpJobGraph();
            miniCluster.submitApplication(new SingleJobApplication(jobGraph)).get();

            JobResult jobResult = miniCluster.requestJobResult(jobGraph.getJobID()).get();
            assertEquals(JobStatus.FINISHED, jobResult.getJobStatus().orElse(null));

            Collection<JobStatusMessage> jobs = miniCluster.listJobs().get();
            assertEquals(1, jobs.size());
            assertEquals(jobGraph.getJobID(), jobs.iterator().next().getJobId());
        }
    }

    /** Tests that a session cluster can terminate gracefully when jobs are still running. */
    @ParameterizedTest
    @EnumSource(value = JobManagerOptions.ArchivedApplicationStoreType.class)
    void testSuspendedJobOnClusterShutdown(
            JobManagerOptions.ArchivedApplicationStoreType archivedApplicationStoreType)
            throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(
                JobManagerOptions.COMPLETED_APPLICATION_STORE_TYPE, archivedApplicationStoreType);
        try (final MiniCluster miniCluster =
                new ArchivedApplicationStoreTestUtils.PersistingMiniCluster(
                        new MiniClusterConfiguration.Builder()
                                .withRandomPorts()
                                .setConfiguration(configuration)
                                .build(),
                        temporaryFolder,
                        new ScheduledExecutorServiceAdapter(EXECUTOR_RESOURCE.getExecutor()))) {
            miniCluster.start();
            final JobVertex vertex = new JobVertex("blockingVertex");
            // The adaptive scheduler expects that every vertex has a configured parallelism
            vertex.setParallelism(1);
            vertex.setInvokableClass(
                    ArchivedApplicationStoreTestUtils.SignallingBlockingNoOpInvokable.class);
            final JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(vertex);
            miniCluster.submitJob(jobGraph);
            ArchivedApplicationStoreTestUtils.SignallingBlockingNoOpInvokable.LATCH.await();
        }
    }
}
