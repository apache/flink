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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorResource;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.concurrent.ScheduledExecutorService;

/** Tests for the {@link MemoryExecutionGraphInfoStore}. */
public class MemoryExecutionGraphInfoStoreITCase extends TestLogger {

    @ClassRule
    public static final TestExecutorResource<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorResource();

    /** Tests that a session cluster can terminate gracefully when jobs are still running. */
    @Test
    public void testPutSuspendedJobOnClusterShutdown() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.JOB_STORE_TYPE, JobManagerOptions.JobStoreType.Memory);
        try (final MiniCluster miniCluster =
                new ExecutionGraphInfoStoreTestUtils.PersistingMiniCluster(
                        new MiniClusterConfiguration.Builder()
                                .withRandomPorts()
                                .setConfiguration(configuration)
                                .build(),
                        new ScheduledExecutorServiceAdapter(EXECUTOR_RESOURCE.getExecutor()))) {
            miniCluster.start();
            final JobVertex vertex = new JobVertex("blockingVertex");
            // The adaptive scheduler expects that every vertex has a configured parallelism
            vertex.setParallelism(1);
            vertex.setInvokableClass(
                    ExecutionGraphInfoStoreTestUtils.SignallingBlockingNoOpInvokable.class);
            final JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(vertex);
            miniCluster.submitJob(jobGraph);
            ExecutionGraphInfoStoreTestUtils.SignallingBlockingNoOpInvokable.LATCH.await();
        }
    }
}
