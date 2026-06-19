/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.test.example.failing;

import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.function.Consumer;

import static org.apache.flink.test.util.TestUtils.submitJobAndWaitForResult;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for failing job submissions. */
@ExtendWith(TestLoggerExtension.class)
class JobSubmissionFailsITCase {

    private static final int NUM_TM = 2;
    private static final int NUM_SLOTS = 20;

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(getConfiguration())
                            .setNumberTaskManagers(NUM_TM)
                            .setNumberSlotsPerTaskManager(NUM_SLOTS / NUM_TM)
                            .build());

    private static Configuration getConfiguration() {
        Configuration config = new Configuration();
        config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("4m"));

        // to accommodate for 10 netty arenas (NUM_SLOTS / NUM_TM) x 16Mb
        // (NettyBufferPool.ARENA_SIZE)
        config.set(TaskManagerOptions.NETWORK_MEMORY_MIN, MemorySize.parse("256m"));

        return config;
    }

    private static JobGraph getWorkingJobGraph() {
        return JobGraphTestUtils.singleNoOpJobGraph();
    }

    // --------------------------------------------------------------------------------------------

    @Test
    void testExceptionInInitializeOnMaster(@InjectClusterClient ClusterClient<?> clusterClient)
            throws Exception {
        final JobVertex failingJobVertex = new FailingJobVertex("Failing job vertex");
        failingJobVertex.setInvokableClass(NoOpInvokable.class);
        failingJobVertex.setParallelism(1);

        final JobGraph failingJobGraph = JobGraphTestUtils.streamingJobGraph(failingJobVertex);
        runJobSubmissionTest(
                clusterClient,
                failingJobGraph,
                e -> assertThat(e).hasRootCauseMessage("Test exception."));
    }

    @Test
    void testSubmitEmptyJobGraph(@InjectClusterClient ClusterClient<?> clusterClient)
            throws Exception {
        final JobGraph jobGraph = JobGraphTestUtils.emptyJobGraph();
        runJobSubmissionTest(
                clusterClient,
                jobGraph,
                e -> assertThat(e).rootCause().hasMessageContaining("empty"));
    }

    @Test
    void testMissingJarBlob(@InjectClusterClient ClusterClient<?> clusterClient) throws Exception {
        final JobGraph jobGraph = getJobGraphWithMissingBlobKey();
        runJobSubmissionTest(
                clusterClient,
                jobGraph,
                e -> assertThat(e).hasRootCauseInstanceOf(IOException.class));
    }

    private void runJobSubmissionTest(
            ClusterClient<?> clusterClient, JobGraph jobGraph, Consumer<Throwable> assertion)
            throws Exception {
        assertThatThrownBy(
                        () ->
                                submitJobAndWaitForResult(
                                        clusterClient, jobGraph, getClass().getClassLoader()))
                .satisfies(assertion);

        submitJobAndWaitForResult(clusterClient, getWorkingJobGraph(), getClass().getClassLoader());
    }

    @Nonnull
    private static JobGraph getJobGraphWithMissingBlobKey() {
        final JobGraph jobGraph = getWorkingJobGraph();
        jobGraph.addUserJarBlobKey(new PermanentBlobKey());
        return jobGraph;
    }

    // --------------------------------------------------------------------------------------------

    private static class FailingJobVertex extends JobVertex {
        private static final long serialVersionUID = -6365291240199412135L;

        public FailingJobVertex(final String msg) {
            super(msg);
        }

        @Override
        public void initializeOnMaster(InitializeOnMasterContext context) throws Exception {
            throw new Exception("Test exception.");
        }
    }
}
