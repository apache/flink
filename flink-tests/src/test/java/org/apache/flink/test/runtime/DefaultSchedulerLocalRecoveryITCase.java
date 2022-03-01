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

package org.apache.flink.test.runtime;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.client.program.MiniClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.scheduler.DefaultScheduler;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.WaitingCancelableInvokable;
import org.apache.flink.testutils.junit.FailsWithAdaptiveScheduler;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.configuration.JobManagerOptions.EXECUTION_FAILOVER_STRATEGY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotNull;

/** IT case to test local recovery using {@link DefaultScheduler}. */
public class DefaultSchedulerLocalRecoveryITCase extends TestLogger {

    private static final long TIMEOUT = 10_000L;

    @Test
    @Category(FailsWithAdaptiveScheduler.class) // FLINK-21450
    public void testLocalRecoveryFull() throws Exception {
        testLocalRecoveryInternal("full");
    }

    @Test
    @Category(FailsWithAdaptiveScheduler.class) // FLINK-21450
    public void testLocalRecoveryRegion() throws Exception {
        testLocalRecoveryInternal("region");
    }

    private void testLocalRecoveryInternal(String failoverStrategyValue) throws Exception {
        final Configuration configuration = new Configuration();
        configuration.setBoolean(CheckpointingOptions.LOCAL_RECOVERY, true);
        configuration.setString(EXECUTION_FAILOVER_STRATEGY.key(), failoverStrategyValue);

        final int parallelism = 10;
        final ArchivedExecutionGraph graph = executeSchedulingTest(configuration, parallelism);
        assertNonLocalRecoveredTasksEquals(graph, 1);
    }

    private void assertNonLocalRecoveredTasksEquals(ArchivedExecutionGraph graph, int expected) {
        int nonLocalRecoveredTasks = 0;
        for (ArchivedExecutionVertex vertex : graph.getAllExecutionVertices()) {
            int currentAttemptNumber = vertex.getCurrentExecutionAttempt().getAttemptNumber();
            if (currentAttemptNumber == 0) {
                // the task had never restarted and do not need to recover
                continue;
            }
            AllocationID priorAllocation =
                    vertex.getPriorExecutionAttempt(currentAttemptNumber - 1)
                            .getAssignedAllocationID();
            AllocationID currentAllocation =
                    vertex.getCurrentExecutionAttempt().getAssignedAllocationID();

            assertNotNull(priorAllocation);
            assertNotNull(currentAllocation);
            if (!currentAllocation.equals(priorAllocation)) {
                nonLocalRecoveredTasks++;
            }
        }
        assertThat(nonLocalRecoveredTasks, is(expected));
    }

    private ArchivedExecutionGraph executeSchedulingTest(
            Configuration configuration, int parallelism) throws Exception {
        configuration.setString(RestOptions.BIND_PORT, "0");

        final long slotIdleTimeout = TIMEOUT;
        configuration.setLong(JobManagerOptions.SLOT_IDLE_TIMEOUT, slotIdleTimeout);

        configuration.set(TaskManagerOptions.TOTAL_FLINK_MEMORY, MemorySize.parse("64mb"));
        configuration.set(TaskManagerOptions.FRAMEWORK_HEAP_MEMORY, MemorySize.parse("16mb"));
        configuration.set(TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY, MemorySize.parse("16mb"));

        final MiniClusterConfiguration miniClusterConfiguration =
                new MiniClusterConfiguration.Builder()
                        .setConfiguration(configuration)
                        .setNumTaskManagers(parallelism)
                        .setNumSlotsPerTaskManager(1)
                        .build();

        try (MiniCluster miniCluster = new MiniCluster(miniClusterConfiguration)) {
            miniCluster.start();

            MiniClusterClient miniClusterClient = new MiniClusterClient(configuration, miniCluster);

            JobGraph jobGraph = createJobGraph(parallelism);

            // wait for the submission to succeed
            JobID jobId = miniClusterClient.submitJob(jobGraph).get(TIMEOUT, TimeUnit.SECONDS);

            // wait until all tasks running before triggering task failures
            waitUntilAllVerticesRunning(jobId, miniCluster);

            // kill one TM to trigger task failure and remove one existing slot
            CompletableFuture<Void> terminationFuture = miniCluster.terminateTaskManager(0);
            terminationFuture.get();

            // restart a taskmanager as a replacement for the killed one
            miniCluster.startTaskManager();

            // wait until all tasks running again
            waitUntilAllVerticesRunning(jobId, miniCluster);

            ArchivedExecutionGraph graph =
                    miniCluster.getArchivedExecutionGraph(jobGraph.getJobID()).get();

            miniCluster.cancelJob(jobId).get();

            return graph;
        }
    }

    private void waitUntilAllVerticesRunning(JobID jobId, MiniCluster miniCluster)
            throws Exception {
        CommonTestUtils.waitForAllTaskRunning(
                () -> miniCluster.getExecutionGraph(jobId).get(TIMEOUT, TimeUnit.SECONDS),
                Deadline.fromNow(Duration.ofMillis(TIMEOUT)),
                false);
    }

    private JobGraph createJobGraph(int parallelism) throws IOException {
        final JobVertex source = new JobVertex("v1");
        source.setInvokableClass(WaitingCancelableInvokable.class);
        source.setParallelism(parallelism);

        ExecutionConfig executionConfig = new ExecutionConfig();
        executionConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 10));

        return JobGraphBuilder.newStreamingJobGraphBuilder()
                .addJobVertices(Arrays.asList(source))
                .setExecutionConfig(executionConfig)
                .build();
    }
}
