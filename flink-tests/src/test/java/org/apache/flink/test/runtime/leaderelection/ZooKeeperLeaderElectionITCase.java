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

package org.apache.flink.test.runtime.leaderelection;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.minicluster.TestingMiniCluster;
import org.apache.flink.runtime.minicluster.TestingMiniClusterConfiguration;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.ZooKeeperTestUtils;
import org.apache.flink.util.TestLogger;

import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** Test the election of a new JobManager leader. */
public class ZooKeeperLeaderElectionITCase extends TestLogger {

    private static final Duration TEST_TIMEOUT = Duration.ofMinutes(5L);

    private static final Time RPC_TIMEOUT = Time.minutes(1L);

    private static TestingServer zkServer;

    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    @BeforeClass
    public static void setup() throws Exception {
        zkServer = new TestingServer(true);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (zkServer != null) {
            zkServer.close();
            zkServer = null;
        }
    }

    /**
     * Tests that a job can be executed after a new leader has been elected. For all except for the
     * last leader, the job is blocking. The JobManager will be terminated while executing the
     * blocking job. Once only one JobManager is left, it is checked that a non-blocking can be
     * successfully executed.
     */
    @Test
    public void testJobExecutionOnClusterWithLeaderChange() throws Exception {
        final int numDispatchers = 3;
        final int numTMs = 2;
        final int numSlotsPerTM = 2;

        final Configuration configuration =
                ZooKeeperTestUtils.createZooKeeperHAConfig(
                        zkServer.getConnectString(), tempFolder.newFolder().getAbsolutePath());

        // speed up refused registration retries
        configuration.setLong(ClusterOptions.REFUSED_REGISTRATION_DELAY, 50L);

        final TestingMiniClusterConfiguration miniClusterConfiguration =
                new TestingMiniClusterConfiguration.Builder()
                        .setConfiguration(configuration)
                        .setNumberDispatcherResourceManagerComponents(numDispatchers)
                        .setNumTaskManagers(numTMs)
                        .setNumSlotsPerTaskManager(numSlotsPerTM)
                        .build();

        Deadline timeout = Deadline.fromNow(TEST_TIMEOUT);

        try (TestingMiniCluster miniCluster = new TestingMiniCluster(miniClusterConfiguration)) {
            miniCluster.start();

            final int parallelism = numTMs * numSlotsPerTM;
            JobGraph jobGraph = createJobGraph(parallelism);

            miniCluster.submitJob(jobGraph).get();

            String previousLeaderAddress = null;

            for (int i = 0; i < numDispatchers - 1; i++) {
                final DispatcherGateway leaderDispatcherGateway =
                        getNextLeadingDispatcherGateway(
                                miniCluster, previousLeaderAddress, timeout);
                previousLeaderAddress = leaderDispatcherGateway.getAddress();

                CommonTestUtils.waitUntilCondition(
                        () ->
                                leaderDispatcherGateway
                                                .requestJobStatus(jobGraph.getJobID(), RPC_TIMEOUT)
                                                .get()
                                        == JobStatus.RUNNING,
                        timeout,
                        50L);

                leaderDispatcherGateway.shutDownCluster();
            }

            final DispatcherGateway leaderDispatcherGateway =
                    getNextLeadingDispatcherGateway(miniCluster, previousLeaderAddress, timeout);
            CommonTestUtils.waitUntilCondition(
                    () ->
                            leaderDispatcherGateway
                                            .requestJobStatus(jobGraph.getJobID(), RPC_TIMEOUT)
                                            .get()
                                    == JobStatus.RUNNING,
                    timeout,
                    50L);
            CompletableFuture<JobResult> jobResultFuture =
                    leaderDispatcherGateway.requestJobResult(jobGraph.getJobID(), RPC_TIMEOUT);
            BlockingOperator.unblock();

            assertThat(jobResultFuture.get().isSuccess(), is(true));
        }
    }

    private DispatcherGateway getNextLeadingDispatcherGateway(
            TestingMiniCluster miniCluster,
            @Nullable String previousLeaderAddress,
            Deadline timeout)
            throws Exception {
        CommonTestUtils.waitUntilCondition(
                () ->
                        !miniCluster
                                .getDispatcherGatewayFuture()
                                .get()
                                .getAddress()
                                .equals(previousLeaderAddress),
                timeout,
                20L);
        return miniCluster.getDispatcherGatewayFuture().get();
    }

    private JobGraph createJobGraph(int parallelism) throws IOException {
        BlockingOperator.isBlocking = true;
        final JobVertex vertex = new JobVertex("blocking operator");
        vertex.setParallelism(parallelism);
        vertex.setInvokableClass(BlockingOperator.class);

        JobGraph jobGraph = new JobGraph("Blocking test job", vertex);

        // explicitly allow restarts; this is necessary since the shutdown may result in the job
        // failing and hence being
        // removed from ZooKeeper. What happens to running jobs if the Dispatcher shuts down in an
        // orderly fashion
        // is undefined behavior. By allowing restarts we prevent the job from reaching a globally
        // terminal state,
        // causing it to be recovered by the next Dispatcher.
        ExecutionConfig executionConfig = new ExecutionConfig();
        executionConfig.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(10, Duration.ofSeconds(10).toMillis()));
        jobGraph.setExecutionConfig(executionConfig);

        return jobGraph;
    }

    /** Blocking invokable which is controlled by a static field. */
    public static class BlockingOperator extends AbstractInvokable {
        private static final Object lock = new Object();
        private static volatile boolean isBlocking = true;

        public BlockingOperator(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() throws Exception {
            synchronized (lock) {
                while (isBlocking) {
                    lock.wait();
                }
            }
        }

        public static void unblock() {
            synchronized (lock) {
                isBlocking = false;
                lock.notifyAll();
            }
        }
    }
}
