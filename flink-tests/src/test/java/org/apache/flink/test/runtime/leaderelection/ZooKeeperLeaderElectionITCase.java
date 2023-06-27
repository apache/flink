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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.highavailability.zookeeper.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.leaderretrieval.DefaultLeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.minicluster.TestingMiniCluster;
import org.apache.flink.runtime.minicluster.TestingMiniClusterConfiguration;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.ZooKeeperTestUtils;
import org.apache.flink.runtime.util.ZooKeeperUtils;
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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/** Test the election of a new JobManager leader. */
public class ZooKeeperLeaderElectionITCase extends TestLogger {

    private static final Time RPC_TIMEOUT = Time.minutes(1L);

    private static TestingServer zkServer;

    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    @BeforeClass
    public static void setup() throws Exception {
        zkServer = ZooKeeperTestUtils.createAndStartZookeeperTestingServer();
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
                TestingMiniClusterConfiguration.newBuilder()
                        .setConfiguration(configuration)
                        .setNumberDispatcherResourceManagerComponents(numDispatchers)
                        .setNumTaskManagers(numTMs)
                        .setNumSlotsPerTaskManager(numSlotsPerTM)
                        .build();

        try (TestingMiniCluster miniCluster =
                        TestingMiniCluster.newBuilder(miniClusterConfiguration).build();
                final CuratorFrameworkWithUnhandledErrorListener curatorFramework =
                        ZooKeeperUtils.startCuratorFramework(
                                configuration,
                                exception -> fail("Fatal error in curator framework."))) {

            // We need to watch for resource manager leader changes to avoid race conditions.
            final DefaultLeaderRetrievalService resourceManagerLeaderRetrieval =
                    ZooKeeperUtils.createLeaderRetrievalService(
                            curatorFramework.asCuratorFramework(),
                            ZooKeeperUtils.getLeaderPath(ZooKeeperUtils.getResourceManagerNode()),
                            configuration);
            @SuppressWarnings("unchecked")
            final CompletableFuture<String>[] resourceManagerLeaderFutures =
                    (CompletableFuture<String>[]) new CompletableFuture[numDispatchers];
            for (int i = 0; i < numDispatchers; i++) {
                resourceManagerLeaderFutures[i] = new CompletableFuture<>();
            }
            resourceManagerLeaderRetrieval.start(
                    new TestLeaderRetrievalListener(resourceManagerLeaderFutures));

            miniCluster.start();

            final int parallelism = numTMs * numSlotsPerTM;
            JobGraph jobGraph = createJobGraph(parallelism);

            miniCluster.submitJob(jobGraph).get();

            String previousLeaderAddress = null;

            for (int i = 0; i < numDispatchers - 1; i++) {
                final DispatcherGateway leaderDispatcherGateway =
                        getNextLeadingDispatcherGateway(miniCluster, previousLeaderAddress);
                // Make sure resource manager has also changed leadership.
                resourceManagerLeaderFutures[i].get();
                previousLeaderAddress = leaderDispatcherGateway.getAddress();
                awaitRunningStatus(leaderDispatcherGateway, jobGraph);
                leaderDispatcherGateway.shutDownCluster();
            }

            final DispatcherGateway leaderDispatcherGateway =
                    getNextLeadingDispatcherGateway(miniCluster, previousLeaderAddress);
            // Make sure resource manager has also changed leadership.
            resourceManagerLeaderFutures[numDispatchers - 1].get();
            awaitRunningStatus(leaderDispatcherGateway, jobGraph);
            CompletableFuture<JobResult> jobResultFuture =
                    leaderDispatcherGateway.requestJobResult(jobGraph.getJobID(), RPC_TIMEOUT);
            BlockingOperator.unblock();

            assertThat(jobResultFuture.get().isSuccess(), is(true));

            resourceManagerLeaderRetrieval.stop();
        }
    }

    private static void awaitRunningStatus(DispatcherGateway dispatcherGateway, JobGraph jobGraph)
            throws Exception {
        CommonTestUtils.waitUntilCondition(
                () ->
                        dispatcherGateway.requestJobStatus(jobGraph.getJobID(), RPC_TIMEOUT).get()
                                == JobStatus.RUNNING,
                50L);
    }

    private DispatcherGateway getNextLeadingDispatcherGateway(
            TestingMiniCluster miniCluster, @Nullable String previousLeaderAddress)
            throws Exception {
        CommonTestUtils.waitUntilCondition(
                () ->
                        !miniCluster
                                .getDispatcherGatewayFuture()
                                .get()
                                .getAddress()
                                .equals(previousLeaderAddress),
                20L);
        return miniCluster.getDispatcherGatewayFuture().get();
    }

    private JobGraph createJobGraph(int parallelism) throws IOException {
        BlockingOperator.isBlocking = true;
        final JobVertex vertex = new JobVertex("blocking operator");
        vertex.setParallelism(parallelism);
        vertex.setInvokableClass(BlockingOperator.class);

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

        return JobGraphBuilder.newStreamingJobGraphBuilder()
                .addJobVertex(vertex)
                .setExecutionConfig(executionConfig)
                .build();
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

    private static class TestLeaderRetrievalListener implements LeaderRetrievalListener {

        private final CompletableFuture<String>[] futures;

        int changeIdx = 0;

        private TestLeaderRetrievalListener(CompletableFuture<String>[] futures) {
            this.futures = futures;
        }

        @Override
        public void notifyLeaderAddress(
                @Nullable String leaderAddress, @Nullable UUID leaderSessionID) {
            futures[changeIdx++].complete(leaderAddress);
        }

        @Override
        public void handleError(Exception exception) {}
    }
}
