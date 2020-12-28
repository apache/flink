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

package org.apache.flink.test.recovery;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.dispatcher.MemoryArchivedExecutionGraphStore;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponent;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.resourcemanager.StandaloneResourceManagerFactory;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.BlobServerResource;
import org.apache.flink.runtime.util.LeaderConnectionInfo;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.runtime.webmonitor.retriever.impl.VoidMetricQueryServiceRetriever;
import org.apache.flink.runtime.zookeeper.ZooKeeperResource;
import org.apache.flink.test.recovery.AbstractTaskManagerProcessFailureRecoveryTest.TaskExecutorProcessEntryPoint;
import org.apache.flink.test.util.TestProcessBuilder;
import org.apache.flink.test.util.TestProcessBuilder.TestProcess;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.CheckedSupplier;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.testutils.CommonTestUtils.getJavaCommandPath;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * This test makes sure that jobs are canceled properly in cases where the task manager went down
 * and did not respond to cancel messages.
 */
@SuppressWarnings("serial")
public class ProcessFailureCancelingITCase extends TestLogger {

    @Rule public final BlobServerResource blobServerResource = new BlobServerResource();

    @Rule public final ZooKeeperResource zooKeeperResource = new ZooKeeperResource();

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testCancelingOnProcessFailure() throws Exception {
        final Time timeout = Time.minutes(2L);

        RestClusterClient<String> clusterClient = null;
        TestProcess taskManagerProcess = null;
        final TestingFatalErrorHandler fatalErrorHandler = new TestingFatalErrorHandler();

        Configuration config = new Configuration();
        config.setString(JobManagerOptions.ADDRESS, "localhost");
        config.setString(AkkaOptions.ASK_TIMEOUT, "100 s");
        config.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");
        config.setString(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zooKeeperResource.getConnectString());
        config.setString(
                HighAvailabilityOptions.HA_STORAGE_PATH,
                temporaryFolder.newFolder().getAbsolutePath());
        config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 2);
        config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("4m"));
        config.set(TaskManagerOptions.NETWORK_MEMORY_MIN, MemorySize.parse("3200k"));
        config.set(TaskManagerOptions.NETWORK_MEMORY_MAX, MemorySize.parse("3200k"));
        config.set(TaskManagerOptions.TASK_HEAP_MEMORY, MemorySize.parse("128m"));
        config.set(TaskManagerOptions.CPU_CORES, 1.0);
        config.setInteger(RestOptions.PORT, 0);

        final RpcService rpcService =
                AkkaRpcServiceUtils.remoteServiceBuilder(config, "localhost", 0).createAndStart();
        final int jobManagerPort = rpcService.getPort();
        config.setInteger(JobManagerOptions.PORT, jobManagerPort);

        final DispatcherResourceManagerComponentFactory resourceManagerComponentFactory =
                DefaultDispatcherResourceManagerComponentFactory.createSessionComponentFactory(
                        StandaloneResourceManagerFactory.getInstance());
        DispatcherResourceManagerComponent dispatcherResourceManagerComponent = null;

        final ScheduledExecutorService ioExecutor = TestingUtils.defaultExecutor();
        final HighAvailabilityServices haServices =
                HighAvailabilityServicesUtils.createHighAvailabilityServices(
                        config,
                        ioExecutor,
                        HighAvailabilityServicesUtils.AddressResolution.NO_ADDRESS_RESOLUTION);

        try {

            // check that we run this test only if the java command
            // is available on this machine
            if (getJavaCommandPath() == null) {
                System.out.println(
                        "---- Skipping Process Failure test : Could not find java executable ----");
                return;
            }

            dispatcherResourceManagerComponent =
                    resourceManagerComponentFactory.create(
                            config,
                            ioExecutor,
                            rpcService,
                            haServices,
                            blobServerResource.getBlobServer(),
                            new HeartbeatServices(100L, 1000L),
                            NoOpMetricRegistry.INSTANCE,
                            new MemoryArchivedExecutionGraphStore(),
                            VoidMetricQueryServiceRetriever.INSTANCE,
                            fatalErrorHandler);

            final Map<String, String> keyValues = config.toMap();
            final ArrayList<String> commands = new ArrayList<>((keyValues.size() << 1) + 8);

            TestProcessBuilder taskManagerProcessBuilder =
                    new TestProcessBuilder(TaskExecutorProcessEntryPoint.class.getName());
            taskManagerProcessBuilder.addConfigAsMainClassArgs(config);

            taskManagerProcess = taskManagerProcessBuilder.start();

            final Throwable[] errorRef = new Throwable[1];

            // start the test program, which infinitely blocks
            Runnable programRunner =
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                ExecutionEnvironment env =
                                        ExecutionEnvironment.createRemoteEnvironment(
                                                "localhost", 1337, config);
                                env.setParallelism(2);
                                env.setRestartStrategy(RestartStrategies.noRestart());

                                env.generateSequence(0, Long.MAX_VALUE)
                                        .map(
                                                new MapFunction<Long, Long>() {

                                                    @Override
                                                    public Long map(Long value) throws Exception {
                                                        synchronized (this) {
                                                            wait();
                                                        }
                                                        return 0L;
                                                    }
                                                })
                                        .output(new DiscardingOutputFormat<Long>());

                                env.execute();
                            } catch (Throwable t) {
                                errorRef[0] = t;
                            }
                        }
                    };

            Thread programThread = new Thread(programRunner);

            // kill the TaskManager
            programThread.start();

            final DispatcherGateway dispatcherGateway =
                    retrieveDispatcherGateway(rpcService, haServices);
            waitUntilAllSlotsAreUsed(dispatcherGateway, timeout);

            clusterClient = new RestClusterClient<>(config, "standalone");

            final Collection<JobID> jobIds = waitForRunningJobs(clusterClient, timeout);

            assertThat(jobIds, hasSize(1));
            final JobID jobId = jobIds.iterator().next();

            // kill the TaskManager after the job started to run
            taskManagerProcess.destroy();
            taskManagerProcess = null;

            // try to cancel the job
            clusterClient.cancel(jobId).get();

            // we should see a failure within reasonable time (10s is the ask timeout).
            // since the CI environment is often slow, we conservatively give it up to 2 minutes,
            // to fail, which is much lower than the failure time given by the heartbeats ( > 2000s)

            programThread.join(120000);

            assertFalse("The program did not cancel in time (2 minutes)", programThread.isAlive());

            Throwable error = errorRef[0];
            assertNotNull("The program did not fail properly", error);

            assertTrue(error instanceof ProgramInvocationException);
            // all seems well :-)
        } catch (Exception e) {
            printProcessLog("TaskManager", taskManagerProcess.getErrorOutput().toString());
            throw e;
        } catch (Error e) {
            printProcessLog("TaskManager 1", taskManagerProcess.getErrorOutput().toString());
            throw e;
        } finally {
            if (taskManagerProcess != null) {
                taskManagerProcess.destroy();
            }
            if (clusterClient != null) {
                clusterClient.close();
            }
            if (dispatcherResourceManagerComponent != null) {
                dispatcherResourceManagerComponent.deregisterApplicationAndClose(
                        ApplicationStatus.SUCCEEDED, null);
            }

            fatalErrorHandler.rethrowError();

            RpcUtils.terminateRpcService(rpcService, Time.seconds(100L));

            haServices.closeAndCleanupAllData();
        }
    }

    /**
     * Helper method to wait until the {@link Dispatcher} has set its fencing token.
     *
     * @param rpcService to use to connect to the dispatcher
     * @param haServices high availability services to connect to the dispatcher
     * @return {@link DispatcherGateway}
     * @throws Exception if something goes wrong
     */
    static DispatcherGateway retrieveDispatcherGateway(
            RpcService rpcService, HighAvailabilityServices haServices) throws Exception {
        final LeaderConnectionInfo leaderConnectionInfo =
                LeaderRetrievalUtils.retrieveLeaderConnectionInfo(
                        haServices.getDispatcherLeaderRetriever(), Duration.ofSeconds(10L));

        return rpcService
                .connect(
                        leaderConnectionInfo.getAddress(),
                        DispatcherId.fromUuid(leaderConnectionInfo.getLeaderSessionId()),
                        DispatcherGateway.class)
                .get();
    }

    private void waitUntilAllSlotsAreUsed(DispatcherGateway dispatcherGateway, Time timeout)
            throws ExecutionException, InterruptedException {
        FutureUtils.retrySuccessfulWithDelay(
                        () -> dispatcherGateway.requestClusterOverview(timeout),
                        Time.milliseconds(50L),
                        Deadline.fromNow(Duration.ofMillis(timeout.toMilliseconds())),
                        clusterOverview ->
                                clusterOverview.getNumTaskManagersConnected() >= 1
                                        && clusterOverview.getNumSlotsAvailable() == 0
                                        && clusterOverview.getNumSlotsTotal() == 2,
                        TestingUtils.defaultScheduledExecutor())
                .get();
    }

    private Collection<JobID> waitForRunningJobs(ClusterClient<?> clusterClient, Time timeout)
            throws ExecutionException, InterruptedException {
        return FutureUtils.retrySuccessfulWithDelay(
                        CheckedSupplier.unchecked(clusterClient::listJobs),
                        Time.milliseconds(50L),
                        Deadline.fromNow(Duration.ofMillis(timeout.toMilliseconds())),
                        jobs -> !jobs.isEmpty(),
                        TestingUtils.defaultScheduledExecutor())
                .get().stream()
                .map(JobStatusMessage::getJobId)
                .collect(Collectors.toList());
    }

    private void printProcessLog(String processName, String log) {
        if (log == null || log.length() == 0) {
            return;
        }

        System.out.println("-----------------------------------------");
        System.out.println(" BEGIN SPAWNED PROCESS LOG FOR " + processName);
        System.out.println("-----------------------------------------");
        System.out.println(log);
        System.out.println("-----------------------------------------");
        System.out.println("		END SPAWNED PROCESS LOG");
        System.out.println("-----------------------------------------");
    }
}
