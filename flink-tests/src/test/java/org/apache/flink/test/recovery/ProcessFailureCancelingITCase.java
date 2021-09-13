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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.dispatcher.MemoryExecutionGraphInfoStore;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponent;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.resourcemanager.StandaloneResourceManagerFactory;
import org.apache.flink.runtime.rest.util.NoOpFatalErrorHandler;
import org.apache.flink.runtime.rpc.AddressResolution;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.testutils.TestingUtils;
import org.apache.flink.runtime.util.BlobServerResource;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.runtime.webmonitor.retriever.impl.VoidMetricQueryServiceRetriever;
import org.apache.flink.runtime.zookeeper.ZooKeeperResource;
import org.apache.flink.test.recovery.AbstractTaskManagerProcessFailureRecoveryTest.TaskExecutorProcessEntryPoint;
import org.apache.flink.test.util.TestProcessBuilder;
import org.apache.flink.test.util.TestProcessBuilder.TestProcess;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.runtime.testutils.CommonTestUtils.getJavaCommandPath;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * This test makes sure that jobs are canceled properly in cases where the task manager went down
 * and did not respond to cancel messages.
 */
@SuppressWarnings("serial")
public class ProcessFailureCancelingITCase extends TestLogger {

    private static final String TASK_DEPLOYED_MARKER = "deployed";
    private static final Duration TIMEOUT = Duration.ofMinutes(2);

    @Rule public final BlobServerResource blobServerResource = new BlobServerResource();

    @Rule public final ZooKeeperResource zooKeeperResource = new ZooKeeperResource();

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testCancelingOnProcessFailure() throws Throwable {
        Assume.assumeTrue(
                "---- Skipping Process Failure test : Could not find java executable ----",
                getJavaCommandPath() != null);

        TestProcess taskManagerProcess = null;
        final TestingFatalErrorHandler fatalErrorHandler = new TestingFatalErrorHandler();

        Configuration config = new Configuration();
        config.setString(JobManagerOptions.ADDRESS, "localhost");
        config.set(AkkaOptions.ASK_TIMEOUT_DURATION, Duration.ofSeconds(100));
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
                RpcSystem.load().remoteServiceBuilder(config, "localhost", "0").createAndStart();
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
                        AddressResolution.NO_ADDRESS_RESOLUTION,
                        RpcSystem.load(),
                        NoOpFatalErrorHandler.INSTANCE);

        final AtomicReference<Throwable> programException = new AtomicReference<>();

        try {
            dispatcherResourceManagerComponent =
                    resourceManagerComponentFactory.create(
                            config,
                            ioExecutor,
                            rpcService,
                            haServices,
                            blobServerResource.getBlobServer(),
                            new HeartbeatServices(100L, 10000L, 2),
                            NoOpMetricRegistry.INSTANCE,
                            new MemoryExecutionGraphInfoStore(),
                            VoidMetricQueryServiceRetriever.INSTANCE,
                            fatalErrorHandler);

            TestProcessBuilder taskManagerProcessBuilder =
                    new TestProcessBuilder(TaskExecutorProcessEntryPoint.class.getName());
            taskManagerProcessBuilder.addConfigAsMainClassArgs(config);

            taskManagerProcess = taskManagerProcessBuilder.start();

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
                                                            System.out.println(
                                                                    TASK_DEPLOYED_MARKER);
                                                            wait();
                                                        }
                                                        return 0L;
                                                    }
                                                })
                                        .output(new DiscardingOutputFormat<>());

                                env.execute();
                            } catch (Throwable t) {
                                programException.set(t);
                            }
                        }
                    };

            Thread programThread = new Thread(programRunner);
            programThread.start();

            waitUntilAtLeastOneTaskHasBeenDeployed(taskManagerProcess);

            // kill the TaskManager after the job started to run
            taskManagerProcess.destroy();
            taskManagerProcess = null;

            // the job should fail within a few seconds due to heartbeat timeouts
            // since the CI environment is often slow, we conservatively give it up to 2 minutes

            programThread.join(TIMEOUT.toMillis());

            assertFalse("The program did not cancel in time", programThread.isAlive());

            Throwable error = programException.get();
            assertNotNull("The program did not fail properly", error);

            assertTrue(error instanceof ProgramInvocationException);
            // all seems well :-)
        } catch (Exception | Error e) {
            if (taskManagerProcess != null) {
                printOutput("TaskManager OUT", taskManagerProcess.getProcessOutput().toString());
                printOutput("TaskManager ERR", taskManagerProcess.getErrorOutput().toString());
            }
            throw ExceptionUtils.firstOrSuppressed(e, programException.get());
        } finally {
            if (taskManagerProcess != null) {
                taskManagerProcess.destroy();
            }
            if (dispatcherResourceManagerComponent != null) {
                dispatcherResourceManagerComponent.stopApplication(
                        ApplicationStatus.SUCCEEDED, null);
            }

            fatalErrorHandler.rethrowError();

            RpcUtils.terminateRpcService(rpcService, Time.seconds(100L));

            haServices.closeAndCleanupAllData();
        }
    }

    private static void waitUntilAtLeastOneTaskHasBeenDeployed(TestProcess taskManagerProcess)
            throws InterruptedException, TimeoutException {
        CommonTestUtils.waitUtil(
                () ->
                        taskManagerProcess
                                .getProcessOutput()
                                .toString()
                                .contains(TASK_DEPLOYED_MARKER),
                Duration.ofMinutes(2),
                null);
    }

    private static void printOutput(String processName, String logContents) {
        if (logContents == null || logContents.length() == 0) {
            return;
        }

        System.out.println("-----------------------------------------");
        System.out.println(" BEGIN SPAWNED PROCESS LOG FOR " + processName);
        System.out.println("-----------------------------------------");
        System.out.println(logContents);
        System.out.println("-----------------------------------------");
        System.out.println("		END SPAWNED PROCESS LOG");
        System.out.println("-----------------------------------------");
    }
}
