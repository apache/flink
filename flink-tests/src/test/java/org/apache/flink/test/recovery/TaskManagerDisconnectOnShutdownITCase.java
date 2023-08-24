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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.HeartbeatManagerOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.blocklist.BlocklistUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.entrypoint.SessionClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.io.network.partition.ResourceManagerPartitionTrackerImpl;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ArbitraryWorkerResourceSpecFactory;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerFactory;
import org.apache.flink.runtime.resourcemanager.ResourceManagerRuntimeServices;
import org.apache.flink.runtime.resourcemanager.ResourceManagerRuntimeServicesConfiguration;
import org.apache.flink.runtime.resourcemanager.StandaloneResourceManager;
import org.apache.flink.runtime.resourcemanager.StandaloneResourceManagerFactory;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.security.token.DelegationTokenManager;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.test.recovery.utils.TaskExecutorProcessEntryPoint;
import org.apache.flink.test.util.TestProcessBuilder;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.testutils.CommonTestUtils.getJavaCommandPath;
import static org.assertj.core.api.Assertions.assertThat;

/** This test ensures the TaskManager disconnects from the ResourceManager on shutdown. */
@ExtendWith(TestLoggerExtension.class)
public class TaskManagerDisconnectOnShutdownITCase {

    private static final Logger LOG =
            LoggerFactory.getLogger(TaskManagerDisconnectOnShutdownITCase.class);

    @Test
    public void testTaskManagerProcessFailure() {
        Configuration config = new Configuration();
        config.setString(JobManagerOptions.ADDRESS, "localhost");
        config.set(JobManagerOptions.PORT, 0);
        config.set(RestOptions.BIND_PORT, "0");

        // disable heartbeats
        config.set(HeartbeatManagerOptions.HEARTBEAT_RPC_FAILURE_THRESHOLD, -1);
        config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 2);

        config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("4m"));
        config.set(TaskManagerOptions.NETWORK_MEMORY_MIN, MemorySize.parse("3200k"));
        config.set(TaskManagerOptions.NETWORK_MEMORY_MAX, MemorySize.parse("3200k"));
        config.set(TaskManagerOptions.TASK_HEAP_MEMORY, MemorySize.parse("128m"));
        config.set(TaskManagerOptions.CPU_CORES, 1.0);
        config.setString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, "full");
        config.set(JobManagerOptions.RESOURCE_WAIT_TIMEOUT, Duration.ofSeconds(30L));

        // check that we run this test only if the java command
        // is available on this machine
        String javaCommand = getJavaCommandPath();
        if (javaCommand == null) {
            Assertions.fail("cannot find java executable");
        }

        final TaskManagerConnectionTracker tracker = new TaskManagerConnectionTracker();

        TestProcessBuilder.TestProcess taskManagerProcess = null;
        try (final SessionClusterEntrypoint clusterEntrypoint =
                new SessionClusterEntrypoint(config) {
                    @Override
                    protected DefaultDispatcherResourceManagerComponentFactory
                            createDispatcherResourceManagerComponentFactory(
                                    Configuration configuration) {
                        return DefaultDispatcherResourceManagerComponentFactory
                                .createSessionComponentFactory(
                                        new TestingStandaloneResourceManagerFactory(tracker));
                    }
                }) {
            clusterEntrypoint.startCluster();

            final Configuration taskManagerConfig = new Configuration(config);
            taskManagerConfig.set(JobManagerOptions.PORT, clusterEntrypoint.getRpcPort());
            TestProcessBuilder taskManagerProcessBuilder =
                    new TestProcessBuilder(TaskExecutorProcessEntryPoint.class.getName());
            taskManagerProcessBuilder.addConfigAsMainClassArgs(taskManagerConfig);

            // start the TaskManager processes
            taskManagerProcess = taskManagerProcessBuilder.start();

            tracker.waitForTaskManagerConnected();

            // shutdown TaskManager
            taskManagerProcess.destroy();

            tracker.waitForTaskManagerDisconnected();

            assertThat(tracker.getNumberOfConnectedTaskManager()).isEqualTo(1);
        } catch (Throwable t) {
            printProcessLog(taskManagerProcess);
            Assertions.fail(t.getMessage());
        } finally {
            if (taskManagerProcess != null && taskManagerProcess.getProcess().isAlive()) {
                LOG.error("TaskManager did not shutdown in time.");
                printProcessLog(taskManagerProcess);
                taskManagerProcess.getProcess().destroyForcibly();
            }
        }
    }

    protected static void printProcessLog(TestProcessBuilder.TestProcess process) {
        if (process == null) {
            System.out.println("-----------------------------------------");
            System.out.println(" TaskManager WAS NOT STARTED.");
            System.out.println("-----------------------------------------");
        } else {
            System.out.println("-----------------------------------------");
            System.out.println(" BEGIN SPAWNED PROCESS LOG FOR TaskManager");
            System.out.println("-----------------------------------------");
            System.out.println(process.getErrorOutput().toString());
            System.out.println("-----------------------------------------");
            System.out.println("		END SPAWNED PROCESS LOG");
            System.out.println("-----------------------------------------");
        }
    }

    // --------------------------------------------------------------------------------------------

    private static class TestingStandaloneResourceManagerFactory
            extends ResourceManagerFactory<ResourceID> {

        TaskManagerConnectionTracker tracker;

        public TestingStandaloneResourceManagerFactory(TaskManagerConnectionTracker tracker) {
            this.tracker = tracker;
        }

        @Override
        protected ResourceManager<ResourceID> createResourceManager(
                Configuration configuration,
                ResourceID resourceId,
                RpcService rpcService,
                UUID leaderSessionId,
                HeartbeatServices heartbeatServices,
                DelegationTokenManager delegationTokenManager,
                FatalErrorHandler fatalErrorHandler,
                ClusterInformation clusterInformation,
                @Nullable String webInterfaceUrl,
                ResourceManagerMetricGroup resourceManagerMetricGroup,
                ResourceManagerRuntimeServices resourceManagerRuntimeServices,
                Executor ioExecutor) {

            final Time standaloneClusterStartupPeriodTime =
                    ConfigurationUtils.getStandaloneClusterStartupPeriodTime(configuration);

            return new StandaloneResourceManager(
                    rpcService,
                    leaderSessionId,
                    resourceId,
                    heartbeatServices,
                    delegationTokenManager,
                    resourceManagerRuntimeServices.getSlotManager(),
                    ResourceManagerPartitionTrackerImpl::new,
                    BlocklistUtils.loadBlocklistHandlerFactory(configuration),
                    resourceManagerRuntimeServices.getJobLeaderIdService(),
                    clusterInformation,
                    fatalErrorHandler,
                    resourceManagerMetricGroup,
                    standaloneClusterStartupPeriodTime,
                    Time.fromDuration(configuration.get(AkkaOptions.ASK_TIMEOUT_DURATION)),
                    ioExecutor) {

                @Override
                public void disconnectTaskManager(ResourceID resourceId, Exception cause) {
                    tracker.disconnectTaskManager();
                    super.disconnectTaskManager(resourceId, cause);
                }

                @Override
                public CompletableFuture<Acknowledge> sendSlotReport(
                        ResourceID taskManagerResourceId,
                        InstanceID taskManagerRegistrationId,
                        SlotReport slotReport,
                        Time timeout) {
                    final CompletableFuture<Acknowledge> result =
                            super.sendSlotReport(
                                    taskManagerResourceId,
                                    taskManagerRegistrationId,
                                    slotReport,
                                    timeout);
                    tracker.connectTaskManager();
                    return result;
                }
            };
        }

        @Override
        protected ResourceManagerRuntimeServicesConfiguration
                createResourceManagerRuntimeServicesConfiguration(Configuration configuration)
                        throws ConfigurationException {
            return ResourceManagerRuntimeServicesConfiguration.fromConfiguration(
                    StandaloneResourceManagerFactory.getConfigurationWithoutMaxResourceIfSet(
                            configuration),
                    ArbitraryWorkerResourceSpecFactory.INSTANCE);
        }
    }

    private static class TaskManagerConnectionTracker {
        private final CompletableFuture<Void> taskManagerConnectedFuture =
                new CompletableFuture<>();
        private final CompletableFuture<Void> taskManagerDisconnectedFuture =
                new CompletableFuture<>();
        private final AtomicInteger numberOfConnectedTaskManager = new AtomicInteger();

        public void connectTaskManager() {
            numberOfConnectedTaskManager.incrementAndGet();
            taskManagerConnectedFuture.complete(null);
        }

        public void disconnectTaskManager() {
            taskManagerDisconnectedFuture.complete(null);
        }

        public void waitForTaskManagerConnected() throws Exception {
            taskManagerConnectedFuture.get();
        }

        public void waitForTaskManagerDisconnected() throws Exception {
            taskManagerConnectedFuture.get();
        }

        public int getNumberOfConnectedTaskManager() {
            return numberOfConnectedTaskManager.get();
        }
    }
}
