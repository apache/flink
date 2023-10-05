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

package org.apache.flink.runtime.entrypoint;

import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.SchedulerExecutionMode;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.dispatcher.ExecutionGraphInfoStore;
import org.apache.flink.runtime.dispatcher.MemoryExecutionGraphInfoStore;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServices;
import org.apache.flink.runtime.dispatcher.runner.DispatcherRunner;
import org.apache.flink.runtime.dispatcher.runner.DispatcherRunnerFactory;
import org.apache.flink.runtime.dispatcher.runner.TestingDispatcherRunner;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServicesBuilder;
import org.apache.flink.runtime.jobmanager.JobPersistenceComponentFactory;
import org.apache.flink.runtime.leaderelection.LeaderElection;
import org.apache.flink.runtime.resourcemanager.ResourceManagerFactory;
import org.apache.flink.runtime.resourcemanager.StandaloneResourceManagerFactory;
import org.apache.flink.runtime.resourcemanager.TestingResourceManagerFactory;
import org.apache.flink.runtime.rest.SessionRestEndpointFactory;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcSystemUtils;
import org.apache.flink.runtime.security.token.ExceptionThrowingDelegationTokenProvider;
import org.apache.flink.runtime.security.token.ExceptionThrowingDelegationTokenReceiver;
import org.apache.flink.runtime.testutils.TestJvmProcess;
import org.apache.flink.runtime.testutils.TestingClusterEntrypointProcess;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.testutils.executor.TestExecutorResource;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

/** Tests for the {@link ClusterEntrypoint}. */
public class ClusterEntrypointTest extends TestLogger {

    private static final long TIMEOUT_MS = 10000;

    private Configuration flinkConfig;

    @ClassRule
    public static final TestExecutorResource<?> TEST_EXECUTOR_RESOURCE =
            new TestExecutorResource<>(Executors::newSingleThreadExecutor);

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    @Before
    public void before() {
        flinkConfig = new Configuration();
        ExceptionThrowingDelegationTokenProvider.reset();
        ExceptionThrowingDelegationTokenReceiver.reset();
    }

    @After
    public void after() {
        ExceptionThrowingDelegationTokenProvider.reset();
        ExceptionThrowingDelegationTokenReceiver.reset();
    }

    @Test(expected = IllegalConfigurationException.class)
    public void testStandaloneSessionClusterEntrypointDeniedInReactiveMode() {
        flinkConfig.set(JobManagerOptions.SCHEDULER_MODE, SchedulerExecutionMode.REACTIVE);
        new TestingEntryPoint.Builder().setConfiguration(flinkConfig).build();
        fail("Entrypoint initialization is supposed to fail");
    }

    @Test
    public void testClusterStartShouldObtainTokens() throws Exception {
        ExceptionThrowingDelegationTokenProvider.addToken.set(true);
        final HighAvailabilityServices testingHaService =
                new TestingHighAvailabilityServicesBuilder().build();
        final TestingEntryPoint testingEntryPoint =
                new TestingEntryPoint.Builder()
                        .setConfiguration(flinkConfig)
                        .setHighAvailabilityServices(testingHaService)
                        .build();

        final CompletableFuture<ApplicationStatus> appStatusFuture =
                startClusterEntrypoint(testingEntryPoint);

        testingEntryPoint.closeAsync();
        assertThat(
                appStatusFuture.get(TIMEOUT_MS, TimeUnit.MILLISECONDS),
                is(ApplicationStatus.UNKNOWN));
        assertThat(
                ExceptionThrowingDelegationTokenReceiver.onNewTokensObtainedCallCount.get(), is(1));
    }

    @Test
    public void testCloseAsyncShouldNotCleanUpHAData() throws Exception {
        final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        final CompletableFuture<Void> cleanupAllDataFuture = new CompletableFuture<>();
        final HighAvailabilityServices testingHaService =
                new TestingHighAvailabilityServicesBuilder()
                        .setCloseFuture(closeFuture)
                        .setCleanupAllDataFuture(cleanupAllDataFuture)
                        .build();
        final TestingEntryPoint testingEntryPoint =
                new TestingEntryPoint.Builder()
                        .setConfiguration(flinkConfig)
                        .setHighAvailabilityServices(testingHaService)
                        .build();

        final CompletableFuture<ApplicationStatus> appStatusFuture =
                startClusterEntrypoint(testingEntryPoint);

        testingEntryPoint.closeAsync();
        assertThat(
                appStatusFuture.get(TIMEOUT_MS, TimeUnit.MILLISECONDS),
                is(ApplicationStatus.UNKNOWN));
        assertThat(closeFuture.isDone(), is(true));
        assertThat(cleanupAllDataFuture.isDone(), is(false));
    }

    @Test
    public void testCloseAsyncShouldNotDeregisterApp() throws Exception {
        final CompletableFuture<Void> deregisterFuture = new CompletableFuture<>();
        final TestingResourceManagerFactory testingResourceManagerFactory =
                new TestingResourceManagerFactory.Builder()
                        .setInternalDeregisterApplicationConsumer(
                                (ignored1, ignored2, ignore3) -> deregisterFuture.complete(null))
                        .build();
        final TestingEntryPoint testingEntryPoint =
                new TestingEntryPoint.Builder()
                        .setConfiguration(flinkConfig)
                        .setResourceManagerFactory(testingResourceManagerFactory)
                        .build();

        final CompletableFuture<ApplicationStatus> appStatusFuture =
                startClusterEntrypoint(testingEntryPoint);

        testingEntryPoint.closeAsync();
        assertThat(
                appStatusFuture.get(TIMEOUT_MS, TimeUnit.MILLISECONDS),
                is(ApplicationStatus.UNKNOWN));
        assertThat(deregisterFuture.isDone(), is(false));
    }

    @Test
    public void testClusterFinishedNormallyShouldDeregisterAppAndCleanupHAData() throws Exception {
        final CompletableFuture<Void> deregisterFuture = new CompletableFuture<>();
        final CompletableFuture<Void> cleanupAllDataFuture = new CompletableFuture<>();
        final CompletableFuture<ApplicationStatus> dispatcherShutDownFuture =
                new CompletableFuture<>();

        final HighAvailabilityServices testingHaService =
                new TestingHighAvailabilityServicesBuilder()
                        .setCleanupAllDataFuture(cleanupAllDataFuture)
                        .build();
        final TestingResourceManagerFactory testingResourceManagerFactory =
                new TestingResourceManagerFactory.Builder()
                        .setInternalDeregisterApplicationConsumer(
                                (ignored1, ignored2, ignore3) -> deregisterFuture.complete(null))
                        .setInitializeConsumer(
                                (ignore) ->
                                        dispatcherShutDownFuture.complete(
                                                ApplicationStatus.SUCCEEDED))
                        .build();
        final TestingDispatcherRunnerFactory testingDispatcherRunnerFactory =
                new TestingDispatcherRunnerFactory.Builder()
                        .setShutDownFuture(dispatcherShutDownFuture)
                        .build();

        final TestingEntryPoint testingEntryPoint =
                new TestingEntryPoint.Builder()
                        .setConfiguration(flinkConfig)
                        .setResourceManagerFactory(testingResourceManagerFactory)
                        .setDispatcherRunnerFactory(testingDispatcherRunnerFactory)
                        .setHighAvailabilityServices(testingHaService)
                        .build();

        final CompletableFuture<ApplicationStatus> appStatusFuture =
                startClusterEntrypoint(testingEntryPoint);

        assertThat(
                appStatusFuture.get(TIMEOUT_MS, TimeUnit.MILLISECONDS),
                is(ApplicationStatus.SUCCEEDED));
        assertThat(deregisterFuture.isDone(), is(true));
        assertThat(cleanupAllDataFuture.isDone(), is(true));
    }

    @Test
    public void testCloseAsyncShouldBeExecutedInShutdownHook() throws Exception {
        // This test only works on Linux and Mac OS because we are sending SIGTERM to a
        // JAVA process via "kill {pid}"
        assumeTrue(OperatingSystem.isLinux() || OperatingSystem.isMac());
        final File markerFile = new File(TEMPORARY_FOLDER.getRoot(), UUID.randomUUID() + ".marker");
        final TestingClusterEntrypointProcess clusterEntrypointProcess =
                new TestingClusterEntrypointProcess(markerFile);

        clusterEntrypointProcess.startProcess();

        boolean success = false;
        try {
            final long pid = clusterEntrypointProcess.getProcessId();
            assertTrue("Cannot determine process ID", pid != -1);

            // wait for the marker file to appear, which means the process is up properly
            TestJvmProcess.waitForMarkerFile(markerFile, 30000);

            TestJvmProcess.killProcessWithSigTerm(pid);

            final boolean exited =
                    clusterEntrypointProcess.waitFor(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            assertThat(
                    String.format("Process %s does not exit within %s ms", pid, TIMEOUT_MS),
                    exited,
                    is(true));
            assertThat(
                    "markerFile should be deleted in closeAsync shutdownHook",
                    markerFile.exists(),
                    is(false));
            success = true;
        } finally {
            if (!success) {
                clusterEntrypointProcess.printProcessLog();
            }

            clusterEntrypointProcess.destroy();
        }
    }

    @Test
    public void testWorkingDirectoryIsSetupWhenStartingTheClusterEntrypoint() throws Exception {
        final File workingDirBase = TEMPORARY_FOLDER.newFolder();
        final ResourceID resourceId = new ResourceID("foobar");

        configureWorkingDirectory(flinkConfig, workingDirBase, resourceId);

        final File workingDir =
                ClusterEntrypointUtils.generateJobManagerWorkingDirectoryFile(
                        flinkConfig, resourceId);

        try (final TestingEntryPoint testingEntryPoint =
                new TestingEntryPoint.Builder().setConfiguration(flinkConfig).build()) {
            testingEntryPoint.startCluster();
            assertTrue(workingDir.exists());
        }
    }

    private static void configureWorkingDirectory(
            Configuration configuration, File workingDirBase, ResourceID resourceId) {
        configuration.set(
                ClusterOptions.PROCESS_WORKING_DIR_BASE, workingDirBase.getAbsolutePath());
        configuration.set(JobManagerOptions.JOB_MANAGER_RESOURCE_ID, resourceId.toString());
    }

    @Test
    public void testWorkingDirectoryIsNotDeletedWhenStoppingClusterEntrypoint() throws Exception {
        final File workingDirBase = TEMPORARY_FOLDER.newFolder();
        final ResourceID resourceId = new ResourceID("foobar");

        configureWorkingDirectory(flinkConfig, workingDirBase, resourceId);

        final File workingDir =
                ClusterEntrypointUtils.generateJobManagerWorkingDirectoryFile(
                        flinkConfig, resourceId);

        try (final TestingEntryPoint testingEntryPoint =
                new TestingEntryPoint.Builder().setConfiguration(flinkConfig).build()) {
            testingEntryPoint.startCluster();
        }

        assertTrue(
                "The working directory has been deleted when the cluster entrypoint shut down. This should not happen.",
                workingDir.exists());
    }

    @Test
    public void testWorkingDirectoryIsDeletedIfApplicationCompletes() throws Exception {
        final File workingDirBase = TEMPORARY_FOLDER.newFolder();
        final ResourceID resourceId = new ResourceID("foobar");

        configureWorkingDirectory(flinkConfig, workingDirBase, resourceId);

        final File workingDir =
                ClusterEntrypointUtils.generateJobManagerWorkingDirectoryFile(
                        flinkConfig, resourceId);

        final CompletableFuture<ApplicationStatus> shutDownFuture = new CompletableFuture<>();

        final TestingEntryPoint testingEntryPoint =
                new TestingEntryPoint.Builder()
                        .setConfiguration(flinkConfig)
                        .setDispatcherRunnerFactory(
                                new TestingDispatcherRunnerFactory.Builder()
                                        .setShutDownFuture(shutDownFuture)
                                        .build())
                        .build();
        testingEntryPoint.startCluster();

        shutDownFuture.complete(ApplicationStatus.SUCCEEDED);
        testingEntryPoint.getTerminationFuture().join();

        assertFalse(
                "The working directory has not been deleted when the application completed successfully.",
                workingDir.exists());
    }

    private CompletableFuture<ApplicationStatus> startClusterEntrypoint(
            TestingEntryPoint testingEntryPoint) throws Exception {
        testingEntryPoint.startCluster();
        return FutureUtils.supplyAsync(
                () -> testingEntryPoint.getTerminationFuture().get(),
                TEST_EXECUTOR_RESOURCE.getExecutor());
    }

    private static class TestingEntryPoint extends ClusterEntrypoint {

        private final HighAvailabilityServices haService;

        private final ResourceManagerFactory<ResourceID> resourceManagerFactory;

        private final DispatcherRunnerFactory dispatcherRunnerFactory;

        private TestingEntryPoint(
                Configuration configuration,
                HighAvailabilityServices haService,
                ResourceManagerFactory<ResourceID> resourceManagerFactory,
                DispatcherRunnerFactory dispatcherRunnerFactory) {
            super(configuration);
            SignalHandler.register(LOG);
            this.haService = haService;
            this.resourceManagerFactory = resourceManagerFactory;
            this.dispatcherRunnerFactory = dispatcherRunnerFactory;
        }

        @Override
        protected DispatcherResourceManagerComponentFactory
                createDispatcherResourceManagerComponentFactory(Configuration configuration)
                        throws IOException {
            return new DefaultDispatcherResourceManagerComponentFactory(
                    dispatcherRunnerFactory,
                    resourceManagerFactory,
                    SessionRestEndpointFactory.INSTANCE);
        }

        @Override
        protected ExecutionGraphInfoStore createSerializableExecutionGraphStore(
                Configuration configuration, ScheduledExecutor scheduledExecutor) {
            return new MemoryExecutionGraphInfoStore();
        }

        @Override
        protected HighAvailabilityServices createHaServices(
                Configuration configuration, Executor executor, RpcSystemUtils rpcSystemUtils) {
            return haService;
        }

        @Override
        protected boolean supportsReactiveMode() {
            return false;
        }

        public static final class Builder {
            private HighAvailabilityServices haService =
                    new TestingHighAvailabilityServicesBuilder().build();

            private ResourceManagerFactory<ResourceID> resourceManagerFactory =
                    StandaloneResourceManagerFactory.getInstance();

            private DispatcherRunnerFactory dispatcherRunnerFactory =
                    new TestingDispatcherRunnerFactory.Builder().build();

            private Configuration configuration = new Configuration();

            public Builder setHighAvailabilityServices(HighAvailabilityServices haService) {
                this.haService = haService;
                return this;
            }

            public Builder setResourceManagerFactory(
                    ResourceManagerFactory<ResourceID> resourceManagerFactory) {
                this.resourceManagerFactory = resourceManagerFactory;
                return this;
            }

            public Builder setConfiguration(Configuration configuration) {
                this.configuration = configuration;
                return this;
            }

            public Builder setDispatcherRunnerFactory(
                    DispatcherRunnerFactory dispatcherRunnerFactory) {
                this.dispatcherRunnerFactory = dispatcherRunnerFactory;
                return this;
            }

            public TestingEntryPoint build() {
                return new TestingEntryPoint(
                        configuration, haService, resourceManagerFactory, dispatcherRunnerFactory);
            }
        }
    }

    private static class TestingDispatcherRunnerFactory implements DispatcherRunnerFactory {

        private final CompletableFuture<ApplicationStatus> shutDownFuture;

        private TestingDispatcherRunnerFactory(
                CompletableFuture<ApplicationStatus> shutDownFuture) {
            this.shutDownFuture = shutDownFuture;
        }

        @Override
        public DispatcherRunner createDispatcherRunner(
                LeaderElection leaderElection,
                FatalErrorHandler fatalErrorHandler,
                JobPersistenceComponentFactory jobPersistenceComponentFactory,
                Executor ioExecutor,
                RpcService rpcService,
                PartialDispatcherServices partialDispatcherServices)
                throws Exception {
            return TestingDispatcherRunner.newBuilder().setShutDownFuture(shutDownFuture).build();
        }

        public static final class Builder {
            private CompletableFuture<ApplicationStatus> shutDownFuture = new CompletableFuture<>();

            public Builder setShutDownFuture(CompletableFuture<ApplicationStatus> shutDownFuture) {
                this.shutDownFuture = shutDownFuture;
                return this;
            }

            public TestingDispatcherRunnerFactory build() {
                return new TestingDispatcherRunnerFactory(shutDownFuture);
            }
        }
    }
}
