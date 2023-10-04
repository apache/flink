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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.TaskManagerOptionsInternal;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.entrypoint.WorkingDirectory;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.rest.util.NoOpFatalErrorHandler;
import org.apache.flink.runtime.rpc.AddressResolution;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.concurrent.Executors;

import org.apache.flink.shaded.guava31.com.google.common.net.InetAddresses;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opentest4j.TestAbortedException;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validates that the TaskManagerRunner startup properly obeys the configuration values.
 *
 * <p>NOTE: at least {@link #testDefaultFsParameterLoading()} should not be run in parallel to other
 * tests in the same JVM as it modifies a static (private) member of the {@link FileSystem} class
 * and verifies its content.
 */
@NotThreadSafe
class TaskManagerRunnerConfigurationTest {

    private static final RpcSystem RPC_SYSTEM = RpcSystem.load();

    private static final int TEST_TIMEOUT_SECONDS = 10;

    @TempDir private Path temporaryFolder;

    @Test
    void testTaskManagerRpcServiceShouldBindToConfiguredTaskManagerHostname() throws Exception {
        final String taskmanagerHost = "testhostname";
        final Configuration config =
                createFlinkConfigWithPredefinedTaskManagerHostname(taskmanagerHost);
        final HighAvailabilityServices highAvailabilityServices =
                createHighAvailabilityServices(config);

        RpcService taskManagerRpcService = null;
        try {
            taskManagerRpcService =
                    TaskManagerRunner.createRpcService(
                            config, highAvailabilityServices, RPC_SYSTEM);

            assertThat(taskManagerRpcService.getPort()).isGreaterThanOrEqualTo(0);
            assertThat(taskManagerRpcService.getAddress()).isEqualTo(taskmanagerHost);
        } finally {
            maybeCloseRpcService(taskManagerRpcService);
            highAvailabilityServices.closeWithOptionalClean(true);
        }
    }

    @Test
    void testTaskManagerRpcServiceShouldBindToHostnameAddress() throws Exception {
        final Configuration config = createFlinkConfigWithHostBindPolicy(HostBindPolicy.NAME);
        final HighAvailabilityServices highAvailabilityServices =
                createHighAvailabilityServices(config);

        RpcService taskManagerRpcService = null;
        try {
            taskManagerRpcService =
                    TaskManagerRunner.createRpcService(
                            config, highAvailabilityServices, RPC_SYSTEM);
            assertThat(taskManagerRpcService.getAddress()).isNotNull().isNotEmpty();
        } finally {
            maybeCloseRpcService(taskManagerRpcService);
            highAvailabilityServices.closeWithOptionalClean(true);
        }
    }

    @Test
    void testTaskManagerRpcServiceShouldBindToIpAddressDeterminedByConnectingToResourceManager()
            throws Exception {
        final ServerSocket testJobManagerSocket = openServerSocket();
        final Configuration config =
                createFlinkConfigWithJobManagerPort(testJobManagerSocket.getLocalPort());
        final HighAvailabilityServices highAvailabilityServices =
                createHighAvailabilityServices(config);

        RpcService taskManagerRpcService = null;
        try {
            taskManagerRpcService =
                    TaskManagerRunner.createRpcService(
                            config, highAvailabilityServices, RPC_SYSTEM);
            assertThat(taskManagerRpcService.getAddress()).matches(InetAddresses::isInetAddress);
        } finally {
            maybeCloseRpcService(taskManagerRpcService);
            highAvailabilityServices.closeWithOptionalClean(true);
            IOUtils.closeQuietly(testJobManagerSocket);
        }
    }

    @Test
    void testCreatingTaskManagerRpcServiceShouldFailIfRpcPortRangeIsInvalid() throws Exception {
        final Configuration config =
                new Configuration(
                        createFlinkConfigWithPredefinedTaskManagerHostname("example.org"));
        config.setString(TaskManagerOptions.RPC_PORT, "-1");

        final HighAvailabilityServices highAvailabilityServices =
                createHighAvailabilityServices(config);

        try {
            assertThatThrownBy(
                            () ->
                                    TaskManagerRunner.createRpcService(
                                            config, highAvailabilityServices, RPC_SYSTEM))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Invalid port range definition: -1");
        } finally {
            highAvailabilityServices.closeWithOptionalClean(true);
        }
    }

    @Test
    void testDefaultFsParameterLoading() throws Exception {
        try {
            final File tmpDir =
                    Files.createTempDirectory(temporaryFolder, UUID.randomUUID().toString())
                            .toFile();
            final File confFile = new File(tmpDir, GlobalConfiguration.FLINK_CONF_FILENAME);

            final URI defaultFS = new URI("otherFS", null, "localhost", 1234, null, null, null);

            final PrintWriter pw1 = new PrintWriter(confFile);
            pw1.println("fs.default-scheme: " + defaultFS);
            pw1.close();

            String[] args = new String[] {"--configDir", tmpDir.toString()};
            Configuration configuration = TaskManagerRunner.loadConfiguration(args);
            FileSystem.initialize(configuration);

            assertThat(defaultFS).isEqualTo(FileSystem.getDefaultFsUri());
        } finally {
            // reset FS settings
            FileSystem.initialize(new Configuration());
        }
    }

    @Test
    void testLoadDynamicalProperties() throws IOException, FlinkParseException {
        final File tmpDir =
                Files.createTempDirectory(temporaryFolder, UUID.randomUUID().toString()).toFile();
        final File confFile = new File(tmpDir, GlobalConfiguration.FLINK_CONF_FILENAME);
        final PrintWriter pw1 = new PrintWriter(confFile);
        final long managedMemory = 1024 * 1024 * 256;
        pw1.println(JobManagerOptions.ADDRESS.key() + ": localhost");
        pw1.println(TaskManagerOptions.MANAGED_MEMORY_SIZE.key() + ": " + managedMemory + "b");
        pw1.close();

        final String jmHost = "host1";
        final int jmPort = 12345;
        String[] args =
                new String[] {
                    "--configDir",
                    tmpDir.toString(),
                    "-D" + JobManagerOptions.ADDRESS.key() + "=" + jmHost,
                    "-D" + JobManagerOptions.PORT.key() + "=" + jmPort
                };
        Configuration configuration = TaskManagerRunner.loadConfiguration(args);
        assertThat(MemorySize.parse(managedMemory + "b"))
                .isEqualTo(configuration.get(TaskManagerOptions.MANAGED_MEMORY_SIZE));
        assertThat(jmHost).isEqualTo(configuration.get(JobManagerOptions.ADDRESS));
        assertThat(jmPort).isEqualTo(configuration.getInteger(JobManagerOptions.PORT));
    }

    @Test
    void testNodeIdShouldBeConfiguredValueIfExplicitlySet() throws Exception {
        String nodeId = "node1";
        Configuration configuration = new Configuration();
        configuration.set(TaskManagerOptionsInternal.TASK_MANAGER_NODE_ID, nodeId);
        TaskManagerServicesConfiguration servicesConfiguration =
                createTaskManagerServiceConfiguration(configuration);
        assertThat(servicesConfiguration.getNodeId()).isEqualTo(nodeId);
    }

    @Test
    void testNodeIdShouldBeExternalAddressIfNotExplicitlySet() throws Exception {
        TaskManagerServicesConfiguration servicesConfiguration =
                createTaskManagerServiceConfiguration(new Configuration());
        assertThat(servicesConfiguration.getNodeId())
                .isEqualTo(InetAddress.getLocalHost().getHostName());
    }

    private TaskManagerServicesConfiguration createTaskManagerServiceConfiguration(
            Configuration config) throws Exception {
        return TaskManagerServicesConfiguration.fromConfiguration(
                config,
                ResourceID.generate(),
                InetAddress.getLocalHost().getHostName(),
                true,
                TaskExecutorResourceUtils.resourceSpecFromConfigForLocalExecution(config),
                WorkingDirectory.create(
                        Files.createTempDirectory(temporaryFolder, UUID.randomUUID().toString())
                                .toFile()));
    }

    private static Configuration createFlinkConfigWithPredefinedTaskManagerHostname(
            final String taskmanagerHost) {
        final Configuration config = new Configuration();
        config.setString(TaskManagerOptions.HOST, taskmanagerHost);
        config.setString(JobManagerOptions.ADDRESS, "localhost");
        return new UnmodifiableConfiguration(config);
    }

    private static Configuration createFlinkConfigWithHostBindPolicy(
            final HostBindPolicy bindPolicy) {
        final Configuration config = new Configuration();
        config.setString(TaskManagerOptions.HOST_BIND_POLICY, bindPolicy.toString());
        config.setString(JobManagerOptions.ADDRESS, "localhost");
        config.set(AkkaOptions.LOOKUP_TIMEOUT_DURATION, Duration.ofMillis(10));
        return new UnmodifiableConfiguration(config);
    }

    private static Configuration createFlinkConfigWithJobManagerPort(final int port) {
        Configuration config = new Configuration();
        config.setString(JobManagerOptions.ADDRESS, "localhost");
        config.setInteger(JobManagerOptions.PORT, port);
        return new UnmodifiableConfiguration(config);
    }

    private HighAvailabilityServices createHighAvailabilityServices(final Configuration config)
            throws Exception {
        return HighAvailabilityServicesUtils.createHighAvailabilityServices(
                config,
                Executors.directExecutor(),
                AddressResolution.NO_ADDRESS_RESOLUTION,
                RpcSystem.load(),
                NoOpFatalErrorHandler.INSTANCE);
    }

    private static ServerSocket openServerSocket() {
        try {
            return new ServerSocket(0);
        } catch (IOException e) {
            throw new TestAbortedException("Skip test because could not open a server socket");
        }
    }

    private static void maybeCloseRpcService(@Nullable final RpcService rpcService)
            throws Exception {
        if (rpcService != null) {
            rpcService.closeAsync().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }
}
