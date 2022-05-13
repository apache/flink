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
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.rest.util.NoOpFatalErrorHandler;
import org.apache.flink.runtime.rpc.AddressResolution;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.Executors;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import sun.net.util.IPAddressUtil;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNoException;

/**
 * Validates that the TaskManagerRunner startup properly obeys the configuration values.
 *
 * <p>NOTE: at least {@link #testDefaultFsParameterLoading()} should not be run in parallel to other
 * tests in the same JVM as it modifies a static (private) member of the {@link FileSystem} class
 * and verifies its content.
 */
@NotThreadSafe
public class TaskManagerRunnerConfigurationTest extends TestLogger {

    private static final RpcSystem RPC_SYSTEM = RpcSystem.load();

    private static final int TEST_TIMEOUT_SECONDS = 10;

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testTaskManagerRpcServiceShouldBindToConfiguredTaskManagerHostname()
            throws Exception {
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

            assertThat(taskManagerRpcService.getPort(), is(greaterThanOrEqualTo(0)));
            assertThat(taskManagerRpcService.getAddress(), is(equalTo(taskmanagerHost)));
        } finally {
            maybeCloseRpcService(taskManagerRpcService);
            highAvailabilityServices.closeAndCleanupAllData();
        }
    }

    @Test
    public void testTaskManagerRpcServiceShouldBindToHostnameAddress() throws Exception {
        final Configuration config = createFlinkConfigWithHostBindPolicy(HostBindPolicy.NAME);
        final HighAvailabilityServices highAvailabilityServices =
                createHighAvailabilityServices(config);

        RpcService taskManagerRpcService = null;
        try {
            taskManagerRpcService =
                    TaskManagerRunner.createRpcService(
                            config, highAvailabilityServices, RPC_SYSTEM);
            assertThat(taskManagerRpcService.getAddress(), not(isEmptyOrNullString()));
        } finally {
            maybeCloseRpcService(taskManagerRpcService);
            highAvailabilityServices.closeAndCleanupAllData();
        }
    }

    @Test
    public void
            testTaskManagerRpcServiceShouldBindToIpAddressDeterminedByConnectingToResourceManager()
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
            assertThat(taskManagerRpcService.getAddress(), is(ipAddress()));
        } finally {
            maybeCloseRpcService(taskManagerRpcService);
            highAvailabilityServices.closeAndCleanupAllData();
            IOUtils.closeQuietly(testJobManagerSocket);
        }
    }

    @Test
    public void testCreatingTaskManagerRpcServiceShouldFailIfRpcPortRangeIsInvalid()
            throws Exception {
        final Configuration config =
                new Configuration(
                        createFlinkConfigWithPredefinedTaskManagerHostname("example.org"));
        config.setString(TaskManagerOptions.RPC_PORT, "-1");

        final HighAvailabilityServices highAvailabilityServices =
                createHighAvailabilityServices(config);

        try {
            TaskManagerRunner.createRpcService(config, highAvailabilityServices, RPC_SYSTEM);
            fail("Should fail because -1 is not a valid port range");
        } catch (final IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("Invalid port range definition: -1"));
        } finally {
            highAvailabilityServices.closeAndCleanupAllData();
        }
    }

    @Test
    public void testDefaultFsParameterLoading() throws Exception {
        try {
            final File tmpDir = temporaryFolder.newFolder();
            final File confFile = new File(tmpDir, GlobalConfiguration.FLINK_CONF_FILENAME);

            final URI defaultFS = new URI("otherFS", null, "localhost", 1234, null, null, null);

            final PrintWriter pw1 = new PrintWriter(confFile);
            pw1.println("fs.default-scheme: " + defaultFS);
            pw1.close();

            String[] args = new String[] {"--configDir", tmpDir.toString()};
            Configuration configuration = TaskManagerRunner.loadConfiguration(args);
            FileSystem.initialize(configuration);

            assertEquals(defaultFS, FileSystem.getDefaultFsUri());
        } finally {
            // reset FS settings
            FileSystem.initialize(new Configuration());
        }
    }

    @Test
    public void testLoadDynamicalProperties() throws IOException, FlinkParseException {
        final File tmpDir = temporaryFolder.newFolder();
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
        assertEquals(
                MemorySize.parse(managedMemory + "b"),
                configuration.get(TaskManagerOptions.MANAGED_MEMORY_SIZE));
        assertEquals(jmHost, configuration.get(JobManagerOptions.ADDRESS));
        assertEquals(jmPort, configuration.getInteger(JobManagerOptions.PORT));
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
            assumeNoException("Skip test because could not open a server socket", e);
            throw new RuntimeException("satisfy compiler");
        }
    }

    private static void maybeCloseRpcService(@Nullable final RpcService rpcService)
            throws Exception {
        if (rpcService != null) {
            rpcService.stopService().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }

    private static TypeSafeMatcher<String> ipAddress() {
        return new TypeSafeMatcher<String>() {
            @Override
            protected boolean matchesSafely(String value) {
                return IPAddressUtil.isIPv4LiteralAddress(value)
                        || IPAddressUtil.isIPv6LiteralAddress(value);
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("Is an ip address.");
            }
        };
    }
}
