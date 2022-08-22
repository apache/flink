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

package org.apache.flink.streaming.runtime.testutils;

import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.MiniClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.Reference;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.testutils.PseudoRandomValueSelector.randomize;

/**
 * Starts a Flink mini cluster as a resource and registers the respective
 * StreamExecutionEnvironment.
 */
public class MiniClusterWithClientResource {

    private static final boolean RANDOMIZE_BUFFER_DEBLOAT_CONFIG =
            Boolean.parseBoolean(System.getProperty("buffer-debloat.randomization", "false"));

    private static final MemorySize DEFAULT_MANAGED_MEMORY_SIZE = MemorySize.parse("80m");

    private ClusterClient<?> clusterClient;

    private RestClusterClient<MiniClusterClient.MiniClusterId> restClusterClient;

    private final Logger log = LoggerFactory.getLogger(MiniClusterWithClientResource.class);

    private File temporaryFolder;

    private final MiniClusterResourceConfiguration miniClusterResourceConfiguration;

    private MiniCluster miniCluster = null;

    private UnmodifiableConfiguration restClusterClientConfig;

    private static final RpcSystem rpcSystem = RpcSystem.load();

    private TestStreamEnvironment env;

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(rpcSystem::close));
    }

    public MiniClusterWithClientResource(
            final MiniClusterResourceConfiguration miniClusterResourceConfiguration) {
        this.miniClusterResourceConfiguration =
                Preconditions.checkNotNull(miniClusterResourceConfiguration);
    }

    public void before() throws Exception {
        temporaryFolder = Files.createTempDirectory(UUID.randomUUID().toString()).toFile();

        startMiniCluster();

        int numberSlots =
                miniClusterResourceConfiguration.getNumberSlotsPerTaskManager()
                        * miniClusterResourceConfiguration.getNumberTaskManagers();

        clusterClient = createMiniClusterClient();
        restClusterClient = createRestClusterClient();

        env = new TestStreamEnvironment(miniCluster, numberSlots);
        TestStreamEnvironment.setAsContext(miniCluster, numberSlots);
    }

    public void after() {
        log.info("Finalization triggered: Cluster shutdown is going to be initiated.");

        TestStreamEnvironment.unsetAsContext();

        Exception exception = null;

        if (clusterClient != null) {
            try {
                clusterClient.close();
            } catch (Exception e) {
                exception = e;
            }
        }

        clusterClient = null;

        if (restClusterClient != null) {
            try {
                restClusterClient.close();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }
        }

        restClusterClient = null;

        if (miniCluster != null) {
            // try to cancel remaining jobs before shutting down cluster
            cancelAllJobs();

            final CompletableFuture<?> terminationFuture = miniCluster.closeAsync();

            try {
                terminationFuture.get(
                        miniClusterResourceConfiguration.getShutdownTimeout().toMilliseconds(),
                        TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }

            miniCluster = null;
        }

        if (exception != null) {
            log.warn("Could not properly shut down the MiniClusterResource.", exception);
        }

        temporaryFolder.delete();

        if (exception != null) {
            log.warn("Could not properly shut down the MiniClusterWithClientResource.", exception);
        }
    }

    public TestStreamEnvironment getTestStreamEnvironment() {
        return env;
    }

    public URI getRestAddress() {
        return miniCluster.getRestAddress().join();
    }

    private void cancelAllJobs() {
        try {
            final List<CompletableFuture<Acknowledge>> jobCancellationFutures =
                    miniCluster.listJobs().get().stream()
                            .filter(status -> !status.getJobState().isGloballyTerminalState())
                            .map(status -> miniCluster.cancelJob(status.getJobId()))
                            .collect(Collectors.toList());

            FutureUtils.waitForAll(jobCancellationFutures).get();

            CommonTestUtils.waitUntilCondition(
                    () -> {
                        final long unfinishedJobs =
                                miniCluster.listJobs().get().stream()
                                        .filter(
                                                status ->
                                                        !status.getJobState()
                                                                .isGloballyTerminalState())
                                        .count();
                        return unfinishedJobs == 0;
                    });
        } catch (Exception e) {
            log.warn("Exception while shutting down remaining jobs.", e);
        }
    }

    private void startMiniCluster() throws Exception {
        final Configuration configuration =
                new Configuration(miniClusterResourceConfiguration.getConfiguration());
        configuration.setString(CoreOptions.TMP_DIRS, temporaryFolder.getAbsolutePath());

        // we need to set this since a lot of test expect this because TestBaseUtils.startCluster()
        // enabled this by default
        if (!configuration.contains(CoreOptions.FILESYTEM_DEFAULT_OVERRIDE)) {
            configuration.setBoolean(CoreOptions.FILESYTEM_DEFAULT_OVERRIDE, true);
        }

        if (!configuration.contains(TaskManagerOptions.MANAGED_MEMORY_SIZE)) {
            configuration.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, DEFAULT_MANAGED_MEMORY_SIZE);
        }

        // set rest and rpc port to 0 to avoid clashes with concurrent MiniClusters
        configuration.setInteger(JobManagerOptions.PORT, 0);
        configuration.setString(RestOptions.BIND_PORT, "0");

        randomizeConfiguration(configuration);

        final MiniClusterConfiguration miniClusterConfiguration =
                new MiniClusterConfiguration.Builder()
                        .setConfiguration(configuration)
                        .setNumTaskManagers(
                                miniClusterResourceConfiguration.getNumberTaskManagers())
                        .setNumSlotsPerTaskManager(
                                miniClusterResourceConfiguration.getNumberSlotsPerTaskManager())
                        .setRpcServiceSharing(
                                miniClusterResourceConfiguration.getRpcServiceSharing())
                        .setHaServices(miniClusterResourceConfiguration.getHaServices())
                        .build();

        miniCluster =
                new MiniCluster(miniClusterConfiguration, () -> Reference.borrowed(rpcSystem));

        miniCluster.start();

        final URI restAddress = miniCluster.getRestAddress().get();
        createClientConfiguration(restAddress);
    }

    /**
     * This is the place for randomization the configuration that relates to task execution such as
     * TaskManagerConf. Configurations which relates to streaming should be randomized in
     * TestStreamEnvironment#randomizeConfiguration.
     */
    private static void randomizeConfiguration(Configuration configuration) {
        // randomize ITTests for enabling buffer de-bloating
        if (RANDOMIZE_BUFFER_DEBLOAT_CONFIG
                && !configuration.contains(TaskManagerOptions.BUFFER_DEBLOAT_ENABLED)) {
            randomize(configuration, TaskManagerOptions.BUFFER_DEBLOAT_ENABLED, true, false);
        }
    }

    private void createClientConfiguration(URI restAddress) {
        Configuration restClientConfig = new Configuration();
        restClientConfig.setString(JobManagerOptions.ADDRESS, restAddress.getHost());
        restClientConfig.setInteger(RestOptions.PORT, restAddress.getPort());
        this.restClusterClientConfig = new UnmodifiableConfiguration(restClientConfig);
    }

    private MiniClusterClient createMiniClusterClient() {
        return new MiniClusterClient(restClusterClientConfig, miniCluster);
    }

    private RestClusterClient<MiniClusterClient.MiniClusterId> createRestClusterClient()
            throws Exception {
        return new RestClusterClient<>(
                restClusterClientConfig, MiniClusterClient.MiniClusterId.INSTANCE);
    }
}
