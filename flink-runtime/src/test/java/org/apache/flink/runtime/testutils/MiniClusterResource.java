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

package org.apache.flink.runtime.testutils;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.HeartbeatManagerOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.resourcemanager.ResourceOverview;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.Reference;
import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.testutils.PseudoRandomValueSelector.randomize;

/** Resource which starts a {@link MiniCluster} for testing purposes. */
public class MiniClusterResource extends ExternalResource {
    private static final boolean RANDOMIZE_BUFFER_DEBLOAT_CONFIG =
            Boolean.parseBoolean(System.getProperty("buffer-debloat.randomization", "false"));

    private static final MemorySize DEFAULT_MANAGED_MEMORY_SIZE = MemorySize.parse("80m");

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final MiniClusterResourceConfiguration miniClusterResourceConfiguration;

    private MiniCluster miniCluster = null;

    private int numberSlots = -1;

    private UnmodifiableConfiguration restClusterClientConfig;

    private static final RpcSystem rpcSystem = RpcSystem.load();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> rpcSystem.close()));
    }

    public MiniClusterResource(
            final MiniClusterResourceConfiguration miniClusterResourceConfiguration) {
        this.miniClusterResourceConfiguration =
                Preconditions.checkNotNull(miniClusterResourceConfiguration);
    }

    public int getNumberSlots() {
        return numberSlots;
    }

    public MiniCluster getMiniCluster() {
        return miniCluster;
    }

    public UnmodifiableConfiguration getClientConfiguration() {
        return restClusterClientConfig;
    }

    /** @deprecated use {@link #getRestAddress()} instead */
    @Deprecated
    public URI getRestAddres() {
        return getRestAddress();
    }

    public URI getRestAddress() {
        return miniCluster.getRestAddress().join();
    }

    @Override
    public void before() throws Exception {
        temporaryFolder.create();

        startMiniCluster();

        numberSlots =
                miniClusterResourceConfiguration.getNumberSlotsPerTaskManager()
                        * miniClusterResourceConfiguration.getNumberTaskManagers();
    }

    public void cancelAllJobsAndWaitUntilSlotsAreFreed() {
        final long heartbeatTimeout =
                miniCluster.getConfiguration().get(HeartbeatManagerOptions.HEARTBEAT_INTERVAL);
        final long shutdownTimeout =
                miniClusterResourceConfiguration.getShutdownTimeout().toMilliseconds();
        Preconditions.checkState(
                heartbeatTimeout < shutdownTimeout,
                "Heartbeat timeout (%d) needs to be lower than the shutdown timeout (%d) in order to ensure reliable job cancellation and resource cleanup.",
                heartbeatTimeout,
                shutdownTimeout);
        cancelAllJobs(true);
    }

    public void cancelAllJobs() {
        cancelAllJobs(false);
    }

    private void cancelAllJobs(boolean waitUntilSlotsAreFreed) {
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

            if (waitUntilSlotsAreFreed) {
                CommonTestUtils.waitUntilCondition(
                        () -> {
                            final ResourceOverview resourceOverview =
                                    miniCluster.getResourceOverview().get();
                            return resourceOverview.getNumberRegisteredSlots()
                                    == resourceOverview.getNumberFreeSlots();
                        });
            }
        } catch (Exception e) {
            log.warn("Exception while shutting down remaining jobs.", e);
        }
    }

    @Override
    public void after() {
        Exception exception = null;

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
    }

    private void startMiniCluster() throws Exception {
        final Configuration configuration =
                new Configuration(miniClusterResourceConfiguration.getConfiguration());
        configuration.setString(
                CoreOptions.TMP_DIRS, temporaryFolder.newFolder().getAbsolutePath());
        if (!configuration.contains(CheckpointingOptions.CHECKPOINTS_DIRECTORY)) {
            // The channel state or checkpoint file may exceed the upper limit of
            // JobManagerCheckpointStorage, so use FileSystemCheckpointStorage as
            // the default checkpoint storage for all tests.
            configuration.set(
                    CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                    temporaryFolder.newFolder().toURI().toString());
        }

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
        if (!(configuration.contains(RestOptions.BIND_PORT)
                || configuration.contains(RestOptions.PORT))) {
            configuration.setString(RestOptions.BIND_PORT, "0");
        }

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
}
