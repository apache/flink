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

package org.apache.flink.container.entrypoint;

import org.apache.flink.annotation.Internal;
import org.apache.flink.client.deployment.application.ApplicationClusterEntryPoint;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.core.security.FlinkSecurityManager;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.ClusterEntrypointUtils;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.security.contexts.SecurityContext;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Entry point that runs a single application in a {@link MiniCluster}. It takes the same command
 * line parameters as {@link StandaloneApplicationClusterEntryPoint}, and the MiniCluster is built
 * with the same application-mode dispatcher.
 */
@Internal
public final class MiniClusterJobEntryPoint {

    private static final Logger LOG = LoggerFactory.getLogger(MiniClusterJobEntryPoint.class);

    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(30);

    /** The safeguard's default, as used by the other cluster entrypoints. */
    private static final Duration JVM_SHUTDOWN_SAFEGUARD_DELAY = Duration.ofSeconds(5);

    /** Shorter than the safeguard delay, so that a slow close is logged before the JVM halts. */
    private static final Duration SHUTDOWN_HOOK_TIMEOUT = Duration.ofSeconds(4);

    private static final AtomicBoolean STOPPING = new AtomicBoolean();

    public static void main(String[] args) {
        EnvironmentInformation.logEnvironmentInfo(
                LOG, MiniClusterJobEntryPoint.class.getSimpleName(), args);
        SignalHandler.register(LOG);
        JvmShutdownSafeguard.installAsShutdownHook(LOG, JVM_SHUTDOWN_SAFEGUARD_DELAY.toMillis());

        final StandaloneApplicationClusterConfiguration clusterConfiguration =
                ClusterEntrypointUtils.parseParametersOrExit(
                        args,
                        new StandaloneApplicationClusterConfigurationParserFactory(),
                        MiniClusterJobEntryPoint.class);

        int exitCode;
        try {
            final Configuration configuration =
                    ApplicationClusterEntryPointUtils.loadConfiguration(clusterConfiguration);
            FlinkSecurityManager.setFromConfiguration(configuration);
            final PluginManager pluginManager =
                    PluginUtils.createPluginManagerFromRootFolder(configuration);
            FileSystem.initialize(configuration, pluginManager);

            final SecurityContext securityContext =
                    ClusterEntrypoint.installSecurityContext(configuration);
            ClusterEntrypointUtils.configureUncaughtExceptionHandler(configuration);
            exitCode =
                    securityContext.runSecured(
                            () ->
                                    runApplication(
                                            clusterConfiguration, configuration, pluginManager));
        } catch (Throwable t) {
            LOG.error("Could not run the application on a MiniCluster.", t);
            exitCode = 1;
        }
        System.exit(exitCode);
    }

    private static int runApplication(
            final StandaloneApplicationClusterConfiguration clusterConfiguration,
            final Configuration configuration,
            final PluginManager pluginManager)
            throws Exception {
        final PackagedProgram program =
                ApplicationClusterEntryPointUtils.getPackagedProgram(
                        clusterConfiguration, configuration);
        ApplicationClusterEntryPoint.configureExecution(configuration, program);

        final ApplicationModeMiniCluster miniCluster =
                new ApplicationModeMiniCluster(
                        buildMiniClusterConfiguration(configuration, pluginManager), program);
        miniCluster.start();
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    if (STOPPING.compareAndSet(false, true)) {
                                        closeQuietly(miniCluster, SHUTDOWN_HOOK_TIMEOUT);
                                    }
                                }));

        CompletableFuture.anyOf(
                        miniCluster.getApplicationShutDownFuture(),
                        miniCluster.getCloseRequestedFuture())
                .handle((ignored, throwable) -> null)
                .join();

        // only one of this and the shutdown hook closes the MiniCluster, so that a close that
        // times out here isn't waited for again during JVM shutdown
        if (!STOPPING.compareAndSet(false, true)) {
            return 0;
        }

        closeQuietly(miniCluster, SHUTDOWN_TIMEOUT);
        program.close();

        final CompletableFuture<ApplicationStatus> applicationShutDown =
                miniCluster.getApplicationShutDownFuture();
        if (!applicationShutDown.isDone() || applicationShutDown.isCompletedExceptionally()) {
            LOG.error("MiniCluster shut down before the application finished.");
            return 1;
        }
        final ApplicationStatus applicationStatus = applicationShutDown.join();
        LOG.info("Application finished with status {}.", applicationStatus);
        return applicationStatus.processExitCode();
    }

    private static MiniClusterConfiguration buildMiniClusterConfiguration(
            final Configuration configuration, final PluginManager pluginManager) {
        return new MiniClusterConfiguration.Builder()
                .setConfiguration(configuration)
                .setNumSlotsPerTaskManager(configuration.get(TaskManagerOptions.NUM_TASK_SLOTS))
                .setPluginManager(pluginManager)
                .build();
    }

    private static void closeQuietly(final MiniCluster miniCluster, final Duration timeout) {
        try {
            miniCluster.closeAsync().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            LOG.warn("Error closing MiniCluster during shutdown.", e);
        }
    }

    private MiniClusterJobEntryPoint() {}
}
