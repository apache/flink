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
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JMXServerOptions;
import org.apache.flink.configuration.RpcOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.core.security.FlinkSecurityManager;
import org.apache.flink.management.jmx.JMXService;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.ClusterEntrypointUtils;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.security.contexts.SecurityContext;
import org.apache.flink.runtime.state.changelog.StateChangelogStorageLoader;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.ShutdownHookUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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

    /** Completed once whichever of the main thread and the shutdown hook is stopping is done. */
    private static final CompletableFuture<Void> STOPPED = new CompletableFuture<>();

    /** Whether the JVM is exiting for another reason, which decides its exit code. */
    private static volatile boolean closedByShutdownHook;

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
            StateChangelogStorageLoader.initialize(pluginManager);

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
            exitCode = ClusterEntrypoint.STARTUP_FAILURE_RETURN_CODE;
        }
        if (closedByShutdownHook) {
            return;
        }
        LOG.info(
                "Terminating {} with exit code {}.",
                MiniClusterJobEntryPoint.class.getSimpleName(),
                exitCode);
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
        final File webTmpDir =
                new File(configuration.get(WebOptions.TMP_DIR), "flink-web-" + UUID.randomUUID());
        final ApplicationModeMiniCluster miniCluster;
        try {
            ApplicationClusterEntryPoint.configureExecution(configuration, program);

            miniCluster =
                    new ApplicationModeMiniCluster(
                            buildMiniClusterConfiguration(configuration, pluginManager, webTmpDir),
                            program);
            final String serviceName = MiniClusterJobEntryPoint.class.getSimpleName();
            ShutdownHookUtil.addShutdownHookThread(
                    new Thread(
                            () -> {
                                if (STOPPING.compareAndSet(false, true)) {
                                    closedByShutdownHook = true;
                                    tryClose(miniCluster, webTmpDir, SHUTDOWN_HOOK_TIMEOUT);
                                    program.close();
                                } else {
                                    // the JVM halts once the hooks finish, so wait for the
                                    // main thread to finish stopping
                                    awaitStopped();
                                }
                            },
                            serviceName + " shutdown hook"),
                    serviceName,
                    LOG);
            JMXService.startInstance(configuration.get(JMXServerOptions.JMX_SERVER_PORT));
            miniCluster.start();
        } catch (Throwable t) {
            if (STOPPING.compareAndSet(false, true)) {
                tryDeleteWebTmpDir(webTmpDir);
                program.close();
                STOPPED.complete(null);
            }
            throw t;
        }

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

        final boolean closed;
        try {
            closed = tryClose(miniCluster, webTmpDir, SHUTDOWN_TIMEOUT);
            program.close();
        } finally {
            STOPPED.complete(null);
        }

        final CompletableFuture<ApplicationStatus> applicationShutDown =
                miniCluster.getApplicationShutDownFuture();
        if (!applicationShutDown.isDone() || applicationShutDown.isCompletedExceptionally()) {
            LOG.error("MiniCluster shut down before the application finished.");
            return ClusterEntrypoint.RUNTIME_FAILURE_RETURN_CODE;
        }
        final ApplicationStatus applicationStatus = applicationShutDown.join();
        LOG.info("Application finished with status {}.", applicationStatus);
        // report a failed shut down (e.g. not removing the HA data) over application's status
        return closed
                ? applicationStatus.processExitCode()
                : ClusterEntrypoint.RUNTIME_FAILURE_RETURN_CODE;
    }

    private static MiniClusterConfiguration buildMiniClusterConfiguration(
            final Configuration configuration,
            final PluginManager pluginManager,
            final File webTmpDir) {
        final Configuration clusterConfiguration = new Configuration(configuration);
        clusterConfiguration.set(WebOptions.TMP_DIR, webTmpDir.getAbsolutePath());
        if (!clusterConfiguration.contains(RpcOptions.ASK_TIMEOUT_DURATION)) {
            clusterConfiguration.set(
                    RpcOptions.ASK_TIMEOUT_DURATION,
                    RpcOptions.ASK_TIMEOUT_DURATION.defaultValue());
        }
        if (!clusterConfiguration.contains(ClusterOptions.CLUSTER_IO_EXECUTOR_POOL_SIZE)) {
            clusterConfiguration.set(
                    ClusterOptions.CLUSTER_IO_EXECUTOR_POOL_SIZE,
                    ClusterEntrypointUtils.getPoolSize(clusterConfiguration));
        }
        return new MiniClusterConfiguration.Builder()
                .setConfiguration(clusterConfiguration)
                .setNumSlotsPerTaskManager(configuration.get(TaskManagerOptions.NUM_TASK_SLOTS))
                .setPluginManager(pluginManager)
                .build();
    }

    /**
     * Returns whether the MiniCluster closed within the timeout and its web directory was removed.
     */
    private static boolean tryClose(
            final MiniCluster miniCluster, final File webTmpDir, final Duration timeout) {
        boolean closed = true;
        try {
            miniCluster.closeAsync().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            LOG.error("Could not close the MiniCluster.", e);
            closed = false;
        }
        return tryDeleteWebTmpDir(webTmpDir) && closed;
    }

    private static boolean tryDeleteWebTmpDir(final File webTmpDir) {
        try {
            FileUtils.deleteDirectory(webTmpDir);
            return true;
        } catch (IOException e) {
            LOG.error("Could not remove the web directory {}.", webTmpDir, e);
            return false;
        }
    }

    private static void awaitStopped() {
        try {
            STOPPED.get(SHUTDOWN_HOOK_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            LOG.warn("Stopping did not finish within {}.", SHUTDOWN_HOOK_TIMEOUT);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            // never completed exceptionally
        }
    }

    private MiniClusterJobEntryPoint() {}
}
