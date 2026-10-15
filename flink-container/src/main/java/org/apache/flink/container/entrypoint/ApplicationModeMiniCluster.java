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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.client.deployment.application.ApplicationDispatcherLeaderProcessFactoryFactory;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.security.FlinkSecurityManager;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.dispatcher.SessionDispatcherFactory;
import org.apache.flink.runtime.dispatcher.runner.DefaultDispatcherRunnerFactory;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponent;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.externalresource.ExternalResourceUtils;
import org.apache.flink.runtime.failure.FailureEnricherUtils;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.resourcemanager.StandaloneResourceManagerFactory;
import org.apache.flink.runtime.rest.ApplicationRestEndpointFactory;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.runtime.security.token.DelegationTokenManager;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link MiniCluster} that runs a single application in the same way as a distributed application
 * cluster. The dispatcher is built with the user's program and starts the application itself once
 * it has leadership, recovering from high availability metadata where available.
 */
@Internal
class ApplicationModeMiniCluster extends MiniCluster {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationModeMiniCluster.class);

    private final Configuration configuration;
    private final PackagedProgram program;
    @Nullable private final PluginManager pluginManager;

    private final CompletableFuture<ApplicationStatus> applicationShutDownFuture =
            new CompletableFuture<>();
    private final CompletableFuture<Void> closeRequestedFuture = new CompletableFuture<>();
    private final Collection<DispatcherResourceManagerComponent>
            dispatcherResourceManagerComponents = new CopyOnWriteArrayList<>();

    /** The close that is under way, once one has started while the MiniCluster was running. */
    private final AtomicReference<CompletableFuture<Void>> closeFuture = new AtomicReference<>();

    ApplicationModeMiniCluster(
            final MiniClusterConfiguration miniClusterConfiguration,
            final PackagedProgram program) {
        super(miniClusterConfiguration);
        this.configuration = new Configuration(miniClusterConfiguration.getConfiguration());
        this.program = checkNotNull(program);
        this.pluginManager = miniClusterConfiguration.getPluginManager();
    }

    CompletableFuture<ApplicationStatus> getApplicationShutDownFuture() {
        return applicationShutDownFuture;
    }

    CompletableFuture<Void> getCloseRequestedFuture() {
        return closeRequestedFuture;
    }

    @Override
    protected DispatcherResourceManagerComponentFactory
            createDispatcherResourceManagerComponentFactory() {
        return new DefaultDispatcherResourceManagerComponentFactory(
                new DefaultDispatcherRunnerFactory(
                        ApplicationDispatcherLeaderProcessFactoryFactory.create(
                                configuration, SessionDispatcherFactory.INSTANCE, program)),
                StandaloneResourceManagerFactory.getInstance(),
                ApplicationRestEndpointFactory.INSTANCE);
    }

    @Override
    protected Collection<? extends DispatcherResourceManagerComponent>
            createDispatcherResourceManagerComponents(
                    Configuration configuration,
                    RpcServiceFactory rpcServiceFactory,
                    BlobServer blobServer,
                    HeartbeatServices heartbeatServices,
                    DelegationTokenManager delegationTokenManager,
                    MetricRegistry metricRegistry,
                    MetricQueryServiceRetriever metricQueryServiceRetriever,
                    FatalErrorHandler fatalErrorHandler)
                    throws Exception {
        final Collection<? extends DispatcherResourceManagerComponent> components =
                super.createDispatcherResourceManagerComponents(
                        configuration,
                        rpcServiceFactory,
                        blobServer,
                        heartbeatServices,
                        delegationTokenManager,
                        metricRegistry,
                        metricQueryServiceRetriever,
                        fatalErrorHandler);
        for (DispatcherResourceManagerComponent component : components) {
            FutureUtils.forward(component.getShutDownFuture(), applicationShutDownFuture);
        }
        dispatcherResourceManagerComponents.addAll(components);
        return components;
    }

    @Override
    protected FatalErrorHandler createJobManagerFatalErrorHandler() {
        return createProcessFatalErrorHandler("JobManager");
    }

    @Override
    protected FatalErrorHandler createTaskManagerFatalErrorHandler(int index) {
        return createProcessFatalErrorHandler("TaskManager #" + index);
    }

    /**
     * Nothing replaces a JobManager or TaskManager that fails, so the whole process has to fail
     * instead, like their processes do in a distributed cluster. That leaves the restart to the
     * process supervisor, with high availability recovering the job.
     */
    private FatalErrorHandler createProcessFatalErrorHandler(String component) {
        return exception -> {
            if (closeRequestedFuture.isDone()) {
                LOG.debug("Ignoring {} error during shutdown.", component, exception);
                return;
            }
            LOG.error("{} failed. Shutting the MiniCluster down.", component, exception);
            if (ExceptionUtils.isJvmFatalOrOutOfMemoryError(exception)) {
                LOG.error(
                        "Terminating the process with exit code {}.",
                        ClusterEntrypoint.RUNTIME_FAILURE_RETURN_CODE);
                FlinkSecurityManager.forceProcessExit(
                        ClusterEntrypoint.RUNTIME_FAILURE_RETURN_CODE);
            } else {
                closeAsync();
            }
        };
    }

    @Override
    protected Collection<FailureEnricher> createFailureEnrichers(Configuration configuration) {
        return FailureEnricherUtils.getFailureEnrichers(configuration);
    }

    @Override
    protected ExternalResourceInfoProvider createExternalResourceInfoProvider(
            Configuration configuration) {
        return ExternalResourceUtils.createStaticExternalResourceInfoProviderFromConfig(
                configuration, pluginManager);
    }

    /** Sizes the RPC thread pool from the {@code pekko.fork-join-executor} options. */
    @Override
    protected RpcService createLocalRpcService(Configuration configuration, RpcSystem rpcSystem)
            throws Exception {
        return rpcSystem.localServiceBuilder(configuration).createAndStart();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        closeRequestedFuture.complete(null);
        // checked first, as a close is recorded before it stops the MiniCluster running
        final boolean running = isRunning();
        final CompletableFuture<Void> closeUnderWay = closeFuture.get();
        if (closeUnderWay != null) {
            return closeUnderWay;
        }
        if (!running) {
            // Nothing more to close while it is not running. A failed start calls this while
            // holding the lock that closing it waits for, so returning now to avoid deadlock
            return stopDispatcher();
        }
        final CompletableFuture<Void> newCloseFuture = new CompletableFuture<>();
        if (!closeFuture.compareAndSet(null, newCloseFuture)) {
            return closeFuture.get();
        }
        // the cluster's high availability data is only removed if the dispatcher has shut down,
        // which it does when the application finishes if execution.shutdown-on-application-finish
        // is set. Otherwise, the data is kept so that a restarted process can recover.
        final boolean cleanupHighAvailabilityData = isDispatcherShutDown();
        FutureUtils.forward(
                FutureUtils.composeAfterwards(
                        // stop the dispatcher before the MiniCluster stops TaskManagers so it
                        // suspends jobs first
                        stopDispatcher(),
                        cleanupHighAvailabilityData
                                ? super::closeAsync
                                : super::closeAsyncWithoutCleaningHighAvailabilityData),
                newCloseFuture);
        return newCloseFuture;
    }

    private CompletableFuture<Void> stopDispatcher() {
        return FutureUtils.completeAll(
                dispatcherResourceManagerComponents.stream()
                        .map(DispatcherResourceManagerComponent::stopProcess)
                        .collect(Collectors.toList()));
    }

    @VisibleForTesting
    boolean isDispatcherShutDown() {
        return !dispatcherResourceManagerComponents.isEmpty()
                && dispatcherResourceManagerComponents.stream()
                        .map(DispatcherResourceManagerComponent::getShutDownFuture)
                        .allMatch(
                                shutDownFuture ->
                                        shutDownFuture.isDone()
                                                && !shutDownFuture.isCompletedExceptionally());
    }
}
