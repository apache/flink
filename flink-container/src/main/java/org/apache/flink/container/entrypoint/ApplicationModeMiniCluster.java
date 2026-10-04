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
import org.apache.flink.client.deployment.application.ApplicationDispatcherLeaderProcessFactoryFactory;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.security.FlinkSecurityManager;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.dispatcher.SessionDispatcherFactory;
import org.apache.flink.runtime.dispatcher.runner.DefaultDispatcherRunnerFactory;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponent;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.resourcemanager.StandaloneResourceManagerFactory;
import org.apache.flink.runtime.rest.ApplicationRestEndpointFactory;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.security.token.DelegationTokenManager;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link MiniCluster} that runs a single application in the same way as a distributed application
 * cluster. The dispatcher is built with the user's program and starts the application itself once
 * it has leadership, recovering from high availability metadata where available.
 */
@Internal
final class ApplicationModeMiniCluster extends MiniCluster {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationModeMiniCluster.class);

    private final Configuration configuration;
    private final PackagedProgram program;

    private final CompletableFuture<ApplicationStatus> applicationShutDownFuture =
            new CompletableFuture<>();
    private final CompletableFuture<Void> closeRequestedFuture = new CompletableFuture<>();

    ApplicationModeMiniCluster(
            final MiniClusterConfiguration miniClusterConfiguration,
            final PackagedProgram program) {
        super(miniClusterConfiguration);
        this.configuration = new Configuration(miniClusterConfiguration.getConfiguration());
        this.program = checkNotNull(program);
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
        return components;
    }

    /**
     * Nothing replaces a TaskManager that fails, so the whole process has to fail instead, like a
     * TaskManager process does in a distributed cluster. That leaves the restart to the process
     * supervisor, with high availability recovering the job.
     */
    @Override
    protected FatalErrorHandler createTaskManagerFatalErrorHandler(int index) {
        return exception -> {
            if (closeRequestedFuture.isDone()) {
                LOG.debug("Ignoring TaskManager #{} error during shutdown.", index, exception);
                return;
            }
            LOG.error("TaskManager #{} failed. Shutting the MiniCluster down.", index, exception);
            if (ExceptionUtils.isJvmFatalOrOutOfMemoryError(exception)) {
                FlinkSecurityManager.forceProcessExit(1);
            } else {
                closeAsync();
            }
        };
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        closeRequestedFuture.complete(null);
        return super.closeAsyncWithoutCleaningHighAvailabilityData();
    }
}
