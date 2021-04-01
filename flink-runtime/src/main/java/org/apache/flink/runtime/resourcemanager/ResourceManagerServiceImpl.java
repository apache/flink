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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Default implementation of {@link ResourceManagerService}. */
public class ResourceManagerServiceImpl implements ResourceManagerService, LeaderContender {

    private static final Logger LOG = LoggerFactory.getLogger(ResourceManagerServiceImpl.class);

    private final ResourceManagerFactory<?> resourceManagerFactory;
    private final ResourceManagerProcessContext rmProcessContext;

    private final LeaderElectionService leaderElectionService;
    private final FatalErrorHandler fatalErrorHandler;
    private final Executor ioExecutor;

    private final Executor handleLeaderEventExecutor;
    private final CompletableFuture<Void> terminationFuture;

    private final Object lock = new Object();

    @GuardedBy("lock")
    private boolean running;

    @Nullable
    @GuardedBy("lock")
    private ResourceManager<?> resourceManager;

    @Nullable
    @GuardedBy("lock")
    private UUID leaderSessionID;

    private ResourceManagerServiceImpl(
            ResourceManagerFactory<?> resourceManagerFactory,
            ResourceManagerProcessContext rmProcessContext) {
        this.resourceManagerFactory = checkNotNull(resourceManagerFactory);
        this.rmProcessContext = checkNotNull(rmProcessContext);

        this.leaderElectionService =
                rmProcessContext
                        .getHighAvailabilityServices()
                        .getResourceManagerLeaderElectionService();
        this.fatalErrorHandler = rmProcessContext.getFatalErrorHandler();
        this.ioExecutor = rmProcessContext.getIoExecutor();

        this.handleLeaderEventExecutor = Executors.newSingleThreadExecutor();
        this.terminationFuture = new CompletableFuture<>();

        this.running = false;
        this.resourceManager = null;
        this.leaderSessionID = null;
    }

    // ------------------------------------------------------------------------
    //  ResourceManagerService
    // ------------------------------------------------------------------------

    @Override
    public void start() throws Exception {
        synchronized (lock) {
            if (running) {
                LOG.debug("Resource manager service has already started.");
                return;
            }
            running = true;
        }

        LOG.info("Starting resource manager service.");

        leaderElectionService.start(this);
    }

    @Override
    public CompletableFuture<Void> getTerminationFuture() {
        return terminationFuture;
    }

    @Override
    public CompletableFuture<Void> deregisterApplication(
            final ApplicationStatus applicationStatus, final @Nullable String diagnostics) {
        synchronized (lock) {
            if (running && resourceManager != null) {
                return resourceManager
                        .getSelfGateway(ResourceManagerGateway.class)
                        .deregisterApplication(applicationStatus, diagnostics)
                        .thenApply(ack -> null);
            } else {
                return FutureUtils.completedExceptionally(
                        new FlinkException(
                                "Cannot deregister application. Resource manager service is not available."));
            }
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        synchronized (lock) {
            if (running) {
                LOG.info("Stopping resource manager service.");
                running = false;
                try {
                    leaderElectionService.stop();
                } catch (Exception e) {
                    terminationFuture.completeExceptionally(
                            new FlinkException("Cannot stop leader election service.", e));
                }
            } else {
                LOG.debug("Resource manager service is not running.");
            }

            if (resourceManager != null) {
                resourceManager.closeAsync();
            } else {
                // resource manager is never started
                terminationFuture.complete(null);
            }
        }

        return terminationFuture;
    }

    // ------------------------------------------------------------------------
    //  LeaderContender
    // ------------------------------------------------------------------------

    @Override
    public void grantLeadership(UUID newLeaderSessionID) {
        handleLeaderEventExecutor.execute(
                () -> {
                    synchronized (lock) {
                        if (!running) {
                            LOG.info(
                                    "Resource manager service is not running. Ignore granting leadership with session ID {}.",
                                    newLeaderSessionID);
                            return;
                        }

                        LOG.info(
                                "Resource manager service is granted leadership with session id {}.",
                                newLeaderSessionID);

                        this.leaderSessionID = newLeaderSessionID;

                        try {
                            resourceManager =
                                    resourceManagerFactory.createResourceManager(
                                            rmProcessContext,
                                            newLeaderSessionID,
                                            ResourceID.generate());
                            FutureUtils.forward(
                                    resourceManager.getTerminationFuture(), terminationFuture);

                            resourceManager.start();
                            resourceManager.getStartedFuture().get();

                            ioExecutor.execute(
                                    () ->
                                            leaderElectionService.confirmLeadership(
                                                    newLeaderSessionID,
                                                    resourceManager.getAddress()));
                        } catch (Throwable t) {
                            fatalErrorHandler.onFatalError(
                                    new FlinkException("Cannot start resource manager.", t));
                        }
                    }
                });
    }

    @Override
    public void revokeLeadership() {
        handleLeaderEventExecutor.execute(
                () -> {
                    synchronized (lock) {
                        if (!running) {
                            LOG.info(
                                    "Resource manager service is not running. Ignore revoking leadership.");
                            return;
                        }

                        LOG.info(
                                "Resource manager service is revoked leadership with session id {}.",
                                leaderSessionID);

                        closeAsync();
                    }
                });
    }

    @Override
    public void handleError(Exception exception) {
        fatalErrorHandler.onFatalError(
                new FlinkException(
                        "Exception during leader election of resource manager occurred.",
                        exception));
    }

    @VisibleForTesting
    @Nullable
    public ResourceManager<?> getResourceManager() {
        synchronized (lock) {
            return resourceManager;
        }
    }

    public static ResourceManagerServiceImpl create(
            ResourceManagerFactory<?> resourceManagerFactory,
            Configuration configuration,
            RpcService rpcService,
            HighAvailabilityServices highAvailabilityServices,
            HeartbeatServices heartbeatServices,
            FatalErrorHandler fatalErrorHandler,
            ClusterInformation clusterInformation,
            @Nullable String webInterfaceUrl,
            MetricRegistry metricRegistry,
            String hostname,
            Executor ioExecutor)
            throws ConfigurationException {

        return new ResourceManagerServiceImpl(
                resourceManagerFactory,
                resourceManagerFactory.createResourceManagerProcessContext(
                        configuration,
                        rpcService,
                        highAvailabilityServices,
                        heartbeatServices,
                        fatalErrorHandler,
                        clusterInformation,
                        webInterfaceUrl,
                        metricRegistry,
                        hostname,
                        ioExecutor));
    }
}
