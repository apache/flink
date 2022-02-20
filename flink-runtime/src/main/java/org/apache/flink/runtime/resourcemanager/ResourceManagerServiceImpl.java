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
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
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

    private final ExecutorService handleLeaderEventExecutor;
    private final CompletableFuture<Void> serviceTerminationFuture;

    private final Object lock = new Object();

    @GuardedBy("lock")
    private boolean running;

    @Nullable
    @GuardedBy("lock")
    private ResourceManager<?> leaderResourceManager;

    @Nullable
    @GuardedBy("lock")
    private UUID leaderSessionID;

    @GuardedBy("lock")
    private CompletableFuture<Void> previousResourceManagerTerminationFuture;

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
        this.serviceTerminationFuture = new CompletableFuture<>();

        this.running = false;
        this.leaderResourceManager = null;
        this.leaderSessionID = null;
        this.previousResourceManagerTerminationFuture = FutureUtils.completedVoidFuture();
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
        return serviceTerminationFuture;
    }

    @Override
    public CompletableFuture<Void> deregisterApplication(
            final ApplicationStatus applicationStatus, final @Nullable String diagnostics) {

        synchronized (lock) {
            if (!running || leaderResourceManager == null) {
                return deregisterWithoutLeaderRm();
            }

            final ResourceManager<?> currentLeaderRM = leaderResourceManager;
            return currentLeaderRM
                    .getStartedFuture()
                    .thenCompose(
                            ignore -> {
                                synchronized (lock) {
                                    if (isLeader(currentLeaderRM)) {
                                        return currentLeaderRM
                                                .getSelfGateway(ResourceManagerGateway.class)
                                                .deregisterApplication(
                                                        applicationStatus, diagnostics)
                                                .thenApply(ack -> null);
                                    } else {
                                        return deregisterWithoutLeaderRm();
                                    }
                                }
                            });
        }
    }

    private static CompletableFuture<Void> deregisterWithoutLeaderRm() {
        LOG.warn("Cannot deregister application. Resource manager service is not available.");
        return FutureUtils.completedVoidFuture();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        synchronized (lock) {
            if (running) {
                LOG.info("Stopping resource manager service.");
                running = false;
                stopLeaderElectionService();
                stopLeaderResourceManager();
            } else {
                LOG.debug("Resource manager service is not running.");
            }

            FutureUtils.forward(previousResourceManagerTerminationFuture, serviceTerminationFuture);
        }

        handleLeaderEventExecutor.shutdownNow();

        return serviceTerminationFuture;
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

                        try {
                            startNewLeaderResourceManager(newLeaderSessionID);
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

                        stopLeaderResourceManager();

                        if (!resourceManagerFactory.supportMultiLeaderSession()) {
                            closeAsync();
                        }
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

    // ------------------------------------------------------------------------
    //  Internal
    // ------------------------------------------------------------------------

    @GuardedBy("lock")
    private void startNewLeaderResourceManager(UUID newLeaderSessionID) throws Exception {
        stopLeaderResourceManager();

        this.leaderSessionID = newLeaderSessionID;
        this.leaderResourceManager =
                resourceManagerFactory.createResourceManager(
                        rmProcessContext, newLeaderSessionID, ResourceID.generate());

        final ResourceManager<?> newLeaderResourceManager = this.leaderResourceManager;

        previousResourceManagerTerminationFuture
                .thenComposeAsync(
                        (ignore) -> {
                            synchronized (lock) {
                                return startResourceManagerIfIsLeader(newLeaderResourceManager);
                            }
                        },
                        handleLeaderEventExecutor)
                .thenAcceptAsync(
                        (isStillLeader) -> {
                            if (isStillLeader) {
                                leaderElectionService.confirmLeadership(
                                        newLeaderSessionID, newLeaderResourceManager.getAddress());
                            }
                        },
                        ioExecutor);
    }

    /**
     * Returns a future that completes as {@code true} if the resource manager is still leader and
     * started, and {@code false} if it's no longer leader.
     */
    @GuardedBy("lock")
    private CompletableFuture<Boolean> startResourceManagerIfIsLeader(
            ResourceManager<?> resourceManager) {
        if (isLeader(resourceManager)) {
            resourceManager.start();
            forwardTerminationFuture(resourceManager);
            return resourceManager.getStartedFuture().thenApply(ignore -> true);
        } else {
            return CompletableFuture.completedFuture(false);
        }
    }

    private void forwardTerminationFuture(ResourceManager<?> resourceManager) {
        resourceManager
                .getTerminationFuture()
                .whenComplete(
                        (ignore, throwable) -> {
                            synchronized (lock) {
                                if (isLeader(resourceManager)) {
                                    if (throwable != null) {
                                        serviceTerminationFuture.completeExceptionally(throwable);
                                    } else {
                                        serviceTerminationFuture.complete(null);
                                    }
                                }
                            }
                        });
    }

    @GuardedBy("lock")
    private boolean isLeader(ResourceManager<?> resourceManager) {
        return running && this.leaderResourceManager == resourceManager;
    }

    @GuardedBy("lock")
    private void stopLeaderResourceManager() {
        if (leaderResourceManager != null) {
            previousResourceManagerTerminationFuture =
                    previousResourceManagerTerminationFuture.thenCombine(
                            leaderResourceManager.closeAsync(), (ignore1, ignore2) -> null);
            leaderResourceManager = null;
            leaderSessionID = null;
        }
    }

    private void stopLeaderElectionService() {
        try {
            leaderElectionService.stop();
        } catch (Exception e) {
            serviceTerminationFuture.completeExceptionally(
                    new FlinkException("Cannot stop leader election service.", e));
        }
    }

    @VisibleForTesting
    @Nullable
    public ResourceManager<?> getLeaderResourceManager() {
        synchronized (lock) {
            return leaderResourceManager;
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
