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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.FlinkException;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Default implementation of {@link ResourceManagerService}. */
public class ResourceManagerServiceImpl implements ResourceManagerService {

    private final ResourceManagerFactory<?> resourceManagerFactory;
    private final ResourceManagerProcessContext rmProcessContext;

    private final CompletableFuture<Void> terminationFuture;

    private final Object lock = new Object();

    @Nullable
    @GuardedBy("lock")
    private ResourceManager<?> resourceManager;

    private ResourceManagerServiceImpl(
            ResourceManagerFactory<?> resourceManagerFactory,
            ResourceManagerProcessContext rmProcessContext) {
        this.resourceManagerFactory = checkNotNull(resourceManagerFactory);
        this.rmProcessContext = checkNotNull(rmProcessContext);

        this.terminationFuture = new CompletableFuture<>();

        this.resourceManager = null;
    }

    @Override
    public void start() throws Exception {
        synchronized (lock) {
            resourceManager =
                    resourceManagerFactory.createResourceManager(
                            rmProcessContext, ResourceID.generate());

            FutureUtils.forward(resourceManager.getTerminationFuture(), terminationFuture);

            resourceManager.start();
        }
    }

    @Override
    public CompletableFuture<Void> getTerminationFuture() {
        return terminationFuture;
    }

    @Override
    public CompletableFuture<Void> deregisterApplication(
            final ApplicationStatus applicationStatus, final @Nullable String diagnostics) {
        synchronized (lock) {
            if (resourceManager != null) {
                return resourceManager
                        .getSelfGateway(ResourceManagerGateway.class)
                        .deregisterApplication(applicationStatus, diagnostics)
                        .thenApply(ack -> null);
            } else {
                return FutureUtils.completedExceptionally(
                        new FlinkException(
                                "Cannot deregister application. Resource manager service is not started."));
            }
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        synchronized (lock) {
            if (resourceManager != null) {
                resourceManager.closeAsync();
            } else {
                // resource manager is never started
                terminationFuture.complete(null);
            }
        }

        return terminationFuture;
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
