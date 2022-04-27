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
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;

import javax.annotation.Nullable;

import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class contains information and services needed for creating a {@link
 * org.apache.flink.runtime.resourcemanager.ResourceManager}, which do not change during the
 * lifetime of the process and can be reused between multiple resource manager instances in the
 * process.
 */
public class ResourceManagerProcessContext {
    private final Configuration rmConfig;
    private final ResourceManagerRuntimeServicesConfiguration rmRuntimeServicesConfig;
    private final RpcService rpcService;
    private final HighAvailabilityServices highAvailabilityServices;
    private final HeartbeatServices heartbeatServices;
    private final FatalErrorHandler fatalErrorHandler;
    private final ClusterInformation clusterInformation;
    @Nullable private final String webInterfaceUrl;
    private final MetricRegistry metricRegistry;
    private final String hostname;
    private final Executor ioExecutor;

    public ResourceManagerProcessContext(
            Configuration rmConfig,
            ResourceManagerRuntimeServicesConfiguration rmRuntimeServicesConfig,
            RpcService rpcService,
            HighAvailabilityServices highAvailabilityServices,
            HeartbeatServices heartbeatServices,
            FatalErrorHandler fatalErrorHandler,
            ClusterInformation clusterInformation,
            @Nullable String webInterfaceUrl,
            MetricRegistry metricRegistry,
            String hostname,
            Executor ioExecutor) {
        this.rmConfig = checkNotNull(rmConfig);
        this.rmRuntimeServicesConfig = checkNotNull(rmRuntimeServicesConfig);
        this.rpcService = checkNotNull(rpcService);
        this.highAvailabilityServices = checkNotNull(highAvailabilityServices);
        this.heartbeatServices = checkNotNull(heartbeatServices);
        this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
        this.clusterInformation = checkNotNull(clusterInformation);
        this.metricRegistry = checkNotNull(metricRegistry);
        this.hostname = checkNotNull(hostname);
        this.ioExecutor = checkNotNull(ioExecutor);

        this.webInterfaceUrl = webInterfaceUrl;
    }

    public Configuration getRmConfig() {
        return rmConfig;
    }

    public ResourceManagerRuntimeServicesConfiguration getRmRuntimeServicesConfig() {
        return rmRuntimeServicesConfig;
    }

    public RpcService getRpcService() {
        return rpcService;
    }

    public HighAvailabilityServices getHighAvailabilityServices() {
        return highAvailabilityServices;
    }

    public HeartbeatServices getHeartbeatServices() {
        return heartbeatServices;
    }

    public FatalErrorHandler getFatalErrorHandler() {
        return fatalErrorHandler;
    }

    public ClusterInformation getClusterInformation() {
        return clusterInformation;
    }

    @Nullable
    public String getWebInterfaceUrl() {
        return webInterfaceUrl;
    }

    public MetricRegistry getMetricRegistry() {
        return metricRegistry;
    }

    public String getHostname() {
        return hostname;
    }

    public Executor getIoExecutor() {
        return ioExecutor;
    }
}
