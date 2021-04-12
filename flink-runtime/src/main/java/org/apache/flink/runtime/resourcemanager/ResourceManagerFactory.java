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
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.SlotManagerMetricGroup;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.ConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.concurrent.Executor;

/**
 * {@link ResourceManager} factory.
 *
 * @param <T> type of the workers of the ResourceManager
 */
public abstract class ResourceManagerFactory<T extends ResourceIDRetrievable> {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    public ResourceManager<T> createResourceManager(
            Configuration configuration,
            ResourceID resourceId,
            RpcService rpcService,
            HighAvailabilityServices highAvailabilityServices,
            HeartbeatServices heartbeatServices,
            FatalErrorHandler fatalErrorHandler,
            ClusterInformation clusterInformation,
            @Nullable String webInterfaceUrl,
            MetricRegistry metricRegistry,
            String hostname,
            Executor ioExecutor)
            throws Exception {

        final Configuration effectiveResourceManagerAndRuntimeServicesConfig =
                getEffectiveConfigurationForResourceManagerAndRuntimeServices(configuration);

        final ResourceManagerMetricGroup resourceManagerMetricGroup =
                ResourceManagerMetricGroup.create(metricRegistry, hostname);
        final SlotManagerMetricGroup slotManagerMetricGroup =
                SlotManagerMetricGroup.create(metricRegistry, hostname);

        final ResourceManagerRuntimeServices resourceManagerRuntimeServices =
                createResourceManagerRuntimeServices(
                        effectiveResourceManagerAndRuntimeServicesConfig,
                        rpcService,
                        highAvailabilityServices,
                        slotManagerMetricGroup);

        return createResourceManager(
                getEffectiveConfigurationForResourceManager(
                        effectiveResourceManagerAndRuntimeServicesConfig),
                resourceId,
                rpcService,
                highAvailabilityServices,
                heartbeatServices,
                fatalErrorHandler,
                clusterInformation,
                webInterfaceUrl,
                resourceManagerMetricGroup,
                resourceManagerRuntimeServices,
                ioExecutor);
    }

    /**
     * Configuration changes in this method will be visible to both {@link ResourceManager} and
     * {@link ResourceManagerRuntimeServices}. This can be overwritten by {@link
     * #getEffectiveConfigurationForResourceManager}.
     */
    protected Configuration getEffectiveConfigurationForResourceManagerAndRuntimeServices(
            final Configuration configuration) {
        return configuration;
    }

    /**
     * Configuration changes in this method will be visible to only {@link ResourceManager}. This
     * can overwrite {@link #getEffectiveConfigurationForResourceManagerAndRuntimeServices}.
     */
    protected Configuration getEffectiveConfigurationForResourceManager(
            final Configuration configuration) {
        return configuration;
    }

    protected abstract ResourceManager<T> createResourceManager(
            Configuration configuration,
            ResourceID resourceId,
            RpcService rpcService,
            HighAvailabilityServices highAvailabilityServices,
            HeartbeatServices heartbeatServices,
            FatalErrorHandler fatalErrorHandler,
            ClusterInformation clusterInformation,
            @Nullable String webInterfaceUrl,
            ResourceManagerMetricGroup resourceManagerMetricGroup,
            ResourceManagerRuntimeServices resourceManagerRuntimeServices,
            Executor ioExecutor)
            throws Exception;

    private ResourceManagerRuntimeServices createResourceManagerRuntimeServices(
            Configuration configuration,
            RpcService rpcService,
            HighAvailabilityServices highAvailabilityServices,
            SlotManagerMetricGroup slotManagerMetricGroup)
            throws ConfigurationException {

        return ResourceManagerRuntimeServices.fromConfiguration(
                createResourceManagerRuntimeServicesConfiguration(configuration),
                highAvailabilityServices,
                rpcService.getScheduledExecutor(),
                slotManagerMetricGroup);
    }

    protected abstract ResourceManagerRuntimeServicesConfiguration
            createResourceManagerRuntimeServicesConfiguration(Configuration configuration)
                    throws ConfigurationException;
}
