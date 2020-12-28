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

package org.apache.flink.runtime.resourcemanager.active;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.io.network.partition.ResourceManagerPartitionTrackerImpl;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerFactory;
import org.apache.flink.runtime.resourcemanager.ResourceManagerRuntimeServices;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;

import javax.annotation.Nullable;

import java.util.concurrent.Executor;

/**
 * Factory class for creating {@link ActiveResourceManager} with various implementations of {@link
 * ResourceManagerDriver}.
 */
public abstract class ActiveResourceManagerFactory<WorkerType extends ResourceIDRetrievable>
        extends ResourceManagerFactory<WorkerType> {

    @Override
    public ResourceManager<WorkerType> createResourceManager(
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
        return super.createResourceManager(
                createActiveResourceManagerConfiguration(configuration),
                resourceId,
                rpcService,
                highAvailabilityServices,
                heartbeatServices,
                fatalErrorHandler,
                clusterInformation,
                webInterfaceUrl,
                metricRegistry,
                hostname,
                ioExecutor);
    }

    private Configuration createActiveResourceManagerConfiguration(
            Configuration originalConfiguration) {
        final Configuration copiedConfig = new Configuration(originalConfiguration);
        // In active mode, it depends on the ResourceManager to set the ResourceID of TaskManagers.
        copiedConfig.removeConfig(TaskManagerOptions.TASK_MANAGER_RESOURCE_ID);
        return TaskExecutorProcessUtils.getConfigurationMapLegacyTaskManagerHeapSizeToConfigOption(
                copiedConfig, TaskManagerOptions.TOTAL_PROCESS_MEMORY);
    }

    @Override
    public ResourceManager<WorkerType> createResourceManager(
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
            throws Exception {

        return new ActiveResourceManager<>(
                createResourceManagerDriver(
                        configuration, webInterfaceUrl, rpcService.getAddress()),
                configuration,
                rpcService,
                resourceId,
                highAvailabilityServices,
                heartbeatServices,
                resourceManagerRuntimeServices.getSlotManager(),
                ResourceManagerPartitionTrackerImpl::new,
                resourceManagerRuntimeServices.getJobLeaderIdService(),
                clusterInformation,
                fatalErrorHandler,
                resourceManagerMetricGroup,
                ioExecutor);
    }

    protected abstract ResourceManagerDriver<WorkerType> createResourceManagerDriver(
            Configuration configuration, @Nullable String webInterfaceUrl, String rpcAddress)
            throws Exception;
}
