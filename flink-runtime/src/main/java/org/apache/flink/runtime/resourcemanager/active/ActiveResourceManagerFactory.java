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

import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.io.network.partition.ResourceManagerPartitionTrackerImpl;
import org.apache.flink.runtime.metrics.ThresholdMeter;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerFactory;
import org.apache.flink.runtime.resourcemanager.ResourceManagerRuntimeServices;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.concurrent.Executor;

/**
 * Factory class for creating {@link ActiveResourceManager} with various implementations of {@link
 * ResourceManagerDriver}.
 */
public abstract class ActiveResourceManagerFactory<WorkerType extends ResourceIDRetrievable>
        extends ResourceManagerFactory<WorkerType> {

    @Override
    protected Configuration getEffectiveConfigurationForResourceManagerAndRuntimeServices(
            Configuration configuration) {
        return TaskExecutorProcessUtils.getConfigurationMapLegacyTaskManagerHeapSizeToConfigOption(
                configuration, TaskManagerOptions.TOTAL_PROCESS_MEMORY);
    }

    @Override
    protected Configuration getEffectiveConfigurationForResourceManager(
            Configuration configuration) {
        if (ClusterOptions.isFineGrainedResourceManagementEnabled(configuration)) {
            final Configuration copiedConfig = new Configuration(configuration);

            if (copiedConfig.removeConfig(TaskManagerOptions.TOTAL_PROCESS_MEMORY)) {
                logIgnoreTotalMemory(TaskManagerOptions.TOTAL_PROCESS_MEMORY);
            }

            if (copiedConfig.removeConfig(TaskManagerOptions.TOTAL_FLINK_MEMORY)) {
                logIgnoreTotalMemory(TaskManagerOptions.TOTAL_FLINK_MEMORY);
            }

            return copiedConfig;
        }

        return configuration;
    }

    private void logIgnoreTotalMemory(ConfigOption<MemorySize> option) {
        log.warn(
                "Configured size for '{}' is ignored. Total memory size for TaskManagers are"
                        + " dynamically decided in fine-grained resource management.",
                option.key());
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

        final ThresholdMeter failureRater = createStartWorkerFailureRater(configuration);
        final Duration retryInterval =
                configuration.get(ResourceManagerOptions.START_WORKER_RETRY_INTERVAL);
        final Duration workerRegistrationTimeout =
                configuration.get(ResourceManagerOptions.TASK_MANAGER_REGISTRATION_TIMEOUT);
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
                failureRater,
                retryInterval,
                workerRegistrationTimeout,
                ioExecutor);
    }

    protected abstract ResourceManagerDriver<WorkerType> createResourceManagerDriver(
            Configuration configuration, @Nullable String webInterfaceUrl, String rpcAddress)
            throws Exception;

    public static ThresholdMeter createStartWorkerFailureRater(Configuration configuration) {
        double rate = configuration.getDouble(ResourceManagerOptions.START_WORKER_MAX_FAILURE_RATE);
        if (rate <= 0) {
            throw new IllegalConfigurationException(
                    String.format(
                            "Configured max start worker failure rate ('%s') must be larger than 0. Current: %f",
                            ResourceManagerOptions.START_WORKER_MAX_FAILURE_RATE.key(), rate));
        }
        return new ThresholdMeter(rate, Duration.ofMinutes(1));
    }
}
