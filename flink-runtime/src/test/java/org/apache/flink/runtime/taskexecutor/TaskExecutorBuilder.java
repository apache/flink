/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.blob.NoOpTaskExecutorBlobService;
import org.apache.flink.runtime.blob.TaskExecutorBlobService;
import org.apache.flink.runtime.blob.VoidPermanentBlobService;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.WorkingDirectory;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.HeartbeatServicesImpl;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.io.network.partition.TaskExecutorPartitionTracker;
import org.apache.flink.runtime.io.network.partition.TestingTaskExecutorPartitionTracker;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.rest.util.NoOpFatalErrorHandler;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.security.token.DelegationTokenReceiverRepository;
import org.apache.flink.util.concurrent.Executors;

import javax.annotation.Nullable;

/** Builder for testing {@link TaskExecutor}. */
public class TaskExecutorBuilder {

    private final RpcService rpcService;

    private final HighAvailabilityServices haServices;

    private ResourceID resourceId = ResourceID.generate();

    private Configuration configuration = new Configuration();

    @Nullable private TaskManagerConfiguration taskManagerConfiguration = null;

    @Nullable private TaskManagerServices taskManagerServices = null;

    private ExternalResourceInfoProvider externalResourceInfoProvider =
            ExternalResourceInfoProvider.NO_EXTERNAL_RESOURCES;

    private HeartbeatServices heartbeatServices = new HeartbeatServicesImpl(1 << 20, 1 << 20);

    private TaskManagerMetricGroup taskManagerMetricGroup =
            UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup();

    @Nullable private String metricQueryServiceAddress = null;

    @Nullable private BlobCacheService taskExecutorBlobService = null;

    private FatalErrorHandler fatalErrorHandler = NoOpFatalErrorHandler.INSTANCE;

    private TaskExecutorPartitionTracker partitionTracker =
            new TestingTaskExecutorPartitionTracker();

    private final WorkingDirectory workingDirectory;

    private TaskExecutorBuilder(
            RpcService rpcService,
            HighAvailabilityServices haServices,
            WorkingDirectory workingDirectory) {
        this.rpcService = rpcService;
        this.haServices = haServices;
        this.workingDirectory = workingDirectory;
    }

    public TaskExecutorBuilder setConfiguration(Configuration configuration) {
        this.configuration = configuration;
        return this;
    }

    public TaskExecutorBuilder setResourceId(ResourceID resourceId) {
        this.resourceId = resourceId;
        return this;
    }

    public TaskExecutor build() throws Exception {

        final TaskExecutorBlobService resolvedTaskExecutorBlobService;

        TaskExecutorResourceUtils.adjustForLocalExecution(configuration);

        if (taskExecutorBlobService == null) {
            resolvedTaskExecutorBlobService = NoOpTaskExecutorBlobService.INSTANCE;
        } else {
            resolvedTaskExecutorBlobService = taskExecutorBlobService;
        }

        final TaskExecutorResourceSpec taskExecutorResourceSpec =
                TaskExecutorResourceUtils.resourceSpecFromConfigForLocalExecution(configuration);

        final TaskManagerConfiguration resolvedTaskManagerConfiguration;

        if (taskManagerConfiguration == null) {
            resolvedTaskManagerConfiguration =
                    TaskManagerConfiguration.fromConfiguration(
                            configuration,
                            taskExecutorResourceSpec,
                            rpcService.getAddress(),
                            workingDirectory.getTmpDirectory());
        } else {
            resolvedTaskManagerConfiguration = taskManagerConfiguration;
        }

        final TaskManagerServices resolvedTaskManagerServices;

        if (taskManagerServices == null) {
            final TaskManagerServicesConfiguration taskManagerServicesConfiguration =
                    TaskManagerServicesConfiguration.fromConfiguration(
                            configuration,
                            resourceId,
                            rpcService.getAddress(),
                            true,
                            taskExecutorResourceSpec,
                            workingDirectory);
            resolvedTaskManagerServices =
                    TaskManagerServices.fromConfiguration(
                            taskManagerServicesConfiguration,
                            VoidPermanentBlobService.INSTANCE,
                            UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup(),
                            Executors.newDirectExecutorService(),
                            rpcService.getScheduledExecutor(),
                            throwable -> {},
                            workingDirectory);
        } else {
            resolvedTaskManagerServices = taskManagerServices;
        }

        final DelegationTokenReceiverRepository delegationTokenReceiverRepository =
                new DelegationTokenReceiverRepository(configuration, null);

        return new TaskExecutor(
                rpcService,
                resolvedTaskManagerConfiguration,
                haServices,
                resolvedTaskManagerServices,
                externalResourceInfoProvider,
                heartbeatServices,
                taskManagerMetricGroup,
                metricQueryServiceAddress,
                resolvedTaskExecutorBlobService,
                fatalErrorHandler,
                partitionTracker,
                delegationTokenReceiverRepository);
    }

    public static TaskExecutorBuilder newBuilder(
            RpcService rpcService,
            HighAvailabilityServices highAvailabilityServices,
            WorkingDirectory workingDirectory) {
        return new TaskExecutorBuilder(rpcService, highAvailabilityServices, workingDirectory);
    }
}
