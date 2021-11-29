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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobmanager.JobGraphWriter;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.concurrent.Executor;

/** {@link Dispatcher} services container. */
public class DispatcherServices {

    private final Configuration configuration;

    private final HighAvailabilityServices highAvailabilityServices;

    private final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;

    private final BlobServer blobServer;

    private final HeartbeatServices heartbeatServices;

    private final JobManagerMetricGroup jobManagerMetricGroup;

    private final ExecutionGraphInfoStore executionGraphInfoStore;

    private final FatalErrorHandler fatalErrorHandler;

    private final HistoryServerArchivist historyServerArchivist;

    @Nullable private final String metricQueryServiceAddress;

    private final DispatcherOperationCaches operationCaches;

    private final JobGraphWriter jobGraphWriter;

    private final JobManagerRunnerFactory jobManagerRunnerFactory;

    private final Executor ioExecutor;

    DispatcherServices(
            Configuration configuration,
            HighAvailabilityServices highAvailabilityServices,
            GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
            BlobServer blobServer,
            HeartbeatServices heartbeatServices,
            ExecutionGraphInfoStore executionGraphInfoStore,
            FatalErrorHandler fatalErrorHandler,
            HistoryServerArchivist historyServerArchivist,
            @Nullable String metricQueryServiceAddress,
            DispatcherOperationCaches operationCaches,
            JobManagerMetricGroup jobManagerMetricGroup,
            JobGraphWriter jobGraphWriter,
            JobManagerRunnerFactory jobManagerRunnerFactory,
            Executor ioExecutor) {
        this.configuration = Preconditions.checkNotNull(configuration, "Configuration");
        this.highAvailabilityServices =
                Preconditions.checkNotNull(highAvailabilityServices, "HighAvailabilityServices");
        this.resourceManagerGatewayRetriever =
                Preconditions.checkNotNull(
                        resourceManagerGatewayRetriever, "ResourceManagerGatewayRetriever");
        this.blobServer = Preconditions.checkNotNull(blobServer, "BlobServer");
        this.heartbeatServices = Preconditions.checkNotNull(heartbeatServices, "HeartBeatServices");
        this.executionGraphInfoStore =
                Preconditions.checkNotNull(executionGraphInfoStore, "ExecutionGraphInfoStore");
        this.fatalErrorHandler = Preconditions.checkNotNull(fatalErrorHandler, "FatalErrorHandler");
        this.historyServerArchivist =
                Preconditions.checkNotNull(historyServerArchivist, "HistoryServerArchivist");
        this.metricQueryServiceAddress = metricQueryServiceAddress;
        this.operationCaches = Preconditions.checkNotNull(operationCaches, "OperationCaches");
        this.jobManagerMetricGroup =
                Preconditions.checkNotNull(jobManagerMetricGroup, "JobManagerMetricGroup");
        this.jobGraphWriter = Preconditions.checkNotNull(jobGraphWriter, "JobGraphWriter");
        this.jobManagerRunnerFactory =
                Preconditions.checkNotNull(jobManagerRunnerFactory, "JobManagerRunnerFactory");
        this.ioExecutor = Preconditions.checkNotNull(ioExecutor, "IOExecutor");
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public HighAvailabilityServices getHighAvailabilityServices() {
        return highAvailabilityServices;
    }

    public GatewayRetriever<ResourceManagerGateway> getResourceManagerGatewayRetriever() {
        return resourceManagerGatewayRetriever;
    }

    public BlobServer getBlobServer() {
        return blobServer;
    }

    public HeartbeatServices getHeartbeatServices() {
        return heartbeatServices;
    }

    public JobManagerMetricGroup getJobManagerMetricGroup() {
        return jobManagerMetricGroup;
    }

    public ExecutionGraphInfoStore getArchivedExecutionGraphStore() {
        return executionGraphInfoStore;
    }

    public FatalErrorHandler getFatalErrorHandler() {
        return fatalErrorHandler;
    }

    public HistoryServerArchivist getHistoryServerArchivist() {
        return historyServerArchivist;
    }

    @Nullable
    public String getMetricQueryServiceAddress() {
        return metricQueryServiceAddress;
    }

    public DispatcherOperationCaches getOperationCaches() {
        return operationCaches;
    }

    public JobGraphWriter getJobGraphWriter() {
        return jobGraphWriter;
    }

    JobManagerRunnerFactory getJobManagerRunnerFactory() {
        return jobManagerRunnerFactory;
    }

    public Executor getIoExecutor() {
        return ioExecutor;
    }

    public static DispatcherServices from(
            PartialDispatcherServicesWithJobGraphStore
                    partialDispatcherServicesWithJobGraphStore,
            JobManagerRunnerFactory jobManagerRunnerFactory) {
        return new DispatcherServices(
                partialDispatcherServicesWithJobGraphStore.getConfiguration(),
                partialDispatcherServicesWithJobGraphStore.getHighAvailabilityServices(),
                partialDispatcherServicesWithJobGraphStore.getResourceManagerGatewayRetriever(),
                partialDispatcherServicesWithJobGraphStore.getBlobServer(),
                partialDispatcherServicesWithJobGraphStore.getHeartbeatServices(),
                partialDispatcherServicesWithJobGraphStore.getArchivedExecutionGraphStore(),
                partialDispatcherServicesWithJobGraphStore.getFatalErrorHandler(),
                partialDispatcherServicesWithJobGraphStore.getHistoryServerArchivist(),
                partialDispatcherServicesWithJobGraphStore.getMetricQueryServiceAddress(),
                partialDispatcherServicesWithJobGraphStore.getOperationCaches(),
                partialDispatcherServicesWithJobGraphStore
                        .getJobManagerMetricGroupFactory()
                        .create(),
                partialDispatcherServicesWithJobGraphStore.getJobGraphWriter(),
                jobManagerRunnerFactory,
                partialDispatcherServicesWithJobGraphStore.getIoExecutor());
    }
}
