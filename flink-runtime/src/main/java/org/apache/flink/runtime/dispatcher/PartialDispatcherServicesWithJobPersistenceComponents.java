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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.jobmanager.JobGraphWriter;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.concurrent.Executor;

/** {@link DispatcherFactory} services container. */
public class PartialDispatcherServicesWithJobPersistenceComponents
        extends PartialDispatcherServices {

    private final JobGraphWriter jobGraphWriter;
    private final JobResultStore jobResultStore;

    private PartialDispatcherServicesWithJobPersistenceComponents(
            Configuration configuration,
            HighAvailabilityServices highAvailabilityServices,
            GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
            BlobServer blobServer,
            HeartbeatServices heartbeatServices,
            JobManagerMetricGroupFactory jobManagerMetricGroupFactory,
            ExecutionGraphInfoStore executionGraphInfoStore,
            FatalErrorHandler fatalErrorHandler,
            HistoryServerArchivist historyServerArchivist,
            @Nullable String metricQueryServiceAddress,
            Executor ioExecutor,
            DispatcherOperationCaches operationCaches,
            Collection<FailureEnricher> failureEnrichers,
            JobGraphWriter jobGraphWriter,
            JobResultStore jobResultStore) {
        super(
                configuration,
                highAvailabilityServices,
                resourceManagerGatewayRetriever,
                blobServer,
                heartbeatServices,
                jobManagerMetricGroupFactory,
                executionGraphInfoStore,
                fatalErrorHandler,
                historyServerArchivist,
                metricQueryServiceAddress,
                ioExecutor,
                operationCaches,
                failureEnrichers);
        this.jobGraphWriter = jobGraphWriter;
        this.jobResultStore = jobResultStore;
    }

    public JobGraphWriter getJobGraphWriter() {
        return jobGraphWriter;
    }

    public JobResultStore getJobResultStore() {
        return jobResultStore;
    }

    public static PartialDispatcherServicesWithJobPersistenceComponents from(
            PartialDispatcherServices partialDispatcherServices,
            JobGraphWriter jobGraphWriter,
            JobResultStore jobResultStore) {
        return new PartialDispatcherServicesWithJobPersistenceComponents(
                partialDispatcherServices.getConfiguration(),
                partialDispatcherServices.getHighAvailabilityServices(),
                partialDispatcherServices.getResourceManagerGatewayRetriever(),
                partialDispatcherServices.getBlobServer(),
                partialDispatcherServices.getHeartbeatServices(),
                partialDispatcherServices.getJobManagerMetricGroupFactory(),
                partialDispatcherServices.getArchivedExecutionGraphStore(),
                partialDispatcherServices.getFatalErrorHandler(),
                partialDispatcherServices.getHistoryServerArchivist(),
                partialDispatcherServices.getMetricQueryServiceAddress(),
                partialDispatcherServices.getIoExecutor(),
                partialDispatcherServices.getOperationCaches(),
                partialDispatcherServices.getFailureEnrichers(),
                jobGraphWriter,
                jobResultStore);
    }
}
