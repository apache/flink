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
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.BlobStore;
import org.apache.flink.runtime.blob.TestingBlobStoreBuilder;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.TestingHeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServicesBuilder;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.util.NoOpFatalErrorHandler;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

/**
 * {@code TestingPartialDispatcherServices} implements {@link PartialDispatcherServices} to be used
 * in test contexts.
 */
public class TestingPartialDispatcherServices extends PartialDispatcherServices {
    public TestingPartialDispatcherServices(
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
            Collection<FailureEnricher> failureEnrichers) {
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
    }

    public static Builder builder() {
        return new Builder();
    }

    /** {@code Builder} for creating {@code TestingPartialDispatcherServices} instances. */
    public static class Builder {

        private HighAvailabilityServices highAvailabilityServices =
                new TestingHighAvailabilityServicesBuilder().build();
        private GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever =
                CompletableFuture::new;
        private BlobStore blobStore = new TestingBlobStoreBuilder().createTestingBlobStore();
        private HeartbeatServices heartbeatServices = new TestingHeartbeatServices();
        private JobManagerMetricGroupFactory jobManagerMetricGroupFactory =
                UnregisteredMetricGroups::createUnregisteredJobManagerMetricGroup;
        private ExecutionGraphInfoStore executionGraphInfoStore =
                new MemoryExecutionGraphInfoStore();
        private FatalErrorHandler fatalErrorHandler = NoOpFatalErrorHandler.INSTANCE;
        private HistoryServerArchivist historyServerArchivist = VoidHistoryServerArchivist.INSTANCE;
        @Nullable private String metricQueryServiceAddress = null;
        private DispatcherOperationCaches operationCaches = new DispatcherOperationCaches();
        private Executor ioExecutor = ForkJoinPool.commonPool();

        private Builder() {}

        public Builder withHighAvailabilityServices(
                HighAvailabilityServices highAvailabilityServices) {
            this.highAvailabilityServices = highAvailabilityServices;
            return this;
        }

        public Builder withResourceManagerGatewayRetriever(
                GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever) {
            this.resourceManagerGatewayRetriever = resourceManagerGatewayRetriever;
            return this;
        }

        public Builder withBlobStore(BlobStore blobStore) {
            this.blobStore = blobStore;
            return this;
        }

        public Builder withHeartbeatServices(HeartbeatServices heartbeatServices) {
            this.heartbeatServices = heartbeatServices;
            return this;
        }

        public Builder withJobManagerMetricGroupFactory(
                JobManagerMetricGroupFactory jobManagerMetricGroupFactory) {
            this.jobManagerMetricGroupFactory = jobManagerMetricGroupFactory;
            return this;
        }

        public Builder withExecutionGraphInfoStore(
                ExecutionGraphInfoStore executionGraphInfoStore) {
            this.executionGraphInfoStore = executionGraphInfoStore;
            return this;
        }

        public Builder withFatalErrorHandler(FatalErrorHandler fatalErrorHandler) {
            this.fatalErrorHandler = fatalErrorHandler;
            return this;
        }

        public Builder withHistoryServerArchivist(HistoryServerArchivist historyServerArchivist) {
            this.historyServerArchivist = historyServerArchivist;
            return this;
        }

        public Builder withMetricQueryServiceAddress(@Nullable String metricQueryServiceAddress) {
            this.metricQueryServiceAddress = metricQueryServiceAddress;
            return this;
        }

        public Builder withOperationCaches(DispatcherOperationCaches operationCaches) {
            this.operationCaches = operationCaches;
            return this;
        }

        public Builder withIoExecutor(Executor ioExecutor) {
            this.ioExecutor = ioExecutor;
            return this;
        }

        public TestingPartialDispatcherServices build(File storageDir, Configuration configuration)
                throws IOException {
            try (BlobServer blobServer = new BlobServer(configuration, storageDir, blobStore)) {
                return build(blobServer, configuration);
            }
        }

        public TestingPartialDispatcherServices build(
                BlobServer blobServer, Configuration configuration) {
            return new TestingPartialDispatcherServices(
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
                    Collections.emptySet());
        }
    }
}
