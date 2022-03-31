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

package org.apache.flink.runtime.minicluster;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.MemoryExecutionGraphInfoStore;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponent;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;
import org.apache.flink.util.concurrent.FutureUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/** {@link MiniCluster} extension which allows to set a custom {@link HighAvailabilityServices}. */
public class TestingMiniCluster extends MiniCluster {

    public static Builder newBuilder(TestingMiniClusterConfiguration configuration) {
        return new Builder(configuration);
    }

    /** Builder for {@link TestingMiniCluster}. */
    public static class Builder {

        private final TestingMiniClusterConfiguration configuration;

        @Nullable private Supplier<HighAvailabilityServices> highAvailabilityServicesSupplier;

        @Nullable
        private Supplier<DispatcherResourceManagerComponentFactory>
                dispatcherResourceManagerComponentFactorySupplier;

        public Builder(TestingMiniClusterConfiguration configuration) {
            this.configuration = configuration;
        }

        public Builder setHighAvailabilityServicesSupplier(
                @Nullable Supplier<HighAvailabilityServices> highAvailabilityServicesSupplier) {
            this.highAvailabilityServicesSupplier = highAvailabilityServicesSupplier;
            return this;
        }

        public Builder setDispatcherResourceManagerComponentFactorySupplier(
                @Nullable
                        Supplier<DispatcherResourceManagerComponentFactory>
                                dispatcherResourceManagerComponentFactorySupplier) {
            this.dispatcherResourceManagerComponentFactorySupplier =
                    dispatcherResourceManagerComponentFactorySupplier;
            return this;
        }

        public TestingMiniCluster build() {
            return new TestingMiniCluster(
                    configuration,
                    highAvailabilityServicesSupplier,
                    dispatcherResourceManagerComponentFactorySupplier);
        }
    }

    private final int numberDispatcherResourceManagerComponents;

    private final boolean localCommunication;

    @Nullable private final Supplier<HighAvailabilityServices> highAvailabilityServicesSupplier;

    @Nullable
    private final Supplier<DispatcherResourceManagerComponentFactory>
            dispatcherResourceManagerComponentFactorySupplier;

    private TestingMiniCluster(
            TestingMiniClusterConfiguration miniClusterConfiguration,
            @Nullable Supplier<HighAvailabilityServices> highAvailabilityServicesSupplier,
            @Nullable
                    Supplier<DispatcherResourceManagerComponentFactory>
                            dispatcherResourceManagerComponentFactorySupplier) {
        super(miniClusterConfiguration);
        this.numberDispatcherResourceManagerComponents =
                miniClusterConfiguration.getNumberDispatcherResourceManagerComponents();
        this.highAvailabilityServicesSupplier = highAvailabilityServicesSupplier;
        this.dispatcherResourceManagerComponentFactorySupplier =
                dispatcherResourceManagerComponentFactorySupplier;
        this.localCommunication = miniClusterConfiguration.isLocalCommunication();
    }

    @Override
    protected boolean useLocalCommunication() {
        return localCommunication;
    }

    @Override
    protected HighAvailabilityServices createHighAvailabilityServices(
            Configuration configuration, Executor executor) throws Exception {
        if (highAvailabilityServicesSupplier != null) {
            return highAvailabilityServicesSupplier.get();
        } else {
            return super.createHighAvailabilityServices(configuration, executor);
        }
    }

    @Override
    protected DispatcherResourceManagerComponentFactory
            createDispatcherResourceManagerComponentFactory() {
        if (dispatcherResourceManagerComponentFactorySupplier != null) {
            return dispatcherResourceManagerComponentFactorySupplier.get();
        } else {
            return super.createDispatcherResourceManagerComponentFactory();
        }
    }

    @Override
    protected Collection<? extends DispatcherResourceManagerComponent>
            createDispatcherResourceManagerComponents(
                    Configuration configuration,
                    RpcServiceFactory rpcServiceFactory,
                    BlobServer blobServer,
                    HeartbeatServices heartbeatServices,
                    MetricRegistry metricRegistry,
                    MetricQueryServiceRetriever metricQueryServiceRetriever,
                    FatalErrorHandler fatalErrorHandler)
                    throws Exception {
        DispatcherResourceManagerComponentFactory dispatcherResourceManagerComponentFactory =
                createDispatcherResourceManagerComponentFactory();

        final List<DispatcherResourceManagerComponent> result =
                new ArrayList<>(numberDispatcherResourceManagerComponents);

        for (int i = 0; i < numberDispatcherResourceManagerComponents; i++) {
            // FLINK-24038 relies on the fact that there is only one leader election instance per
            // JVM that is freed when the JobManager stops. This is simulated in the
            // TestingMiniCluster by providing individual HighAvailabilityServices per
            // DispatcherResourceManagerComponent to allow running more-than-once JobManager tests
            final HighAvailabilityServices thisHaServices =
                    createHighAvailabilityServices(configuration, getIOExecutor());
            final DispatcherResourceManagerComponent dispatcherResourceManagerComponent =
                    dispatcherResourceManagerComponentFactory.create(
                            configuration,
                            ResourceID.generate(),
                            getIOExecutor(),
                            rpcServiceFactory.createRpcService(),
                            thisHaServices,
                            blobServer,
                            heartbeatServices,
                            metricRegistry,
                            new MemoryExecutionGraphInfoStore(),
                            metricQueryServiceRetriever,
                            fatalErrorHandler);

            final CompletableFuture<Void> shutDownFuture =
                    dispatcherResourceManagerComponent
                            .getShutDownFuture()
                            .thenCompose(
                                    applicationStatus ->
                                            dispatcherResourceManagerComponent.stopApplication(
                                                    applicationStatus, null))
                            .thenRun(
                                    () -> {
                                        try {
                                            // The individual HighAvailabilityServices have to be
                                            // closed explicitly to trigger the revocation of the
                                            // leadership when shutting down the JobManager
                                            thisHaServices.close();
                                        } catch (Exception ex) {
                                            throw new CompletionException(
                                                    "HighAvailabilityServices were not expected to fail but did",
                                                    ex);
                                        }
                                    });
            FutureUtils.assertNoException(shutDownFuture);
            result.add(dispatcherResourceManagerComponent);
        }

        return result;
    }

    @Override
    public CompletableFuture<DispatcherGateway> getDispatcherGatewayFuture() {
        return super.getDispatcherGatewayFuture();
    }
}
