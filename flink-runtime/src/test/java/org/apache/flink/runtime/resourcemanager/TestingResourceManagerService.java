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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.TestingHeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.leaderelection.LeaderInformation;
import org.apache.flink.runtime.leaderelection.TestingLeaderElection;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.metrics.util.TestingMetricRegistry;
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.security.token.NoOpDelegationTokenManager;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.concurrent.FutureUtils;

import javax.annotation.Nullable;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Implementation of {@link ResourceManagerService} for testing purpose. */
public class TestingResourceManagerService implements ResourceManagerService {

    private static final Time TIMEOUT = Time.seconds(10L);

    private final ResourceManagerServiceImpl rmService;
    private final TestingLeaderElection leaderElection;
    private final TestingFatalErrorHandler fatalErrorHandler;
    private final RpcService rpcService;
    private final boolean needStopRpcService;

    private TestingResourceManagerService(
            ResourceManagerServiceImpl rmService,
            TestingLeaderElection leaderElection,
            TestingFatalErrorHandler fatalErrorHandler,
            RpcService rpcService,
            boolean needStopRpcService) {
        this.rmService = rmService;
        this.leaderElection = leaderElection;
        this.fatalErrorHandler = fatalErrorHandler;
        this.rpcService = rpcService;
        this.needStopRpcService = needStopRpcService;
    }

    @Override
    public void start() throws Exception {
        rmService.start();
    }

    @Override
    public CompletableFuture<Void> getTerminationFuture() {
        return rmService.getTerminationFuture();
    }

    @Override
    public CompletableFuture<Void> deregisterApplication(
            ApplicationStatus applicationStatus, @Nullable String diagnostics) {
        return rmService.deregisterApplication(applicationStatus, diagnostics);
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return rmService.closeAsync();
    }

    public Optional<ResourceManagerGateway> getResourceManagerGateway() {
        return getResourceManagerOpt().map(rm -> rm.getSelfGateway(ResourceManagerGateway.class));
    }

    public Optional<ResourceManagerId> getResourceManagerFencingToken() {
        return getResourceManagerOpt().map(FencedRpcEndpoint::getFencingToken);
    }

    public Optional<CompletableFuture<Void>> getResourceManagerTerminationFuture() {
        return getResourceManagerOpt().map(RpcEndpoint::getTerminationFuture);
    }

    private Optional<ResourceManager<?>> getResourceManagerOpt() {
        return Optional.ofNullable(rmService.getLeaderResourceManager());
    }

    public CompletableFuture<LeaderInformation> isLeader(UUID uuid) {
        return leaderElection.isLeader(uuid);
    }

    public void notLeader() {
        leaderElection.notLeader();
    }

    public void rethrowFatalErrorIfAny() throws Exception {
        if (fatalErrorHandler.hasExceptionOccurred()) {
            fatalErrorHandler.rethrowError();
        }
    }

    public void ignoreFatalErrors() {
        fatalErrorHandler.clearError();
    }

    public void cleanUp() throws Exception {
        rmService
                .closeAsync()
                .thenCompose((ignore) -> this.stopRpcServiceIfNeeded())
                .get(TIMEOUT.getSize(), TIMEOUT.getUnit());
    }

    private CompletableFuture<Void> stopRpcServiceIfNeeded() {
        return needStopRpcService ? rpcService.closeAsync() : FutureUtils.completedVoidFuture();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {

        private RpcService rpcService = null;
        private boolean needStopRpcService = true;
        private TestingLeaderElection rmLeaderElection = null;
        private Function<JobID, LeaderRetrievalService> jmLeaderRetrieverFunction = null;

        public Builder setRpcService(RpcService rpcService) {
            this.rpcService = checkNotNull(rpcService);
            this.needStopRpcService = false;
            return this;
        }

        public Builder setRmLeaderElection(TestingLeaderElection rmLeaderElection) {
            this.rmLeaderElection = checkNotNull(rmLeaderElection);
            return this;
        }

        public Builder setJmLeaderRetrieverFunction(
                Function<JobID, LeaderRetrievalService> jmLeaderRetrieverFunction) {
            this.jmLeaderRetrieverFunction = checkNotNull(jmLeaderRetrieverFunction);
            return this;
        }

        public TestingResourceManagerService build() throws Exception {
            rpcService = rpcService != null ? rpcService : new TestingRpcService();
            rmLeaderElection =
                    rmLeaderElection != null ? rmLeaderElection : new TestingLeaderElection();

            final TestingHighAvailabilityServices haServices =
                    new TestingHighAvailabilityServices();
            haServices.setResourceManagerLeaderElection(rmLeaderElection);
            if (jmLeaderRetrieverFunction != null) {
                haServices.setJobMasterLeaderRetrieverFunction(jmLeaderRetrieverFunction);
            }

            final TestingFatalErrorHandler fatalErrorHandler = new TestingFatalErrorHandler();

            return new TestingResourceManagerService(
                    ResourceManagerServiceImpl.create(
                            StandaloneResourceManagerFactory.getInstance(),
                            new Configuration(),
                            ResourceID.generate(),
                            rpcService,
                            haServices,
                            new TestingHeartbeatServices(),
                            new NoOpDelegationTokenManager(),
                            fatalErrorHandler,
                            new ClusterInformation("localhost", 1234),
                            null,
                            TestingMetricRegistry.builder().build(),
                            "localhost",
                            ForkJoinPool.commonPool()),
                    rmLeaderElection,
                    fatalErrorHandler,
                    rpcService,
                    needStopRpcService);
        }
    }
}
