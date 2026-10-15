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

package org.apache.flink.container.entrypoint;

import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponent;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedHaServices;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.security.token.DelegationTokenManager;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.event.Level;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ApplicationModeMiniCluster}. */
class ApplicationModeMiniClusterTest {

    @RegisterExtension
    private final LoggerAuditingExtension loggerAuditingExtension =
            new LoggerAuditingExtension(ApplicationModeMiniCluster.class, Level.WARN);

    private MiniClusterConfiguration miniClusterConfiguration;
    private PackagedProgram program;

    @BeforeEach
    void setUp() throws ProgramInvocationException {
        miniClusterConfiguration = new MiniClusterConfiguration.Builder().build();
        program =
                PackagedProgram.newBuilder()
                        .setEntryPointClassName(NoOpProgram.class.getName())
                        .build();
    }

    @Test
    void rejectsNullProgram() {
        assertThatThrownBy(() -> new ApplicationModeMiniCluster(miniClusterConfiguration, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void futuresIncompleteOnConstruction() {
        final ApplicationModeMiniCluster miniCluster =
                new ApplicationModeMiniCluster(miniClusterConfiguration, program);

        assertThat(miniCluster.getApplicationShutDownFuture()).isNotDone();
        assertThat(miniCluster.getCloseRequestedFuture()).isNotDone();
    }

    @Test
    void closeBeforeStartRecordsCloseRequestOnly() {
        final ApplicationModeMiniCluster miniCluster =
                new ApplicationModeMiniCluster(miniClusterConfiguration, program);

        final CompletableFuture<Void> closeFuture = miniCluster.closeAsync();

        assertThat(closeFuture).isCompleted();
        assertThat(miniCluster.getCloseRequestedFuture()).isCompleted();
        assertThat(miniCluster.getApplicationShutDownFuture())
                .as("closing the cluster must not look like the application finishing")
                .isNotDone();
    }

    @Test
    void dispatcherNotShutDownBeforeStart() {
        final ApplicationModeMiniCluster miniCluster =
                new ApplicationModeMiniCluster(miniClusterConfiguration, program);

        assertThat(miniCluster.isDispatcherShutDown())
                .as("closing during startup must not clean up the HA data")
                .isFalse();
    }

    @Test
    void closeIsIdempotent() {
        final ApplicationModeMiniCluster miniCluster =
                new ApplicationModeMiniCluster(miniClusterConfiguration, program);

        assertThat(miniCluster.closeAsync()).isCompleted();
        assertThat(miniCluster.closeAsync()).isCompleted();
        assertThat(miniCluster.getCloseRequestedFuture()).isCompleted();
        assertThat(miniCluster.getApplicationShutDownFuture()).isNotDone();
    }

    @Test
    void taskManagerFatalErrorIgnoredAfterCloseRequested() {
        final ApplicationModeMiniCluster miniCluster =
                new ApplicationModeMiniCluster(miniClusterConfiguration, program);
        miniCluster.closeAsync();

        miniCluster
                .createTaskManagerFatalErrorHandler(0)
                .onFatalError(new RuntimeException("error during shutdown"));

        assertThat(loggerAuditingExtension.getMessages()).isEmpty();
    }

    @Test
    void failedStartAfterDispatcherCreationDoesNotDeadlock() {
        final ApplicationModeMiniCluster miniCluster =
                new ApplicationModeMiniCluster(miniClusterConfiguration, program) {
                    @Override
                    protected Collection<? extends DispatcherResourceManagerComponent>
                            createDispatcherResourceManagerComponents(
                                    Configuration configuration,
                                    RpcServiceFactory rpcServiceFactory,
                                    BlobServer blobServer,
                                    HeartbeatServices heartbeatServices,
                                    DelegationTokenManager delegationTokenManager,
                                    MetricRegistry metricRegistry,
                                    MetricQueryServiceRetriever metricQueryServiceRetriever,
                                    FatalErrorHandler fatalErrorHandler)
                                    throws Exception {
                        super.createDispatcherResourceManagerComponents(
                                configuration,
                                rpcServiceFactory,
                                blobServer,
                                heartbeatServices,
                                delegationTokenManager,
                                metricRegistry,
                                metricQueryServiceRetriever,
                                fatalErrorHandler);
                        throw new Exception("startup failure");
                    }
                };

        assertThatThrownBy(miniCluster::start).hasMessage("startup failure");
    }

    @Test
    void closeDuringStartDoesNotWaitForStart() throws Exception {
        final OneShotLatch startBlocked = new OneShotLatch();
        final OneShotLatch unblockStart = new OneShotLatch();
        final ApplicationModeMiniCluster miniCluster =
                new ApplicationModeMiniCluster(miniClusterConfiguration, program) {
                    @Override
                    protected Collection<? extends DispatcherResourceManagerComponent>
                            createDispatcherResourceManagerComponents(
                                    Configuration configuration,
                                    RpcServiceFactory rpcServiceFactory,
                                    BlobServer blobServer,
                                    HeartbeatServices heartbeatServices,
                                    DelegationTokenManager delegationTokenManager,
                                    MetricRegistry metricRegistry,
                                    MetricQueryServiceRetriever metricQueryServiceRetriever,
                                    FatalErrorHandler fatalErrorHandler)
                                    throws Exception {
                        startBlocked.trigger();
                        unblockStart.await();
                        return super.createDispatcherResourceManagerComponents(
                                configuration,
                                rpcServiceFactory,
                                blobServer,
                                heartbeatServices,
                                delegationTokenManager,
                                metricRegistry,
                                metricQueryServiceRetriever,
                                fatalErrorHandler);
                    }
                };
        final CompletableFuture<Void> startFuture =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                miniCluster.start();
                            } catch (Exception e) {
                                throw new CompletionException(e);
                            }
                        });
        try {
            startBlocked.await();

            assertThat(CompletableFuture.supplyAsync(miniCluster::closeAsync))
                    .as("closing must not wait for the lock held by start()")
                    .succeedsWithin(Duration.ofSeconds(10));
        } finally {
            unblockStart.trigger();
            startFuture.get();
            miniCluster.close();
        }
    }

    @Test
    void closeWhileAnotherCloseIsUnderWayWaitsForIt() throws Exception {
        final OneShotLatch haServicesClosing = new OneShotLatch();
        final OneShotLatch finishClosingHaServices = new OneShotLatch();
        final ApplicationModeMiniCluster miniCluster =
                new ApplicationModeMiniCluster(miniClusterConfiguration, program) {
                    @Override
                    protected HighAvailabilityServices createHighAvailabilityServices(
                            Configuration configuration, Executor executor) {
                        return new EmbeddedHaServices(executor) {
                            @Override
                            public void close() throws Exception {
                                haServicesClosing.trigger();
                                finishClosingHaServices.await();
                                super.close();
                            }
                        };
                    }
                };
        miniCluster.start();
        final CompletableFuture<Void> firstClose = miniCluster.closeAsync();
        haServicesClosing.await();
        assertThat(miniCluster.isRunning()).isFalse();

        final CompletableFuture<Void> secondClose = miniCluster.closeAsync();
        try {
            assertThat(secondClose)
                    .as("a second close must wait for the close under way")
                    .isNotDone();
        } finally {
            finishClosingHaServices.trigger();
        }
        assertThat(secondClose).succeedsWithin(Duration.ofSeconds(30));
        assertThat(firstClose).succeedsWithin(Duration.ofSeconds(30));
    }

    public static class NoOpProgram {
        public static void main(String[] args) {}
    }
}
