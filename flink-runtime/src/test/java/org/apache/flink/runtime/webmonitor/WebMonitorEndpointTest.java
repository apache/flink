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

package org.apache.flink.runtime.webmonitor;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HistoryServerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.NoOpTransientBlobService;
import org.apache.flink.runtime.leaderelection.StandaloneLeaderElection;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;
import org.apache.flink.runtime.rest.handler.cluster.JobManagerLogUrlHandler;
import org.apache.flink.runtime.rest.handler.job.GeneratedLogUrlHandler;
import org.apache.flink.runtime.rest.handler.legacy.metrics.VoidMetricFetcher;
import org.apache.flink.runtime.rest.handler.taskmanager.TaskManagerLogUrlHandler;
import org.apache.flink.runtime.rest.messages.JobManagerLogUrlHeaders;
import org.apache.flink.runtime.rest.messages.TaskManagerLogUrlHeaders;
import org.apache.flink.runtime.rest.util.EnvironmentInfoUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.ExecutorUtils;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link WebMonitorEndpoint}. */
class WebMonitorEndpointTest {

    @Test
    void cleansUpExpiredExecutionGraphs() throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(RestOptions.ADDRESS, "localhost");
        configuration.set(WebOptions.REFRESH_INTERVAL, Duration.ofMillis(5L));
        final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        final long timeout = 10000L;

        final OneShotLatch cleanupLatch = new OneShotLatch();
        final TestingExecutionGraphCache executionGraphCache =
                TestingExecutionGraphCache.newBuilder()
                        .setCleanupRunnable(cleanupLatch::trigger)
                        .build();
        try (final WebMonitorEndpoint<RestfulGateway> webMonitorEndpoint =
                new WebMonitorEndpoint<>(
                        CompletableFuture::new,
                        configuration,
                        RestHandlerConfiguration.fromConfiguration(configuration),
                        CompletableFuture::new,
                        NoOpTransientBlobService.INSTANCE,
                        executor,
                        VoidMetricFetcher.INSTANCE,
                        new StandaloneLeaderElection(UUID.randomUUID()),
                        executionGraphCache,
                        new TestingFatalErrorHandler())) {

            webMonitorEndpoint.start();

            // check that the cleanup will be triggered
            cleanupLatch.await(timeout, TimeUnit.MILLISECONDS);
        } finally {
            ExecutorUtils.gracefulShutdown(timeout, TimeUnit.MILLISECONDS, executor);
        }
    }

    @Test
    void usesGeneratedLogUrlHandlersByDefault() throws Exception {
        assertGeneratedLogUrlHandlers(new Configuration(), Optional.empty());
    }

    @Test
    void usesGeneratedLogUrlHandlersWhenCustomHandlersAreExplicitlyDisabled() throws Exception {
        final Configuration configuration = customHandlersConfiguration(false);

        assertGeneratedLogUrlHandlers(configuration, Optional.of(yarnContext()));
    }

    @Test
    void usesGeneratedLogUrlHandlersWhenYarnEnvironmentIsMissing() throws Exception {
        assertGeneratedLogUrlHandlers(customHandlersConfiguration(true), Optional.empty());
    }

    @Test
    void usesCustomLogUrlHandlersWhenEnabledOnYarn() throws Exception {
        final Configuration configuration = customHandlersConfiguration(true);

        assertThat(
                        findHandler(
                                        configuration,
                                        Optional.of(yarnContext()),
                                        JobManagerLogUrlHeaders.getInstance())
                                .f1)
                .isInstanceOf(JobManagerLogUrlHandler.class);
        assertThat(
                        findHandler(
                                        configuration,
                                        Optional.of(yarnContext()),
                                        TaskManagerLogUrlHeaders.getInstance())
                                .f1)
                .isInstanceOf(TaskManagerLogUrlHandler.class);
    }

    private static Configuration customHandlersConfiguration(boolean enabled) {
        final Configuration configuration = new Configuration();
        configuration.set(
                HistoryServerOptions
                        .HISTORY_SERVER_JOBMANAGER_TASKMANAGER_LOG_ENABLE_CUSTOM_HANDLERS,
                enabled);
        return configuration;
    }

    private static EnvironmentInfoUtils.EnvironmentContext yarnContext() {
        return new EnvironmentInfoUtils.EnvironmentContext(
                "container_1", "nm.example.com", "8042", "flink");
    }

    private static void assertGeneratedLogUrlHandlers(
            Configuration configuration,
            Optional<EnvironmentInfoUtils.EnvironmentContext> environmentContext)
            throws Exception {
        assertThat(
                        findHandler(
                                        configuration,
                                        environmentContext,
                                        JobManagerLogUrlHeaders.getInstance())
                                .f1)
                .isInstanceOf(GeneratedLogUrlHandler.class);
        assertThat(
                        findHandler(
                                        configuration,
                                        environmentContext,
                                        TaskManagerLogUrlHeaders.getInstance())
                                .f1)
                .isInstanceOf(GeneratedLogUrlHandler.class);
    }

    private static Tuple2<RestHandlerSpecification, ?> findHandler(
            Configuration configuration,
            Optional<EnvironmentInfoUtils.EnvironmentContext> environmentContext,
            RestHandlerSpecification headers)
            throws Exception {
        final Configuration endpointConfiguration = new Configuration(configuration);
        endpointConfiguration.set(RestOptions.ADDRESS, "localhost");
        final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        try (final WebMonitorEndpoint<RestfulGateway> webMonitorEndpoint =
                new WebMonitorEndpoint<RestfulGateway>(
                        CompletableFuture::new,
                        endpointConfiguration,
                        RestHandlerConfiguration.fromConfiguration(endpointConfiguration),
                        CompletableFuture::new,
                        NoOpTransientBlobService.INSTANCE,
                        executor,
                        VoidMetricFetcher.INSTANCE,
                        new StandaloneLeaderElection(UUID.randomUUID()),
                        TestingExecutionGraphCache.newBuilder().build(),
                        new TestingFatalErrorHandler()) {
                    @Override
                    protected Optional<EnvironmentInfoUtils.EnvironmentContext>
                            readYarnEnvironmentContext() {
                        return environmentContext;
                    }
                }) {

            final List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers =
                    webMonitorEndpoint.initializeHandlers(CompletableFuture.completedFuture(""));

            return handlers.stream()
                    .filter(handler -> handler.f0.equals(headers))
                    .findFirst()
                    .orElseThrow(
                            () ->
                                    new AssertionError(
                                            "No handler registered for " + headers.getClass()));
        } finally {
            ExecutorUtils.gracefulShutdown(10000L, TimeUnit.MILLISECONDS, executor);
        }
    }
}
