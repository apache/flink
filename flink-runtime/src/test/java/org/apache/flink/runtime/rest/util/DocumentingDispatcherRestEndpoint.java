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

package org.apache.flink.runtime.rest.util;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.blob.NoOpTransientBlobService;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherRestEndpoint;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElection;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;
import org.apache.flink.runtime.rest.handler.legacy.metrics.VoidMetricFetcher;
import org.apache.flink.runtime.rest.messages.RuntimeMessageHeaders;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ConfigurationException;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

/**
 * Utility class to extract the {@link RuntimeMessageHeaders} that the {@link
 * DispatcherRestEndpoint} supports.
 */
public class DocumentingDispatcherRestEndpoint extends DispatcherRestEndpoint
        implements DocumentingRestEndpoint {

    private static final Configuration config;
    private static final RestHandlerConfiguration handlerConfig;
    private static final GatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever;
    private static final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;

    static {
        config = new Configuration();
        config.setString(RestOptions.ADDRESS, "localhost");
        // necessary for loading the web-submission extension
        config.setString(JobManagerOptions.ADDRESS, "localhost");
        handlerConfig = RestHandlerConfiguration.fromConfiguration(config);

        dispatcherGatewayRetriever = () -> null;
        resourceManagerGatewayRetriever = () -> null;
    }

    public DocumentingDispatcherRestEndpoint() throws IOException, ConfigurationException {
        super(
                dispatcherGatewayRetriever,
                config,
                handlerConfig,
                resourceManagerGatewayRetriever,
                NoOpTransientBlobService.INSTANCE,
                Executors.newScheduledThreadPool(1),
                VoidMetricFetcher.INSTANCE,
                NoOpLeaderElection.INSTANCE,
                NoOpExecutionGraphCache.INSTANCE,
                NoOpFatalErrorHandler.INSTANCE);
    }

    @Override
    public List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> initializeHandlers(
            final CompletableFuture<String> localAddressFuture) {
        return super.initializeHandlers(localAddressFuture);
    }

    private enum NoOpLeaderElection implements LeaderElection {
        INSTANCE;

        @Override
        public void startLeaderElection(LeaderContender contender) {}

        @Override
        public void confirmLeadership(UUID leaderSessionID, String leaderAddress) {}

        @Override
        public boolean hasLeadership(UUID leaderSessionId) {
            return false;
        }

        @Override
        public void close() {}
    }
}
