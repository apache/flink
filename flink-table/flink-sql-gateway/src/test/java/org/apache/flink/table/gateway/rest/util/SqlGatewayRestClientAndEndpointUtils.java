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

package org.apache.flink.table.gateway.rest.util;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.rest.SqlGatewayRestEndpoint;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** The test REST client and endpoint utils for sql gateway rest api testing. */
public class SqlGatewayRestClientAndEndpointUtils {

    // --------------------------------------------------------------------------------------------
    // Rest Endpoint
    // --------------------------------------------------------------------------------------------

    /**
     * Utility for setting up a rest server based on {@link SqlGatewayRestEndpoint} with a given set
     * of handlers.
     */
    public static class TestSqlGatewayRestEndpoint extends SqlGatewayRestEndpoint {

        public static Builder builder(Configuration configuration) {
            return new Builder(configuration, null);
        }

        /**
         * TestSqlGatewayRestEndpoint.Builder is a utility class for instantiating a
         * TestSqlGatewayRestEndpoint.
         */
        public static class Builder {

            private final Configuration configuration;
            private final List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers =
                    new ArrayList<>();
            private final SqlGatewayService sqlGatewayService;

            private Builder(Configuration configuration, SqlGatewayService sqlGatewayService) {
                this.configuration = configuration;
                this.sqlGatewayService = sqlGatewayService;
            }

            public Builder withHandler(
                    RestHandlerSpecification messageHeaders, ChannelInboundHandler handler) {
                this.handlers.add(Tuple2.of(messageHeaders, handler));
                return this;
            }

            public TestSqlGatewayRestEndpoint build() throws IOException, ConfigurationException {
                return new TestSqlGatewayRestEndpoint(configuration, handlers, sqlGatewayService);
            }

            public TestSqlGatewayRestEndpoint buildAndStart() throws Exception {
                TestSqlGatewayRestEndpoint serverEndpoint = build();
                serverEndpoint.start();

                return serverEndpoint;
            }
        }

        private final List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers;

        private TestSqlGatewayRestEndpoint(
                final Configuration configuration,
                final List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers,
                SqlGatewayService sqlGatewayService)
                throws IOException, ConfigurationException {
            super(configuration, sqlGatewayService);
            this.handlers = handlers;
        }

        @Override
        protected List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> initializeHandlers(
                final CompletableFuture<String> ignore) {
            return this.handlers;
        }

        @Override
        protected void startInternal() {}
    }

    // --------------------------------------------------------------------------------------------
    // Rest Client
    // --------------------------------------------------------------------------------------------

    /** Utility for setting up a rest client based on {@link RestClient} with default settings. */
    public static class TestRestClient extends RestClient {

        private final ExecutorService executorService;

        private TestRestClient(ExecutorService executorService) throws ConfigurationException {
            super(new Configuration(), executorService);
            this.executorService = executorService;
        }

        public static TestRestClient getTestRestClient() throws Exception {
            return new TestRestClient(
                    Executors.newFixedThreadPool(
                            1, new ExecutorThreadFactory("rest-client-thread-pool")));
        }

        public void shutdown() throws Exception {
            ExecutorUtils.gracefulShutdown(1, TimeUnit.SECONDS, executorService);
            super.closeAsync().get();
        }
    }
}
