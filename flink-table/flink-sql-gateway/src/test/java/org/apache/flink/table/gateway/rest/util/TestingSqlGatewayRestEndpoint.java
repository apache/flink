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
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.rest.SqlGatewayRestEndpoint;
import org.apache.flink.util.ConfigurationException;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Utility for setting up a rest server based on {@link SqlGatewayRestEndpoint} with a given set of
 * handlers.
 */
public class TestingSqlGatewayRestEndpoint extends SqlGatewayRestEndpoint {

    public static Builder builder(
            Configuration configuration, SqlGatewayService sqlGatewayService) {
        return new Builder(configuration, sqlGatewayService);
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

        public TestingSqlGatewayRestEndpoint build() throws IOException, ConfigurationException {
            return new TestingSqlGatewayRestEndpoint(configuration, handlers, sqlGatewayService);
        }

        public TestingSqlGatewayRestEndpoint buildAndStart() throws Exception {
            TestingSqlGatewayRestEndpoint serverEndpoint = build();
            serverEndpoint.start();

            return serverEndpoint;
        }
    }

    private final List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers;

    private TestingSqlGatewayRestEndpoint(
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
