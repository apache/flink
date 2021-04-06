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
import org.apache.flink.runtime.rest.RestServerEndpoint;
import org.apache.flink.runtime.rest.RestServerEndpointConfiguration;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Utility {@link RestServerEndpoint} for setting up a rest server with a given set of handlers. */
public class TestRestServerEndpoint extends RestServerEndpoint {

    public static Builder builder(RestServerEndpointConfiguration configuration) {
        return new Builder(configuration);
    }

    /**
     * TestRestServerEndpoint.Builder is a utility class for instantiating a TestRestServerEndPoint.
     */
    public static class Builder {

        private final RestServerEndpointConfiguration configuration;
        private final List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers =
                new ArrayList<>();

        private Builder(RestServerEndpointConfiguration configuration) {
            this.configuration = configuration;
        }

        public Builder withHandler(
                RestHandlerSpecification messageHeaders, ChannelInboundHandler handler) {
            this.handlers.add(Tuple2.of(messageHeaders, handler));
            return this;
        }

        public Builder withHandler(AbstractRestHandler<?, ?, ?, ?> handler) {
            this.handlers.add(Tuple2.of(handler.getMessageHeaders(), handler));
            return this;
        }

        public TestRestServerEndpoint build() throws IOException {
            return new TestRestServerEndpoint(configuration, handlers);
        }

        public TestRestServerEndpoint buildAndStart() throws Exception {
            TestRestServerEndpoint serverEndpoint = build();
            serverEndpoint.start();

            return serverEndpoint;
        }
    }

    private final List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers;

    private TestRestServerEndpoint(
            final RestServerEndpointConfiguration configuration,
            final List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers)
            throws IOException {
        super(configuration);
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
