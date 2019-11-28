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

package org.apache.flink.client.program.rest;

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

/**
 * Utility {@link RestServerEndpoint} for setting up a rest server with a given set of handlers.
 */
class TestRestServerEndpoint extends RestServerEndpoint {

	private final AbstractRestHandler<?, ?, ?, ?>[] abstractRestHandlers;

	TestRestServerEndpoint(
			final RestServerEndpointConfiguration configuration,
			final AbstractRestHandler<?, ?, ?, ?>... abstractRestHandlers) throws IOException {
		super(configuration);
		this.abstractRestHandlers = abstractRestHandlers;
	}

	@Override
	protected List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> initializeHandlers(final CompletableFuture<String> localAddressFuture) {
		final List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers = new ArrayList<>(abstractRestHandlers.length);
		for (final AbstractRestHandler abstractRestHandler : abstractRestHandlers) {
			handlers.add(Tuple2.of(
				abstractRestHandler.getMessageHeaders(),
				abstractRestHandler));
		}
		return handlers;
	}

	@Override
	protected void startInternal() {
	}

	static TestRestServerEndpoint createAndStartRestServerEndpoint(
			final RestServerEndpointConfiguration restServerEndpointConfiguration,
			final AbstractRestHandler<?, ?, ?, ?>... abstractRestHandlers) throws Exception {
		final TestRestServerEndpoint testRestServerEndpoint = new TestRestServerEndpoint(restServerEndpointConfiguration, abstractRestHandlers);
		testRestServerEndpoint.start();
		return testRestServerEndpoint;
	}
}
