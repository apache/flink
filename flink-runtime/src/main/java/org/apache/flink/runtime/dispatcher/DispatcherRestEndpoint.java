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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.RestServerEndpointConfiguration;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;
import org.apache.flink.runtime.rest.handler.job.BlobServerPortHandler;
import org.apache.flink.runtime.rest.handler.job.JobSubmitHandler;
import org.apache.flink.runtime.rest.handler.job.JobTerminationHandler;
import org.apache.flink.runtime.rest.messages.JobTerminationHeaders;
import org.apache.flink.runtime.webmonitor.WebMonitorEndpoint;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * REST endpoint for the {@link Dispatcher} component.
 */
public class DispatcherRestEndpoint extends WebMonitorEndpoint<DispatcherGateway> {

	public DispatcherRestEndpoint(
			RestServerEndpointConfiguration endpointConfiguration,
			GatewayRetriever<DispatcherGateway> leaderRetriever,
			Configuration clusterConfiguration,
			RestHandlerConfiguration restConfiguration,
			GatewayRetriever<ResourceManagerGateway> resourceManagerRetriever,
			Executor executor,
			MetricQueryServiceRetriever metricQueryServiceRetriever) {
		super(
			endpointConfiguration,
			leaderRetriever,
			clusterConfiguration,
			restConfiguration,
			resourceManagerRetriever,
			executor,
			metricQueryServiceRetriever);
	}

	@Override
	protected List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> initializeHandlers(CompletableFuture<String> restAddressFuture) {
		List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers = super.initializeHandlers(restAddressFuture);

		// Add the Dispatcher specific handlers

		final Time timeout = restConfiguration.getTimeout();
		final Map<String, String> responseHeaders = restConfiguration.getResponseHeaders();

		JobTerminationHandler jobTerminationHandler = new JobTerminationHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			JobTerminationHeaders.getInstance());

		BlobServerPortHandler blobServerPortHandler = new BlobServerPortHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders);

		JobSubmitHandler jobSubmitHandler = new JobSubmitHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders);

		handlers.add(Tuple2.of(JobTerminationHeaders.getInstance(), jobTerminationHandler));
		handlers.add(Tuple2.of(blobServerPortHandler.getMessageHeaders(), blobServerPortHandler));
		handlers.add(Tuple2.of(jobSubmitHandler.getMessageHeaders(), jobSubmitHandler));

		return handlers;
	}
}
