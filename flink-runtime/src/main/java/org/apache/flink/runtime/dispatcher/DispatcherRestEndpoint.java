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
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.blob.TransientBlobService;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.RestServerEndpointConfiguration;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;
import org.apache.flink.runtime.rest.handler.job.JobSubmitHandler;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.webmonitor.WebMonitorEndpoint;
import org.apache.flink.runtime.webmonitor.WebMonitorExtension;
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * REST endpoint for the {@link Dispatcher} component.
 */
public class DispatcherRestEndpoint extends WebMonitorEndpoint<DispatcherGateway> {

	private WebMonitorExtension webSubmissionExtension;

	public DispatcherRestEndpoint(
			RestServerEndpointConfiguration endpointConfiguration,
			GatewayRetriever<DispatcherGateway> leaderRetriever,
			Configuration clusterConfiguration,
			RestHandlerConfiguration restConfiguration,
			GatewayRetriever<ResourceManagerGateway> resourceManagerRetriever,
			TransientBlobService transientBlobService,
			ExecutorService executor,
			MetricQueryServiceRetriever metricQueryServiceRetriever,
			LeaderElectionService leaderElectionService,
			FatalErrorHandler fatalErrorHandler) throws IOException {

		super(
			endpointConfiguration,
			leaderRetriever,
			clusterConfiguration,
			restConfiguration,
			resourceManagerRetriever,
			transientBlobService,
			executor,
			metricQueryServiceRetriever,
			leaderElectionService,
			fatalErrorHandler);

		webSubmissionExtension = WebMonitorExtension.empty();
	}

	@Override
	protected List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> initializeHandlers(CompletableFuture<String> restAddressFuture) {
		List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers = super.initializeHandlers(restAddressFuture);

		// Add the Dispatcher specific handlers

		final Time timeout = restConfiguration.getTimeout();

		JobSubmitHandler jobSubmitHandler = new JobSubmitHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			executor,
			clusterConfiguration);

		if (clusterConfiguration.getBoolean(WebOptions.SUBMIT_ENABLE)) {
			try {
				webSubmissionExtension = WebMonitorUtils.loadWebSubmissionExtension(
					leaderRetriever,
					restAddressFuture,
					timeout,
					responseHeaders,
					uploadDir,
					executor,
					clusterConfiguration);

				// register extension handlers
				handlers.addAll(webSubmissionExtension.getHandlers());
			} catch (FlinkException e) {
				if (log.isDebugEnabled()) {
					log.debug("Failed to load web based job submission extension.", e);
				} else {
					log.info("Failed to load web based job submission extension. " +
						"Probable reason: flink-runtime-web is not in the classpath.");
				}
			}
		} else {
			log.info("Web-based job submission is not enabled.");
		}

		handlers.add(Tuple2.of(jobSubmitHandler.getMessageHeaders(), jobSubmitHandler));

		return handlers;
	}

	@Override
	protected CompletableFuture<Void> shutDownInternal() {
		final CompletableFuture<Void> shutdownFuture = super.shutDownInternal();

		final CompletableFuture<Void> shutdownResultFuture = new CompletableFuture<>();

		shutdownFuture.whenComplete(
			(Void ignored, Throwable throwable) -> {
				webSubmissionExtension.closeAsync().whenComplete(
					(Void innerIgnored, Throwable innerThrowable) -> {
						if (innerThrowable != null) {
							shutdownResultFuture.completeExceptionally(
								ExceptionUtils.firstOrSuppressed(innerThrowable, throwable));
						} else if (throwable != null) {
							shutdownResultFuture.completeExceptionally(throwable);
						} else {
							shutdownResultFuture.complete(null);
						}
					});
			});

		return shutdownResultFuture;
	}
}
