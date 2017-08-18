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
import org.apache.flink.runtime.messages.webmonitor.StatusOverviewWithVersion;
import org.apache.flink.runtime.rest.RestServerEndpoint;
import org.apache.flink.runtime.rest.RestServerEndpointConfiguration;
import org.apache.flink.runtime.rest.handler.LegacyRestHandlerAdapter;
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;
import org.apache.flink.runtime.rest.handler.legacy.ClusterOverviewHandler;
import org.apache.flink.runtime.rest.handler.legacy.files.StaticFileServerHandler;
import org.apache.flink.runtime.rest.handler.legacy.files.WebContentHandlerSpecification;
import org.apache.flink.runtime.rest.messages.ClusterOverviewHeaders;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * REST endpoint for the {@link Dispatcher} component.
 */
public class DispatcherRestEndpoint extends RestServerEndpoint {

	private final GatewayRetriever<DispatcherGateway> leaderRetriever;
	private final Time timeout;
	private final File tmpDir;
	private final Executor executor;

	public DispatcherRestEndpoint(
			RestServerEndpointConfiguration configuration,
			GatewayRetriever<DispatcherGateway> leaderRetriever,
			Time timeout,
			File tmpDir,
			Executor executor) {
		super(configuration);
		this.leaderRetriever = Preconditions.checkNotNull(leaderRetriever);
		this.timeout = Preconditions.checkNotNull(timeout);
		this.tmpDir = Preconditions.checkNotNull(tmpDir);
		this.executor = Preconditions.checkNotNull(executor);
	}

	@Override
	protected Collection<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> initializeHandlers(CompletableFuture<String> restAddressFuture) {
		ArrayList<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers = new ArrayList<>(2);

		LegacyRestHandlerAdapter<DispatcherGateway, StatusOverviewWithVersion, EmptyMessageParameters> clusterOverviewHandler = new LegacyRestHandlerAdapter<>(
			restAddressFuture,
			leaderRetriever,
			timeout,
			ClusterOverviewHeaders.getInstance(),
			new ClusterOverviewHandler(
				executor,
				timeout));

		handlers.add(Tuple2.of(ClusterOverviewHeaders.getInstance(), clusterOverviewHandler));

		Optional<StaticFileServerHandler<DispatcherGateway>> optWebContent;

		try {
			optWebContent = WebMonitorUtils.tryLoadWebContent(
				leaderRetriever,
				restAddressFuture,
				timeout,
				tmpDir);
		} catch (IOException e) {
			log.warn("Could not load web content handler.", e);
			optWebContent = Optional.empty();
		}

		optWebContent.ifPresent(
			webContent -> handlers.add(Tuple2.of(WebContentHandlerSpecification.getInstance(), webContent)));

		return handlers;
	}

	@Override
	public void shutdown(Time timeout) {
		super.shutdown(timeout);

		try {
			log.info("Removing cache directory {}", tmpDir);
			FileUtils.deleteDirectory(tmpDir);
		} catch (Throwable t) {
			log.warn("Error while deleting cache directory {}", tmpDir, t);
		}
	}
}
