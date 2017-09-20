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
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.rest.RestServerEndpoint;
import org.apache.flink.runtime.rest.RestServerEndpointConfiguration;
import org.apache.flink.runtime.rest.handler.LegacyRestHandlerAdapter;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;
import org.apache.flink.runtime.rest.handler.job.BlobServerPortHandler;
import org.apache.flink.runtime.rest.handler.job.JobConfigHandler;
import org.apache.flink.runtime.rest.handler.job.JobSubmitHandler;
import org.apache.flink.runtime.rest.handler.job.JobTerminationHandler;
import org.apache.flink.runtime.rest.handler.job.checkpoints.CheckpointConfigHandler;
import org.apache.flink.runtime.rest.handler.job.checkpoints.CheckpointStatisticsHandler;
import org.apache.flink.runtime.rest.handler.legacy.ClusterConfigHandler;
import org.apache.flink.runtime.rest.handler.legacy.ClusterOverviewHandler;
import org.apache.flink.runtime.rest.handler.legacy.CurrentJobsOverviewHandler;
import org.apache.flink.runtime.rest.handler.legacy.DashboardConfigHandler;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.files.StaticFileServerHandler;
import org.apache.flink.runtime.rest.handler.legacy.files.WebContentHandlerSpecification;
import org.apache.flink.runtime.rest.handler.legacy.messages.ClusterConfigurationInfo;
import org.apache.flink.runtime.rest.handler.legacy.messages.DashboardConfiguration;
import org.apache.flink.runtime.rest.handler.legacy.messages.StatusOverviewWithVersion;
import org.apache.flink.runtime.rest.messages.CheckpointConfigHeaders;
import org.apache.flink.runtime.rest.messages.CheckpointStatisticsHeaders;
import org.apache.flink.runtime.rest.messages.ClusterConfigurationInfoHeaders;
import org.apache.flink.runtime.rest.messages.ClusterOverviewHeaders;
import org.apache.flink.runtime.rest.messages.CurrentJobsOverviewHandlerHeaders;
import org.apache.flink.runtime.rest.messages.DashboardConfigurationHeaders;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.JobConfigHeaders;
import org.apache.flink.runtime.rest.messages.JobTerminationHeaders;
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
	private final Configuration clusterConfiguration;
	private final RestHandlerConfiguration restConfiguration;
	private final Executor executor;

	private final ExecutionGraphCache executionGraphCache;

	public DispatcherRestEndpoint(
			RestServerEndpointConfiguration endpointConfiguration,
			GatewayRetriever<DispatcherGateway> leaderRetriever,
			Configuration clusterConfiguration,
			RestHandlerConfiguration restConfiguration,
			Executor executor) {
		super(endpointConfiguration);
		this.leaderRetriever = Preconditions.checkNotNull(leaderRetriever);
		this.clusterConfiguration = Preconditions.checkNotNull(clusterConfiguration);
		this.restConfiguration = Preconditions.checkNotNull(restConfiguration);
		this.executor = Preconditions.checkNotNull(executor);

		this.executionGraphCache = new ExecutionGraphCache(
			restConfiguration.getTimeout(),
			Time.milliseconds(restConfiguration.getRefreshInterval()));
	}

	@Override
	protected Collection<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> initializeHandlers(CompletableFuture<String> restAddressFuture) {
		ArrayList<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers = new ArrayList<>(3);

		final Time timeout = restConfiguration.getTimeout();

		LegacyRestHandlerAdapter<DispatcherGateway, StatusOverviewWithVersion, EmptyMessageParameters> clusterOverviewHandler = new LegacyRestHandlerAdapter<>(
			restAddressFuture,
			leaderRetriever,
			timeout,
			ClusterOverviewHeaders.getInstance(),
			new ClusterOverviewHandler(
				executor,
				timeout));

		LegacyRestHandlerAdapter<DispatcherGateway, DashboardConfiguration, EmptyMessageParameters> dashboardConfigurationHandler = new LegacyRestHandlerAdapter<>(
			restAddressFuture,
			leaderRetriever,
			timeout,
			DashboardConfigurationHeaders.getInstance(),
			new DashboardConfigHandler(
				executor,
				restConfiguration.getRefreshInterval()));

		LegacyRestHandlerAdapter<DispatcherGateway, MultipleJobsDetails, EmptyMessageParameters> currentJobsOverviewHandler = new LegacyRestHandlerAdapter<>(
			restAddressFuture,
			leaderRetriever,
			timeout,
			CurrentJobsOverviewHandlerHeaders.getInstance(),
			new CurrentJobsOverviewHandler(
				executor,
				timeout,
				true,
				true));

		LegacyRestHandlerAdapter<DispatcherGateway, ClusterConfigurationInfo, EmptyMessageParameters> clusterConfigurationHandler = new LegacyRestHandlerAdapter<>(
			restAddressFuture,
			leaderRetriever,
			timeout,
			ClusterConfigurationInfoHeaders.getInstance(),
			new ClusterConfigHandler(
				executor,
				clusterConfiguration));

		JobTerminationHandler jobTerminationHandler = new JobTerminationHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			JobTerminationHeaders.getInstance());

		JobConfigHandler jobConfigHandler = new JobConfigHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			JobConfigHeaders.getInstance(),
			executionGraphCache,
			executor);

		CheckpointConfigHandler checkpointConfigHandler = new CheckpointConfigHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			CheckpointConfigHeaders.getInstance(),
			executionGraphCache,
			executor);

		CheckpointStatisticsHandler checkpointStatisticsHandler = new CheckpointStatisticsHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			CheckpointStatisticsHeaders.getInstance(),
			executionGraphCache,
			executor);

		final File tmpDir = restConfiguration.getTmpDir();

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

		handlers.add(Tuple2.of(ClusterOverviewHeaders.getInstance(), clusterOverviewHandler));
		handlers.add(Tuple2.of(ClusterConfigurationInfoHeaders.getInstance(), clusterConfigurationHandler));
		handlers.add(Tuple2.of(DashboardConfigurationHeaders.getInstance(), dashboardConfigurationHandler));
		handlers.add(Tuple2.of(CurrentJobsOverviewHandlerHeaders.getInstance(), currentJobsOverviewHandler));
		handlers.add(Tuple2.of(JobTerminationHeaders.getInstance(), jobTerminationHandler));
		handlers.add(Tuple2.of(JobConfigHeaders.getInstance(), jobConfigHandler));
		handlers.add(Tuple2.of(CheckpointConfigHeaders.getInstance(), checkpointConfigHandler));
		handlers.add(Tuple2.of(CheckpointStatisticsHeaders.getInstance(), checkpointStatisticsHandler));

		BlobServerPortHandler blobServerPortHandler = new BlobServerPortHandler(restAddressFuture, leaderRetriever, timeout);
		handlers.add(Tuple2.of(blobServerPortHandler.getMessageHeaders(), blobServerPortHandler));

		JobSubmitHandler jobSubmitHandler = new JobSubmitHandler(restAddressFuture, leaderRetriever, timeout);
		handlers.add(Tuple2.of(jobSubmitHandler.getMessageHeaders(), jobSubmitHandler));

		// This handler MUST be added last, as it otherwise masks all subsequent GET handlers
		optWebContent.ifPresent(
			webContent -> handlers.add(Tuple2.of(WebContentHandlerSpecification.getInstance(), webContent)));

		return handlers;
	}

	@Override
	public void shutdown(Time timeout) {
		super.shutdown(timeout);

		executionGraphCache.close();

		final File tmpDir = restConfiguration.getTmpDir();

		try {
			log.info("Removing cache directory {}", tmpDir);
			FileUtils.deleteDirectory(tmpDir);
		} catch (Throwable t) {
			log.warn("Error while deleting cache directory {}", tmpDir, t);
		}
	}
}
