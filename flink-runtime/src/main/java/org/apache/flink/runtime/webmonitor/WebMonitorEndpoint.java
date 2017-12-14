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

package org.apache.flink.runtime.webmonitor;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.RestServerEndpoint;
import org.apache.flink.runtime.rest.RestServerEndpointConfiguration;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;
import org.apache.flink.runtime.rest.handler.cluster.ClusterConfigHandler;
import org.apache.flink.runtime.rest.handler.cluster.ClusterOverviewHandler;
import org.apache.flink.runtime.rest.handler.cluster.DashboardConfigHandler;
import org.apache.flink.runtime.rest.handler.job.JobAccumulatorsHandler;
import org.apache.flink.runtime.rest.handler.job.JobConfigHandler;
import org.apache.flink.runtime.rest.handler.job.JobDetailsHandler;
import org.apache.flink.runtime.rest.handler.job.JobExceptionsHandler;
import org.apache.flink.runtime.rest.handler.job.JobIdsHandler;
import org.apache.flink.runtime.rest.handler.job.JobPlanHandler;
import org.apache.flink.runtime.rest.handler.job.JobVertexAccumulatorsHandler;
import org.apache.flink.runtime.rest.handler.job.JobsOverviewHandler;
import org.apache.flink.runtime.rest.handler.job.SubtasksTimesHandler;
import org.apache.flink.runtime.rest.handler.job.checkpoints.CheckpointConfigHandler;
import org.apache.flink.runtime.rest.handler.job.checkpoints.CheckpointStatisticDetailsHandler;
import org.apache.flink.runtime.rest.handler.job.checkpoints.CheckpointStatsCache;
import org.apache.flink.runtime.rest.handler.job.checkpoints.CheckpointingStatisticsHandler;
import org.apache.flink.runtime.rest.handler.job.checkpoints.TaskCheckpointStatisticDetailsHandler;
import org.apache.flink.runtime.rest.handler.job.metrics.JobManagerMetricsHandler;
import org.apache.flink.runtime.rest.handler.job.metrics.JobMetricsHandler;
import org.apache.flink.runtime.rest.handler.job.metrics.JobVertexMetricsHandler;
import org.apache.flink.runtime.rest.handler.job.metrics.SubtaskMetricsHandler;
import org.apache.flink.runtime.rest.handler.job.metrics.TaskManagerMetricsHandler;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.files.StaticFileServerHandler;
import org.apache.flink.runtime.rest.handler.legacy.files.WebContentHandlerSpecification;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.taskmanager.TaskManagerDetailsHandler;
import org.apache.flink.runtime.rest.handler.taskmanager.TaskManagersHandler;
import org.apache.flink.runtime.rest.messages.ClusterConfigurationInfoHeaders;
import org.apache.flink.runtime.rest.messages.ClusterOverviewHeaders;
import org.apache.flink.runtime.rest.messages.DashboardConfigurationHeaders;
import org.apache.flink.runtime.rest.messages.JobAccumulatorsHeaders;
import org.apache.flink.runtime.rest.messages.JobConfigHeaders;
import org.apache.flink.runtime.rest.messages.JobExceptionsHeaders;
import org.apache.flink.runtime.rest.messages.JobIdsWithStatusesOverviewHeaders;
import org.apache.flink.runtime.rest.messages.JobPlanHeaders;
import org.apache.flink.runtime.rest.messages.JobVertexAccumulatorsHeaders;
import org.apache.flink.runtime.rest.messages.JobsOverviewHeaders;
import org.apache.flink.runtime.rest.messages.SubtasksTimesHeaders;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointConfigHeaders;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointStatisticDetailsHeaders;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointingStatisticsHeaders;
import org.apache.flink.runtime.rest.messages.checkpoints.TaskCheckpointStatisticsHeaders;
import org.apache.flink.runtime.rest.messages.job.JobDetailsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.JobManagerMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.JobMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.JobVertexMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.SubtaskMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.TaskManagerMetricsHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerDetailsHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersHeaders;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Rest endpoint which serves the web frontend REST calls.
 *
 * @param <T> type of the leader gateway
 */
public class WebMonitorEndpoint<T extends RestfulGateway> extends RestServerEndpoint {

	protected final GatewayRetriever<T> leaderRetriever;
	private final Configuration clusterConfiguration;
	protected final RestHandlerConfiguration restConfiguration;
	private final GatewayRetriever<ResourceManagerGateway> resourceManagerRetriever;
	private final Executor executor;

	private final ExecutionGraphCache executionGraphCache;
	private final CheckpointStatsCache checkpointStatsCache;

	private final MetricFetcher<T> metricFetcher;

	public WebMonitorEndpoint(
			RestServerEndpointConfiguration endpointConfiguration,
			GatewayRetriever<T> leaderRetriever,
			Configuration clusterConfiguration,
			RestHandlerConfiguration restConfiguration,
			GatewayRetriever<ResourceManagerGateway> resourceManagerRetriever,
			Executor executor,
			MetricQueryServiceRetriever metricQueryServiceRetriever) {
		super(endpointConfiguration);
		this.leaderRetriever = Preconditions.checkNotNull(leaderRetriever);
		this.clusterConfiguration = Preconditions.checkNotNull(clusterConfiguration);
		this.restConfiguration = Preconditions.checkNotNull(restConfiguration);
		this.resourceManagerRetriever = Preconditions.checkNotNull(resourceManagerRetriever);
		this.executor = Preconditions.checkNotNull(executor);

		this.executionGraphCache = new ExecutionGraphCache(
			restConfiguration.getTimeout(),
			Time.milliseconds(restConfiguration.getRefreshInterval()));

		this.checkpointStatsCache = new CheckpointStatsCache(
			restConfiguration.getMaxCheckpointStatisticCacheEntries());

		this.metricFetcher = new MetricFetcher<>(
			leaderRetriever,
			metricQueryServiceRetriever,
			executor,
			restConfiguration.getTimeout());
	}

	@Override
	protected List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> initializeHandlers(CompletableFuture<String> restAddressFuture) {
		ArrayList<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers = new ArrayList<>(30);

		final Time timeout = restConfiguration.getTimeout();
		final Map<String, String> responseHeaders = restConfiguration.getResponseHeaders();

		ClusterOverviewHandler clusterOverviewHandler = new ClusterOverviewHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			ClusterOverviewHeaders.getInstance());

		DashboardConfigHandler dashboardConfigHandler = new DashboardConfigHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			DashboardConfigurationHeaders.getInstance(),
			restConfiguration.getRefreshInterval());

		JobIdsHandler jobIdsHandler = new JobIdsHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			JobIdsWithStatusesOverviewHeaders.getInstance());

		JobsOverviewHandler jobsOverviewHandler = new JobsOverviewHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			JobsOverviewHeaders.getInstance());

		ClusterConfigHandler clusterConfigurationHandler = new ClusterConfigHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			ClusterConfigurationInfoHeaders.getInstance(),
			clusterConfiguration);

		JobConfigHandler jobConfigHandler = new JobConfigHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			JobConfigHeaders.getInstance(),
			executionGraphCache,
			executor);

		CheckpointConfigHandler checkpointConfigHandler = new CheckpointConfigHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			CheckpointConfigHeaders.getInstance(),
			executionGraphCache,
			executor);

		CheckpointingStatisticsHandler checkpointStatisticsHandler = new CheckpointingStatisticsHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			CheckpointingStatisticsHeaders.getInstance(),
			executionGraphCache,
			executor);

		CheckpointStatisticDetailsHandler checkpointStatisticDetailsHandler = new CheckpointStatisticDetailsHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			CheckpointStatisticDetailsHeaders.getInstance(),
			executionGraphCache,
			executor,
			checkpointStatsCache);

		JobPlanHandler jobPlanHandler = new JobPlanHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			JobPlanHeaders.getInstance(),
			executionGraphCache,
			executor);

		TaskCheckpointStatisticDetailsHandler taskCheckpointStatisticDetailsHandler = new TaskCheckpointStatisticDetailsHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			TaskCheckpointStatisticsHeaders.getInstance(),
			executionGraphCache,
			executor,
			checkpointStatsCache);

		JobExceptionsHandler jobExceptionsHandler = new JobExceptionsHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			JobExceptionsHeaders.getInstance(),
			executionGraphCache,
			executor);

		JobVertexAccumulatorsHandler jobVertexAccumulatorsHandler = new JobVertexAccumulatorsHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			JobVertexAccumulatorsHeaders.getInstance(),
			executionGraphCache,
			executor);

		TaskManagersHandler taskManagersHandler = new TaskManagersHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			TaskManagersHeaders.getInstance(),
			resourceManagerRetriever);

		TaskManagerDetailsHandler taskManagerDetailsHandler = new TaskManagerDetailsHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			TaskManagerDetailsHeaders.getInstance(),
			resourceManagerRetriever,
			metricFetcher);

		final JobDetailsHandler jobDetailsHandler = new JobDetailsHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			JobDetailsHeaders.getInstance(),
			executionGraphCache,
			executor,
			metricFetcher);

		JobAccumulatorsHandler jobAccumulatorsHandler = new JobAccumulatorsHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			JobAccumulatorsHeaders.getInstance(),
			executionGraphCache,
			executor);

		SubtasksTimesHandler subtasksTimesHandler = new SubtasksTimesHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			SubtasksTimesHeaders.getInstance(),
			executionGraphCache,
			executor);

		final JobVertexMetricsHandler jobVertexMetricsHandler = new JobVertexMetricsHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			metricFetcher);

		final JobMetricsHandler jobMetricsHandler = new JobMetricsHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			metricFetcher);

		final SubtaskMetricsHandler subtaskMetricsHandler = new SubtaskMetricsHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			metricFetcher);

		final TaskManagerMetricsHandler taskManagerMetricsHandler = new TaskManagerMetricsHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			metricFetcher);

		final JobManagerMetricsHandler jobManagerMetricsHandler = new JobManagerMetricsHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			metricFetcher);

		final File tmpDir = restConfiguration.getTmpDir();

		Optional<StaticFileServerHandler<T>> optWebContent;

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
		handlers.add(Tuple2.of(DashboardConfigurationHeaders.getInstance(), dashboardConfigHandler));
		handlers.add(Tuple2.of(JobIdsWithStatusesOverviewHeaders.getInstance(), jobIdsHandler));
		handlers.add(Tuple2.of(JobsOverviewHeaders.getInstance(), jobsOverviewHandler));
		handlers.add(Tuple2.of(JobConfigHeaders.getInstance(), jobConfigHandler));
		handlers.add(Tuple2.of(CheckpointConfigHeaders.getInstance(), checkpointConfigHandler));
		handlers.add(Tuple2.of(CheckpointingStatisticsHeaders.getInstance(), checkpointStatisticsHandler));
		handlers.add(Tuple2.of(CheckpointStatisticDetailsHeaders.getInstance(), checkpointStatisticDetailsHandler));
		handlers.add(Tuple2.of(JobPlanHeaders.getInstance(), jobPlanHandler));
		handlers.add(Tuple2.of(TaskCheckpointStatisticsHeaders.getInstance(), taskCheckpointStatisticDetailsHandler));
		handlers.add(Tuple2.of(JobExceptionsHeaders.getInstance(), jobExceptionsHandler));
		handlers.add(Tuple2.of(JobVertexAccumulatorsHeaders.getInstance(), jobVertexAccumulatorsHandler));
		handlers.add(Tuple2.of(JobDetailsHeaders.getInstance(), jobDetailsHandler));
		handlers.add(Tuple2.of(JobAccumulatorsHeaders.getInstance(), jobAccumulatorsHandler));
		handlers.add(Tuple2.of(TaskManagersHeaders.getInstance(), taskManagersHandler));
		handlers.add(Tuple2.of(TaskManagerDetailsHeaders.getInstance(), taskManagerDetailsHandler));
		handlers.add(Tuple2.of(SubtasksTimesHeaders.getInstance(), subtasksTimesHandler));
		handlers.add(Tuple2.of(JobVertexMetricsHeaders.getInstance(), jobVertexMetricsHandler));
		handlers.add(Tuple2.of(JobMetricsHeaders.getInstance(), jobMetricsHandler));
		handlers.add(Tuple2.of(SubtaskMetricsHeaders.getInstance(), subtaskMetricsHandler));
		handlers.add(Tuple2.of(TaskManagerMetricsHeaders.getInstance(), taskManagerMetricsHandler));
		handlers.add(Tuple2.of(JobManagerMetricsHeaders.getInstance(), jobManagerMetricsHandler));

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
