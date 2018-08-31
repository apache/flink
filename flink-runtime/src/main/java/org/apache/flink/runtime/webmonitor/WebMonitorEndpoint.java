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
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.TransientBlobService;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.RestServerEndpoint;
import org.apache.flink.runtime.rest.RestServerEndpointConfiguration;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;
import org.apache.flink.runtime.rest.handler.cluster.ClusterConfigHandler;
import org.apache.flink.runtime.rest.handler.cluster.ClusterOverviewHandler;
import org.apache.flink.runtime.rest.handler.cluster.DashboardConfigHandler;
import org.apache.flink.runtime.rest.handler.cluster.ShutdownHandler;
import org.apache.flink.runtime.rest.handler.job.JobAccumulatorsHandler;
import org.apache.flink.runtime.rest.handler.job.JobConfigHandler;
import org.apache.flink.runtime.rest.handler.job.JobDetailsHandler;
import org.apache.flink.runtime.rest.handler.job.JobExceptionsHandler;
import org.apache.flink.runtime.rest.handler.job.JobExecutionResultHandler;
import org.apache.flink.runtime.rest.handler.job.JobIdsHandler;
import org.apache.flink.runtime.rest.handler.job.JobPlanHandler;
import org.apache.flink.runtime.rest.handler.job.JobTerminationHandler;
import org.apache.flink.runtime.rest.handler.job.JobVertexAccumulatorsHandler;
import org.apache.flink.runtime.rest.handler.job.JobVertexBackPressureHandler;
import org.apache.flink.runtime.rest.handler.job.JobVertexDetailsHandler;
import org.apache.flink.runtime.rest.handler.job.JobVertexTaskManagersHandler;
import org.apache.flink.runtime.rest.handler.job.JobsOverviewHandler;
import org.apache.flink.runtime.rest.handler.job.SubtaskCurrentAttemptDetailsHandler;
import org.apache.flink.runtime.rest.handler.job.SubtaskExecutionAttemptAccumulatorsHandler;
import org.apache.flink.runtime.rest.handler.job.SubtaskExecutionAttemptDetailsHandler;
import org.apache.flink.runtime.rest.handler.job.SubtasksAllAccumulatorsHandler;
import org.apache.flink.runtime.rest.handler.job.SubtasksTimesHandler;
import org.apache.flink.runtime.rest.handler.job.checkpoints.CheckpointConfigHandler;
import org.apache.flink.runtime.rest.handler.job.checkpoints.CheckpointStatisticDetailsHandler;
import org.apache.flink.runtime.rest.handler.job.checkpoints.CheckpointStatsCache;
import org.apache.flink.runtime.rest.handler.job.checkpoints.CheckpointingStatisticsHandler;
import org.apache.flink.runtime.rest.handler.job.checkpoints.TaskCheckpointStatisticDetailsHandler;
import org.apache.flink.runtime.rest.handler.job.metrics.AggregatingJobsMetricsHandler;
import org.apache.flink.runtime.rest.handler.job.metrics.AggregatingSubtasksMetricsHandler;
import org.apache.flink.runtime.rest.handler.job.metrics.AggregatingTaskManagersMetricsHandler;
import org.apache.flink.runtime.rest.handler.job.metrics.JobManagerMetricsHandler;
import org.apache.flink.runtime.rest.handler.job.metrics.JobMetricsHandler;
import org.apache.flink.runtime.rest.handler.job.metrics.JobVertexMetricsHandler;
import org.apache.flink.runtime.rest.handler.job.metrics.SubtaskMetricsHandler;
import org.apache.flink.runtime.rest.handler.job.metrics.TaskManagerMetricsHandler;
import org.apache.flink.runtime.rest.handler.job.rescaling.RescalingHandlers;
import org.apache.flink.runtime.rest.handler.job.savepoints.SavepointDisposalHandlers;
import org.apache.flink.runtime.rest.handler.job.savepoints.SavepointHandlers;
import org.apache.flink.runtime.rest.handler.legacy.ConstantTextHandler;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.files.LogFileHandlerSpecification;
import org.apache.flink.runtime.rest.handler.legacy.files.StaticFileServerHandler;
import org.apache.flink.runtime.rest.handler.legacy.files.StdoutFileHandlerSpecification;
import org.apache.flink.runtime.rest.handler.legacy.files.WebContentHandlerSpecification;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.taskmanager.TaskManagerDetailsHandler;
import org.apache.flink.runtime.rest.handler.taskmanager.TaskManagerLogFileHandler;
import org.apache.flink.runtime.rest.handler.taskmanager.TaskManagerStdoutFileHandler;
import org.apache.flink.runtime.rest.handler.taskmanager.TaskManagersHandler;
import org.apache.flink.runtime.rest.messages.ClusterConfigurationInfoHeaders;
import org.apache.flink.runtime.rest.messages.ClusterOverviewHeaders;
import org.apache.flink.runtime.rest.messages.DashboardConfigurationHeaders;
import org.apache.flink.runtime.rest.messages.JobAccumulatorsHeaders;
import org.apache.flink.runtime.rest.messages.JobConfigHeaders;
import org.apache.flink.runtime.rest.messages.JobExceptionsHeaders;
import org.apache.flink.runtime.rest.messages.JobIdsWithStatusesOverviewHeaders;
import org.apache.flink.runtime.rest.messages.JobPlanHeaders;
import org.apache.flink.runtime.rest.messages.JobTerminationHeaders;
import org.apache.flink.runtime.rest.messages.JobVertexAccumulatorsHeaders;
import org.apache.flink.runtime.rest.messages.JobVertexBackPressureHeaders;
import org.apache.flink.runtime.rest.messages.JobVertexDetailsHeaders;
import org.apache.flink.runtime.rest.messages.JobVertexTaskManagersHeaders;
import org.apache.flink.runtime.rest.messages.JobsOverviewHeaders;
import org.apache.flink.runtime.rest.messages.SubtasksAllAccumulatorsHeaders;
import org.apache.flink.runtime.rest.messages.SubtasksTimesHeaders;
import org.apache.flink.runtime.rest.messages.TerminationModeQueryParameter;
import org.apache.flink.runtime.rest.messages.YarnCancelJobTerminationHeaders;
import org.apache.flink.runtime.rest.messages.YarnStopJobTerminationHeaders;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointConfigHeaders;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointStatisticDetailsHeaders;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointingStatisticsHeaders;
import org.apache.flink.runtime.rest.messages.checkpoints.TaskCheckpointStatisticsHeaders;
import org.apache.flink.runtime.rest.messages.cluster.ShutdownHeaders;
import org.apache.flink.runtime.rest.messages.job.JobDetailsHeaders;
import org.apache.flink.runtime.rest.messages.job.SubtaskCurrentAttemptDetailsHeaders;
import org.apache.flink.runtime.rest.messages.job.SubtaskExecutionAttemptAccumulatorsHeaders;
import org.apache.flink.runtime.rest.messages.job.SubtaskExecutionAttemptDetailsHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerDetailsHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerLogFileHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerStdoutFileHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersHeaders;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Rest endpoint which serves the web frontend REST calls.
 *
 * @param <T> type of the leader gateway
 */
public class WebMonitorEndpoint<T extends RestfulGateway> extends RestServerEndpoint implements LeaderContender, JsonArchivist {

	protected final GatewayRetriever<? extends T> leaderRetriever;
	protected final Configuration clusterConfiguration;
	protected final RestHandlerConfiguration restConfiguration;
	private final GatewayRetriever<ResourceManagerGateway> resourceManagerRetriever;
	private final TransientBlobService transientBlobService;
	protected final Executor executor;

	private final ExecutionGraphCache executionGraphCache;
	private final CheckpointStatsCache checkpointStatsCache;

	private final MetricFetcher<? extends T> metricFetcher;

	private final LeaderElectionService leaderElectionService;

	private final FatalErrorHandler fatalErrorHandler;

	private boolean hasWebUI = false;

	private final Collection<JsonArchivist> archivingHandlers = new ArrayList<>(16);

	public WebMonitorEndpoint(
			RestServerEndpointConfiguration endpointConfiguration,
			GatewayRetriever<? extends T> leaderRetriever,
			Configuration clusterConfiguration,
			RestHandlerConfiguration restConfiguration,
			GatewayRetriever<ResourceManagerGateway> resourceManagerRetriever,
			TransientBlobService transientBlobService,
			Executor executor,
			MetricQueryServiceRetriever metricQueryServiceRetriever,
			LeaderElectionService leaderElectionService,
			FatalErrorHandler fatalErrorHandler) throws IOException {
		super(endpointConfiguration);
		this.leaderRetriever = Preconditions.checkNotNull(leaderRetriever);
		this.clusterConfiguration = Preconditions.checkNotNull(clusterConfiguration);
		this.restConfiguration = Preconditions.checkNotNull(restConfiguration);
		this.resourceManagerRetriever = Preconditions.checkNotNull(resourceManagerRetriever);
		this.transientBlobService = Preconditions.checkNotNull(transientBlobService);
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

		this.leaderElectionService = Preconditions.checkNotNull(leaderElectionService);
		this.fatalErrorHandler = Preconditions.checkNotNull(fatalErrorHandler);
	}

	@Override
	protected List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> initializeHandlers(CompletableFuture<String> restAddressFuture) {
		ArrayList<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers = new ArrayList<>(30);

		final Time timeout = restConfiguration.getTimeout();

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

		SubtasksAllAccumulatorsHandler subtasksAllAccumulatorsHandler = new SubtasksAllAccumulatorsHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			SubtasksAllAccumulatorsHeaders.getInstance(),
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

		final AggregatingTaskManagersMetricsHandler aggregatingTaskManagersMetricsHandler = new AggregatingTaskManagersMetricsHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			executor,
			metricFetcher
		);

		final AggregatingJobsMetricsHandler aggregatingJobsMetricsHandler = new AggregatingJobsMetricsHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			executor,
			metricFetcher
		);

		final AggregatingSubtasksMetricsHandler aggregatingSubtasksMetricsHandler = new AggregatingSubtasksMetricsHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			executor,
			metricFetcher
		);

		final JobVertexTaskManagersHandler jobVertexTaskManagersHandler = new JobVertexTaskManagersHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			JobVertexTaskManagersHeaders.getInstance(),
			executionGraphCache,
			executor,
			metricFetcher);

		final JobExecutionResultHandler jobExecutionResultHandler = new JobExecutionResultHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders);

		final String defaultSavepointDir = clusterConfiguration.getString(CheckpointingOptions.SAVEPOINT_DIRECTORY);

		final SavepointHandlers savepointHandlers = new SavepointHandlers(defaultSavepointDir);
		final SavepointHandlers.SavepointTriggerHandler savepointTriggerHandler = savepointHandlers.new SavepointTriggerHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders);

		final SavepointHandlers.SavepointStatusHandler savepointStatusHandler = savepointHandlers.new SavepointStatusHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders);

		final SubtaskExecutionAttemptDetailsHandler subtaskExecutionAttemptDetailsHandler = new SubtaskExecutionAttemptDetailsHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			SubtaskExecutionAttemptDetailsHeaders.getInstance(),
			executionGraphCache,
			executor,
			metricFetcher);

		final SubtaskExecutionAttemptAccumulatorsHandler subtaskExecutionAttemptAccumulatorsHandler = new SubtaskExecutionAttemptAccumulatorsHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			SubtaskExecutionAttemptAccumulatorsHeaders.getInstance(),
			executionGraphCache,
			executor
		);

		final SubtaskCurrentAttemptDetailsHandler subtaskCurrentAttemptDetailsHandler = new SubtaskCurrentAttemptDetailsHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			SubtaskCurrentAttemptDetailsHeaders.getInstance(),
			executionGraphCache,
			executor,
			metricFetcher);

		final RescalingHandlers rescalingHandlers = new RescalingHandlers();

		final RescalingHandlers.RescalingTriggerHandler rescalingTriggerHandler = rescalingHandlers.new RescalingTriggerHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders);

		final RescalingHandlers.RescalingStatusHandler rescalingStatusHandler = rescalingHandlers.new RescalingStatusHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders);

		JobVertexBackPressureHandler jobVertexBackPressureHandler = new JobVertexBackPressureHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			JobVertexBackPressureHeaders.getInstance());

		final JobTerminationHandler jobCancelTerminationHandler = new JobTerminationHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			JobTerminationHeaders.getInstance(),
			TerminationModeQueryParameter.TerminationMode.CANCEL);

		final JobTerminationHandler jobStopTerminationHandler = new JobTerminationHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			JobTerminationHeaders.getInstance(),
			TerminationModeQueryParameter.TerminationMode.STOP);

		final JobVertexDetailsHandler jobVertexDetailsHandler = new JobVertexDetailsHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			JobVertexDetailsHeaders.getInstance(),
			executionGraphCache,
			executor,
			metricFetcher);

		final SavepointDisposalHandlers savepointDisposalHandlers = new SavepointDisposalHandlers();

		final SavepointDisposalHandlers.SavepointDisposalTriggerHandler savepointDisposalTriggerHandler = savepointDisposalHandlers.new SavepointDisposalTriggerHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders);

		final SavepointDisposalHandlers.SavepointDisposalStatusHandler savepointDisposalStatusHandler = savepointDisposalHandlers.new SavepointDisposalStatusHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders);

		final ShutdownHandler shutdownHandler = new ShutdownHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			ShutdownHeaders.getInstance());

		final File webUiDir = restConfiguration.getWebUiDir();

		Optional<StaticFileServerHandler<T>> optWebContent;

		try {
			optWebContent = WebMonitorUtils.tryLoadWebContent(
				leaderRetriever,
				restAddressFuture,
				timeout,
				webUiDir);
		} catch (IOException e) {
			log.warn("Could not load web content handler.", e);
			optWebContent = Optional.empty();
		}

		handlers.add(Tuple2.of(clusterOverviewHandler.getMessageHeaders(), clusterOverviewHandler));
		handlers.add(Tuple2.of(clusterConfigurationHandler.getMessageHeaders(), clusterConfigurationHandler));
		handlers.add(Tuple2.of(dashboardConfigHandler.getMessageHeaders(), dashboardConfigHandler));
		handlers.add(Tuple2.of(jobIdsHandler.getMessageHeaders(), jobIdsHandler));
		handlers.add(Tuple2.of(jobsOverviewHandler.getMessageHeaders(), jobsOverviewHandler));
		handlers.add(Tuple2.of(jobConfigHandler.getMessageHeaders(), jobConfigHandler));
		handlers.add(Tuple2.of(checkpointConfigHandler.getMessageHeaders(), checkpointConfigHandler));
		handlers.add(Tuple2.of(checkpointStatisticsHandler.getMessageHeaders(), checkpointStatisticsHandler));
		handlers.add(Tuple2.of(checkpointStatisticDetailsHandler.getMessageHeaders(), checkpointStatisticDetailsHandler));
		handlers.add(Tuple2.of(jobPlanHandler.getMessageHeaders(), jobPlanHandler));
		handlers.add(Tuple2.of(taskCheckpointStatisticDetailsHandler.getMessageHeaders(), taskCheckpointStatisticDetailsHandler));
		handlers.add(Tuple2.of(jobExceptionsHandler.getMessageHeaders(), jobExceptionsHandler));
		handlers.add(Tuple2.of(jobVertexAccumulatorsHandler.getMessageHeaders(), jobVertexAccumulatorsHandler));
		handlers.add(Tuple2.of(subtasksAllAccumulatorsHandler.getMessageHeaders(), subtasksAllAccumulatorsHandler));
		handlers.add(Tuple2.of(jobDetailsHandler.getMessageHeaders(), jobDetailsHandler));
		handlers.add(Tuple2.of(jobAccumulatorsHandler.getMessageHeaders(), jobAccumulatorsHandler));
		handlers.add(Tuple2.of(taskManagersHandler.getMessageHeaders(), taskManagersHandler));
		handlers.add(Tuple2.of(taskManagerDetailsHandler.getMessageHeaders(), taskManagerDetailsHandler));
		handlers.add(Tuple2.of(subtasksTimesHandler.getMessageHeaders(), subtasksTimesHandler));
		handlers.add(Tuple2.of(jobVertexMetricsHandler.getMessageHeaders(), jobVertexMetricsHandler));
		handlers.add(Tuple2.of(jobMetricsHandler.getMessageHeaders(), jobMetricsHandler));
		handlers.add(Tuple2.of(subtaskMetricsHandler.getMessageHeaders(), subtaskMetricsHandler));
		handlers.add(Tuple2.of(taskManagerMetricsHandler.getMessageHeaders(), taskManagerMetricsHandler));
		handlers.add(Tuple2.of(jobManagerMetricsHandler.getMessageHeaders(), jobManagerMetricsHandler));
		handlers.add(Tuple2.of(aggregatingTaskManagersMetricsHandler.getMessageHeaders(), aggregatingTaskManagersMetricsHandler));
		handlers.add(Tuple2.of(aggregatingJobsMetricsHandler.getMessageHeaders(), aggregatingJobsMetricsHandler));
		handlers.add(Tuple2.of(aggregatingSubtasksMetricsHandler.getMessageHeaders(), aggregatingSubtasksMetricsHandler));
		handlers.add(Tuple2.of(jobExecutionResultHandler.getMessageHeaders(), jobExecutionResultHandler));
		handlers.add(Tuple2.of(savepointTriggerHandler.getMessageHeaders(), savepointTriggerHandler));
		handlers.add(Tuple2.of(savepointStatusHandler.getMessageHeaders(), savepointStatusHandler));
		handlers.add(Tuple2.of(subtaskExecutionAttemptDetailsHandler.getMessageHeaders(), subtaskExecutionAttemptDetailsHandler));
		handlers.add(Tuple2.of(subtaskExecutionAttemptAccumulatorsHandler.getMessageHeaders(), subtaskExecutionAttemptAccumulatorsHandler));
		handlers.add(Tuple2.of(subtaskCurrentAttemptDetailsHandler.getMessageHeaders(), subtaskCurrentAttemptDetailsHandler));
		handlers.add(Tuple2.of(jobVertexTaskManagersHandler.getMessageHeaders(), jobVertexTaskManagersHandler));
		handlers.add(Tuple2.of(jobVertexBackPressureHandler.getMessageHeaders(), jobVertexBackPressureHandler));
		handlers.add(Tuple2.of(jobCancelTerminationHandler.getMessageHeaders(), jobCancelTerminationHandler));
		handlers.add(Tuple2.of(jobVertexDetailsHandler.getMessageHeaders(), jobVertexDetailsHandler));
		handlers.add(Tuple2.of(rescalingTriggerHandler.getMessageHeaders(), rescalingTriggerHandler));
		handlers.add(Tuple2.of(rescalingStatusHandler.getMessageHeaders(), rescalingStatusHandler));
		handlers.add(Tuple2.of(savepointDisposalTriggerHandler.getMessageHeaders(), savepointDisposalTriggerHandler));
		handlers.add(Tuple2.of(savepointDisposalStatusHandler.getMessageHeaders(), savepointDisposalStatusHandler));

		// TODO: Remove once the Yarn proxy can forward all REST verbs
		handlers.add(Tuple2.of(YarnCancelJobTerminationHeaders.getInstance(), jobCancelTerminationHandler));
		handlers.add(Tuple2.of(YarnStopJobTerminationHeaders.getInstance(), jobStopTerminationHandler));

		handlers.add(Tuple2.of(shutdownHandler.getMessageHeaders(), shutdownHandler));

		optWebContent.ifPresent(
			webContent -> {
				handlers.add(Tuple2.of(WebContentHandlerSpecification.getInstance(), webContent));
				hasWebUI = true;
			});

		// load the log and stdout file handler for the main cluster component
		final WebMonitorUtils.LogFileLocation logFileLocation = WebMonitorUtils.LogFileLocation.find(clusterConfiguration);

		final ChannelInboundHandler logFileHandler = createStaticFileHandler(
			restAddressFuture,
			timeout,
			logFileLocation.logFile);

		final ChannelInboundHandler stdoutFileHandler = createStaticFileHandler(
			restAddressFuture,
			timeout,
			logFileLocation.stdOutFile);

		handlers.add(Tuple2.of(LogFileHandlerSpecification.getInstance(), logFileHandler));
		handlers.add(Tuple2.of(StdoutFileHandlerSpecification.getInstance(), stdoutFileHandler));

		// TaskManager log and stdout file handler

		final Time cacheEntryDuration = Time.milliseconds(restConfiguration.getRefreshInterval());

		final TaskManagerLogFileHandler taskManagerLogFileHandler = new TaskManagerLogFileHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			TaskManagerLogFileHeaders.getInstance(),
			resourceManagerRetriever,
			transientBlobService,
			cacheEntryDuration);

		final TaskManagerStdoutFileHandler taskManagerStdoutFileHandler = new TaskManagerStdoutFileHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			TaskManagerStdoutFileHeaders.getInstance(),
			resourceManagerRetriever,
			transientBlobService,
			cacheEntryDuration);

		handlers.add(Tuple2.of(TaskManagerLogFileHeaders.getInstance(), taskManagerLogFileHandler));
		handlers.add(Tuple2.of(TaskManagerStdoutFileHeaders.getInstance(), taskManagerStdoutFileHandler));

		handlers.stream()
			.map(tuple -> tuple.f1)
			.filter(handler -> handler instanceof JsonArchivist)
			.forEachOrdered(handler -> archivingHandlers.add((JsonArchivist) handler));

		return handlers;
	}

	@Nonnull
	private ChannelInboundHandler createStaticFileHandler(
			CompletableFuture<String> restAddressFuture,
			Time timeout,
			File fileToServe) {

		if (fileToServe == null) {
			return new ConstantTextHandler("(file unavailable)");
		} else {
			try {
				return new StaticFileServerHandler<>(
					leaderRetriever,
					restAddressFuture,
					timeout,
					fileToServe);
			} catch (IOException e) {
				log.info("Cannot load log file handler.", e);
				return new ConstantTextHandler("(log file unavailable)");
			}
		}
	}

	@Override
	public void startInternal() throws Exception {
		leaderElectionService.start(this);
		if (hasWebUI) {
			log.info("Web frontend listening at {}.", getRestBaseUrl());
		}
	}

	@Override
	protected CompletableFuture<Void> shutDownInternal() {
		executionGraphCache.close();

		final CompletableFuture<Void> shutdownFuture = super.shutDownInternal();

		final File webUiDir = restConfiguration.getWebUiDir();

		return FutureUtils.runAfterwardsAsync(
			shutdownFuture,
			() -> {
				Exception exception = null;
				try {
					log.info("Removing cache directory {}", webUiDir);
					FileUtils.deleteDirectory(webUiDir);
				} catch (Exception e) {
					exception = e;
				}

				try {
					leaderElectionService.stop();
				} catch (Exception e) {
					exception = ExceptionUtils.firstOrSuppressed(e, exception);
				}

				if (exception != null) {
					throw exception;
				}
			});
	}

	//-------------------------------------------------------------------------
	// LeaderContender
	//-------------------------------------------------------------------------

	@Override
	public void grantLeadership(final UUID leaderSessionID) {
		log.info("{} was granted leadership with leaderSessionID={}", getRestBaseUrl(), leaderSessionID);
		leaderElectionService.confirmLeaderSessionID(leaderSessionID);
	}

	@Override
	public void revokeLeadership() {
		log.info("{} lost leadership", getRestBaseUrl());
	}

	@Override
	public String getAddress() {
		return getRestBaseUrl();
	}

	@Override
	public void handleError(final Exception exception) {
		fatalErrorHandler.onFatalError(exception);
	}

	@Override
	public Collection<ArchivedJson> archiveJsonWithPath(AccessExecutionGraph graph) throws IOException {
		Collection<ArchivedJson> archivedJson = new ArrayList<>(archivingHandlers.size());
		for (JsonArchivist archivist : archivingHandlers) {
			Collection<ArchivedJson> subArchive = archivist.archiveJsonWithPath(graph);
			archivedJson.addAll(subArchive);
		}
		return archivedJson;
	}
}
