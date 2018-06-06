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

package org.apache.flink.runtime.entrypoint;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.TransientBlobService;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.dispatcher.ArchivedExecutionGraphStore;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherRestEndpoint;
import org.apache.flink.runtime.dispatcher.FileArchivedExecutionGraphStore;
import org.apache.flink.runtime.dispatcher.HistoryServerArchivist;
import org.apache.flink.runtime.dispatcher.StandaloneDispatcher;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.RestServerEndpointConfiguration;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;

import org.apache.flink.shaded.guava18.com.google.common.base.Ticker;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executor;

/**
 * Base class for session cluster entry points.
 */
public abstract class SessionClusterEntrypoint extends ClusterEntrypoint {

	public SessionClusterEntrypoint(Configuration configuration) {
		super(configuration);
	}

	@Override
	protected ArchivedExecutionGraphStore createSerializableExecutionGraphStore(
			Configuration configuration,
			ScheduledExecutor scheduledExecutor) throws IOException {
		final File tmpDir = new File(ConfigurationUtils.parseTempDirectories(configuration)[0]);

		final Time expirationTime =  Time.seconds(configuration.getLong(JobManagerOptions.JOB_STORE_EXPIRATION_TIME));
		final long maximumCacheSizeBytes = configuration.getLong(JobManagerOptions.JOB_STORE_CACHE_SIZE);

		return new FileArchivedExecutionGraphStore(
			tmpDir,
			expirationTime,
			maximumCacheSizeBytes,
			scheduledExecutor,
			Ticker.systemTicker());
	}

	@Override
	protected DispatcherRestEndpoint createRestEndpoint(
			Configuration configuration,
			LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever,
			LeaderGatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
			TransientBlobService transientBlobService,
			Executor executor,
			MetricQueryServiceRetriever metricQueryServiceRetriever,
			LeaderElectionService leaderElectionService) throws Exception {

		final RestHandlerConfiguration restHandlerConfiguration = RestHandlerConfiguration.fromConfiguration(configuration);

		return new DispatcherRestEndpoint(
			RestServerEndpointConfiguration.fromConfiguration(configuration),
			dispatcherGatewayRetriever,
			configuration,
			restHandlerConfiguration,
			resourceManagerGatewayRetriever,
			transientBlobService,
			executor,
			metricQueryServiceRetriever,
			leaderElectionService,
			this);
	}

	@Override
	protected Dispatcher createDispatcher(
			Configuration configuration,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			ResourceManagerGateway resourceManagerGateway,
			BlobServer blobServer,
			HeartbeatServices heartbeatServices,
			JobManagerMetricGroup jobManagerMetricGroup,
			@Nullable String metricQueryServicePath,
			ArchivedExecutionGraphStore archivedExecutionGraphStore,
			FatalErrorHandler fatalErrorHandler,
			@Nullable String restAddress,
			HistoryServerArchivist historyServerArchivist) throws Exception {

		// create the default dispatcher
		return new StandaloneDispatcher(
			rpcService,
			Dispatcher.DISPATCHER_NAME,
			configuration,
			highAvailabilityServices,
			resourceManagerGateway,
			blobServer,
			heartbeatServices,
			jobManagerMetricGroup,
			metricQueryServicePath,
			archivedExecutionGraphStore,
			Dispatcher.DefaultJobManagerRunnerFactory.INSTANCE,
			fatalErrorHandler,
			restAddress,
			historyServerArchivist);
	}
}
