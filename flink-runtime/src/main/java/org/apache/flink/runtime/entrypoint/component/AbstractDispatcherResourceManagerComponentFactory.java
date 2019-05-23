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

package org.apache.flink.runtime.entrypoint.component;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.dispatcher.ArchivedExecutionGraphStore;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherFactory;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.dispatcher.HistoryServerArchivist;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.metrics.util.MetricUtils;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerFactory;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.rest.RestEndpointFactory;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcherImpl;
import org.apache.flink.runtime.rest.handler.legacy.metrics.VoidMetricFetcher;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.WebMonitorEndpoint;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;
import org.apache.flink.runtime.webmonitor.retriever.impl.RpcGatewayRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * Abstract class which implements the creation of the {@link DispatcherResourceManagerComponent} components.
 *
 * @param <T> type of the {@link Dispatcher}
 * @param <U> type of the {@link RestfulGateway} given to the {@link WebMonitorEndpoint}
 */
public abstract class AbstractDispatcherResourceManagerComponentFactory<T extends Dispatcher, U extends RestfulGateway> implements DispatcherResourceManagerComponentFactory<T> {

	private final Logger log = LoggerFactory.getLogger(getClass());

	@Nonnull
	private final DispatcherFactory<T> dispatcherFactory;

	@Nonnull
	private final ResourceManagerFactory<?> resourceManagerFactory;

	@Nonnull
	private final RestEndpointFactory<U> restEndpointFactory;

	public AbstractDispatcherResourceManagerComponentFactory(
			@Nonnull DispatcherFactory<T> dispatcherFactory,
			@Nonnull ResourceManagerFactory<?> resourceManagerFactory,
			@Nonnull RestEndpointFactory<U> restEndpointFactory) {
		this.dispatcherFactory = dispatcherFactory;
		this.resourceManagerFactory = resourceManagerFactory;
		this.restEndpointFactory = restEndpointFactory;
	}

	@Override
	public DispatcherResourceManagerComponent<T> create(
			Configuration configuration,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			BlobServer blobServer,
			HeartbeatServices heartbeatServices,
			MetricRegistry metricRegistry,
			ArchivedExecutionGraphStore archivedExecutionGraphStore,
			MetricQueryServiceRetriever metricQueryServiceRetriever,
			FatalErrorHandler fatalErrorHandler) throws Exception {

		LeaderRetrievalService dispatcherLeaderRetrievalService = null;
		LeaderRetrievalService resourceManagerRetrievalService = null;
		WebMonitorEndpoint<U> webMonitorEndpoint = null;
		ResourceManager<?> resourceManager = null;
		JobManagerMetricGroup jobManagerMetricGroup = null;
		T dispatcher = null;

		try {
			dispatcherLeaderRetrievalService = highAvailabilityServices.getDispatcherLeaderRetriever();

			resourceManagerRetrievalService = highAvailabilityServices.getResourceManagerLeaderRetriever();

			final LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever = new RpcGatewayRetriever<>(
				rpcService,
				DispatcherGateway.class,
				DispatcherId::fromUuid,
				10,
				Time.milliseconds(50L));

			final LeaderGatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever = new RpcGatewayRetriever<>(
				rpcService,
				ResourceManagerGateway.class,
				ResourceManagerId::fromUuid,
				10,
				Time.milliseconds(50L));

			final ExecutorService executor = WebMonitorEndpoint.createExecutorService(
				configuration.getInteger(RestOptions.SERVER_NUM_THREADS),
				configuration.getInteger(RestOptions.SERVER_THREAD_PRIORITY),
				"DispatcherRestEndpoint");

			final long updateInterval = configuration.getLong(MetricOptions.METRIC_FETCHER_UPDATE_INTERVAL);
			final MetricFetcher metricFetcher = updateInterval == 0
				? VoidMetricFetcher.INSTANCE
				: MetricFetcherImpl.fromConfiguration(
					configuration,
					metricQueryServiceRetriever,
					dispatcherGatewayRetriever,
					executor);

			webMonitorEndpoint = restEndpointFactory.createRestEndpoint(
				configuration,
				dispatcherGatewayRetriever,
				resourceManagerGatewayRetriever,
				blobServer,
				executor,
				metricFetcher,
				highAvailabilityServices.getWebMonitorLeaderElectionService(),
				fatalErrorHandler);

			log.debug("Starting Dispatcher REST endpoint.");
			webMonitorEndpoint.start();

			final String hostname = getHostname(rpcService);

			jobManagerMetricGroup = MetricUtils.instantiateJobManagerMetricGroup(
				metricRegistry,
				hostname,
				ConfigurationUtils.getSystemResourceMetricsProbingInterval(configuration));

			resourceManager = resourceManagerFactory.createResourceManager(
				configuration,
				ResourceID.generate(),
				rpcService,
				highAvailabilityServices,
				heartbeatServices,
				metricRegistry,
				fatalErrorHandler,
				new ClusterInformation(hostname, blobServer.getPort()),
				webMonitorEndpoint.getRestBaseUrl(),
				jobManagerMetricGroup);

			final HistoryServerArchivist historyServerArchivist = HistoryServerArchivist.createHistoryServerArchivist(configuration, webMonitorEndpoint);

			dispatcher = dispatcherFactory.createDispatcher(
				configuration,
				rpcService,
				highAvailabilityServices,
				resourceManagerGatewayRetriever,
				blobServer,
				heartbeatServices,
				jobManagerMetricGroup,
				metricRegistry.getMetricQueryServiceGatewayRpcAddress(),
				archivedExecutionGraphStore,
				fatalErrorHandler,
				historyServerArchivist);

			log.debug("Starting ResourceManager.");
			resourceManager.start();
			resourceManagerRetrievalService.start(resourceManagerGatewayRetriever);

			log.debug("Starting Dispatcher.");
			dispatcher.start();
			dispatcherLeaderRetrievalService.start(dispatcherGatewayRetriever);

			return createDispatcherResourceManagerComponent(
				dispatcher,
				resourceManager,
				dispatcherLeaderRetrievalService,
				resourceManagerRetrievalService,
				webMonitorEndpoint,
				jobManagerMetricGroup);

		} catch (Exception exception) {
			// clean up all started components
			if (dispatcherLeaderRetrievalService != null) {
				try {
					dispatcherLeaderRetrievalService.stop();
				} catch (Exception e) {
					exception = ExceptionUtils.firstOrSuppressed(e, exception);
				}
			}

			if (resourceManagerRetrievalService != null) {
				try {
					resourceManagerRetrievalService.stop();
				} catch (Exception e) {
					exception = ExceptionUtils.firstOrSuppressed(e, exception);
				}
			}

			final Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(3);

			if (webMonitorEndpoint != null) {
				terminationFutures.add(webMonitorEndpoint.closeAsync());
			}

			if (resourceManager != null) {
				terminationFutures.add(resourceManager.closeAsync());
			}

			if (dispatcher != null) {
				terminationFutures.add(dispatcher.closeAsync());
			}

			final FutureUtils.ConjunctFuture<Void> terminationFuture = FutureUtils.completeAll(terminationFutures);

			try {
				terminationFuture.get();
			} catch (Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}

			if (jobManagerMetricGroup != null) {
				jobManagerMetricGroup.close();
			}

			throw new FlinkException("Could not create the DispatcherResourceManagerComponent.", exception);
		}
	}

	protected String getHostname(RpcService rpcService) {
		final String rpcServiceAddress = rpcService.getAddress();
		return rpcServiceAddress != null && rpcServiceAddress.isEmpty() ? "localhost" : rpcServiceAddress;
	}

	protected abstract DispatcherResourceManagerComponent<T> createDispatcherResourceManagerComponent(
		T dispatcher,
		ResourceManager<?> resourceManager,
		LeaderRetrievalService dispatcherLeaderRetrievalService,
		LeaderRetrievalService resourceManagerRetrievalService,
		WebMonitorEndpoint<?> webMonitorEndpoint,
		JobManagerMetricGroup jobManagerMetricGroup);
}
