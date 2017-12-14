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
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.dispatcher.DispatcherRestEndpoint;
import org.apache.flink.runtime.dispatcher.StandaloneDispatcher;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.rest.RestServerEndpointConfiguration;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;
import org.apache.flink.runtime.webmonitor.retriever.impl.AkkaQueryServiceRetriever;
import org.apache.flink.runtime.webmonitor.retriever.impl.RpcGatewayRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import akka.actor.ActorSystem;

import javax.annotation.Nullable;

import java.util.concurrent.Executor;

/**
 * Base class for session cluster entry points.
 */
public abstract class SessionClusterEntrypoint extends ClusterEntrypoint {

	private ResourceManager<?> resourceManager;

	private Dispatcher dispatcher;

	private LeaderRetrievalService dispatcherLeaderRetrievalService;

	private LeaderRetrievalService resourceManagerRetrievalService;

	private DispatcherRestEndpoint dispatcherRestEndpoint;

	public SessionClusterEntrypoint(Configuration configuration) {
		super(configuration);
	}

	@Override
	protected void startClusterComponents(
			Configuration configuration,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			BlobServer blobServer,
			HeartbeatServices heartbeatServices,
			MetricRegistry metricRegistry) throws Exception {

		dispatcherLeaderRetrievalService = highAvailabilityServices.getDispatcherLeaderRetriever();

		resourceManagerRetrievalService = highAvailabilityServices.getResourceManagerLeaderRetriever();

		LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever = new RpcGatewayRetriever<>(
			rpcService,
			DispatcherGateway.class,
			DispatcherId::new,
			10,
			Time.milliseconds(50L));

		LeaderGatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever = new RpcGatewayRetriever<>(
			rpcService,
			ResourceManagerGateway.class,
			ResourceManagerId::new,
			10,
			Time.milliseconds(50L));

		// TODO: Remove once we have ported the MetricFetcher to the RpcEndpoint
		final ActorSystem actorSystem = ((AkkaRpcService) rpcService).getActorSystem();
		final Time timeout = Time.milliseconds(configuration.getLong(WebOptions.TIMEOUT));

		dispatcherRestEndpoint = createDispatcherRestEndpoint(
			configuration,
			dispatcherGatewayRetriever,
			resourceManagerGatewayRetriever,
			rpcService.getExecutor(),
			new AkkaQueryServiceRetriever(actorSystem, timeout));

		LOG.debug("Starting Dispatcher REST endpoint.");
		dispatcherRestEndpoint.start();

		resourceManager = createResourceManager(
			configuration,
			ResourceID.generate(),
			rpcService,
			highAvailabilityServices,
			heartbeatServices,
			metricRegistry,
			this,
			dispatcherRestEndpoint.getRestAddress());

		dispatcher = createDispatcher(
			configuration,
			rpcService,
			highAvailabilityServices,
			resourceManager.getSelfGateway(ResourceManagerGateway.class),
			blobServer,
			heartbeatServices,
			metricRegistry,
			this,
			dispatcherRestEndpoint.getRestAddress());

		LOG.debug("Starting ResourceManager.");
		resourceManager.start();
		resourceManagerRetrievalService.start(resourceManagerGatewayRetriever);

		LOG.debug("Starting Dispatcher.");
		dispatcher.start();
		dispatcherLeaderRetrievalService.start(dispatcherGatewayRetriever);
	}

	@Override
	protected void stopClusterComponents(boolean cleanupHaData) throws Exception {
		Throwable exception = null;

		if (dispatcherRestEndpoint != null) {
			dispatcherRestEndpoint.shutdown(Time.seconds(10L));
		}

		if (dispatcherLeaderRetrievalService != null) {
			try {
				dispatcherLeaderRetrievalService.stop();
			} catch (Throwable t) {
				exception = ExceptionUtils.firstOrSuppressed(t, exception);
			}
		}

		if (dispatcher != null) {
			try {
				dispatcher.shutDown();
			} catch (Throwable t) {
				exception = ExceptionUtils.firstOrSuppressed(t, exception);
			}
		}

		if (resourceManagerRetrievalService != null) {
			try {
				resourceManagerRetrievalService.stop();
			} catch (Throwable t) {
				exception = ExceptionUtils.firstOrSuppressed(t, exception);
			}
		}

		if (resourceManager != null) {
			try {
				resourceManager.shutDown();
			} catch (Throwable t) {
				exception = ExceptionUtils.firstOrSuppressed(t, exception);
			}
		}

		if (exception != null) {
			throw new FlinkException("Could not properly shut down the session cluster entry point.", exception);
		}
	}

	protected DispatcherRestEndpoint createDispatcherRestEndpoint(
			Configuration configuration,
			LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever,
			LeaderGatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
			Executor executor,
			MetricQueryServiceRetriever metricQueryServiceRetriever) throws Exception {

		final RestHandlerConfiguration restHandlerConfiguration = RestHandlerConfiguration.fromConfiguration(configuration);

		return new DispatcherRestEndpoint(
			RestServerEndpointConfiguration.fromConfiguration(configuration),
			dispatcherGatewayRetriever,
			configuration,
			restHandlerConfiguration,
			resourceManagerGatewayRetriever,
			executor,
			metricQueryServiceRetriever);
	}

	protected Dispatcher createDispatcher(
		Configuration configuration,
		RpcService rpcService,
		HighAvailabilityServices highAvailabilityServices,
		ResourceManagerGateway resourceManagerGateway,
		BlobServer blobServer,
		HeartbeatServices heartbeatServices,
		MetricRegistry metricRegistry,
		FatalErrorHandler fatalErrorHandler,
		@Nullable String restAddress) throws Exception {

		// create the default dispatcher
		return new StandaloneDispatcher(
			rpcService,
			Dispatcher.DISPATCHER_NAME,
			configuration,
			highAvailabilityServices,
			resourceManagerGateway,
			blobServer,
			heartbeatServices,
			metricRegistry,
			fatalErrorHandler,
			restAddress);
	}

	protected abstract ResourceManager<?> createResourceManager(
		Configuration configuration,
		ResourceID resourceId,
		RpcService rpcService,
		HighAvailabilityServices highAvailabilityServices,
		HeartbeatServices heartbeatServices,
		MetricRegistry metricRegistry,
		FatalErrorHandler fatalErrorHandler,
		@Nullable String webInterfaceUrl) throws Exception;
}
