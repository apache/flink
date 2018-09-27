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
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.dispatcher.ArchivedExecutionGraphStore;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherFactory;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.dispatcher.HistoryServerArchivist;
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
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.webmonitor.WebMonitorEndpoint;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;
import org.apache.flink.runtime.webmonitor.retriever.impl.AkkaQueryServiceRetriever;
import org.apache.flink.runtime.webmonitor.retriever.impl.RpcGatewayRetriever;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import akka.actor.ActorSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Component which starts a {@link Dispatcher}, {@link ResourceManager} and {@link WebMonitorEndpoint}
 * in the same process.
 */
public class ClusterComponent<T extends Dispatcher> implements AutoCloseableAsync {

	private static final Logger LOG = LoggerFactory.getLogger(ClusterComponent.class);

	private final Object lock = new Object();

	private final DispatcherFactory<T> dispatcherFactory;

	private final ResourceManagerFactory<?> resourceManagerFactory;

	private final RestEndpointFactory<?> restEndpointFactory;

	private final CompletableFuture<Void> terminationFuture;

	private final CompletableFuture<ApplicationStatus> shutDownFuture;

	@GuardedBy("lock")
	private State state;

	@GuardedBy("lock")
	private ResourceManager<?> resourceManager;

	@GuardedBy("lock")
	private T dispatcher;

	@GuardedBy("lock")
	private LeaderRetrievalService dispatcherLeaderRetrievalService;

	@GuardedBy("lock")
	private LeaderRetrievalService resourceManagerRetrievalService;

	@GuardedBy("lock")
	private WebMonitorEndpoint<?> webMonitorEndpoint;

	@GuardedBy("lock")
	private JobManagerMetricGroup jobManagerMetricGroup;

	public ClusterComponent(
			DispatcherFactory<T> dispatcherFactory,
			ResourceManagerFactory<?> resourceManagerFactory,
			RestEndpointFactory<?> restEndpointFactory) {
		this.dispatcherFactory = dispatcherFactory;
		this.resourceManagerFactory = resourceManagerFactory;
		this.restEndpointFactory = restEndpointFactory;
		this.terminationFuture = new CompletableFuture<>();
		this.shutDownFuture = new CompletableFuture<>();
		this.state = State.CREATED;

		terminationFuture.whenComplete(
			(aVoid, throwable) -> {
				if (throwable != null) {
					shutDownFuture.completeExceptionally(throwable);
				} else {
					shutDownFuture.complete(ApplicationStatus.SUCCEEDED);
				}
			});
	}

	public T getDispatcher() {
		synchronized (lock) {
			return dispatcher;
		}
	}

	public CompletableFuture<Void> getTerminationFuture() {
		return terminationFuture;
	}

	public CompletableFuture<ApplicationStatus> getShutDownFuture() {
		return shutDownFuture;
	}

	public void startComponent(
			Configuration configuration,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			BlobServer blobServer,
			HeartbeatServices heartbeatServices,
			MetricRegistry metricRegistry,
			ArchivedExecutionGraphStore archivedExecutionGraphStore,
			FatalErrorHandler fatalErrorHandler) throws Exception {
		synchronized (lock) {
			Preconditions.checkState(state == State.CREATED);
			state = State.RUNNING;

			dispatcherLeaderRetrievalService = highAvailabilityServices.getDispatcherLeaderRetriever();

			resourceManagerRetrievalService = highAvailabilityServices.getResourceManagerLeaderRetriever();

			LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever = new RpcGatewayRetriever<>(
				rpcService,
				DispatcherGateway.class,
				DispatcherId::fromUuid,
				10,
				Time.milliseconds(50L));

			LeaderGatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever = new RpcGatewayRetriever<>(
				rpcService,
				ResourceManagerGateway.class,
				ResourceManagerId::fromUuid,
				10,
				Time.milliseconds(50L));

			// TODO: Remove once we have ported the MetricFetcher to the RpcEndpoint
			final ActorSystem actorSystem = ((AkkaRpcService) rpcService).getActorSystem();
			final Time timeout = Time.milliseconds(configuration.getLong(WebOptions.TIMEOUT));

			webMonitorEndpoint = restEndpointFactory.createRestEndpoint(
				configuration,
				dispatcherGatewayRetriever,
				resourceManagerGatewayRetriever,
				blobServer,
				rpcService.getExecutor(),
				new AkkaQueryServiceRetriever(actorSystem, timeout),
				highAvailabilityServices.getWebMonitorLeaderElectionService(),
				fatalErrorHandler);

			LOG.debug("Starting Dispatcher REST endpoint.");
			webMonitorEndpoint.start();

			resourceManager = resourceManagerFactory.createResourceManager(
				configuration,
				ResourceID.generate(),
				rpcService,
				highAvailabilityServices,
				heartbeatServices,
				metricRegistry,
				fatalErrorHandler,
				new ClusterInformation(rpcService.getAddress(), blobServer.getPort()),
				webMonitorEndpoint.getRestBaseUrl());

			jobManagerMetricGroup = MetricUtils.instantiateJobManagerMetricGroup(
				metricRegistry,
				rpcService.getAddress(),
				ConfigurationUtils.getSystemResourceMetricsProbingInterval(configuration));

			final HistoryServerArchivist historyServerArchivist = HistoryServerArchivist.createHistoryServerArchivist(configuration, webMonitorEndpoint);

			dispatcher = dispatcherFactory.createDispatcher(
				configuration,
				rpcService,
				highAvailabilityServices,
				resourceManager.getSelfGateway(ResourceManagerGateway.class),
				blobServer,
				heartbeatServices,
				jobManagerMetricGroup,
				metricRegistry.getMetricQueryServicePath(),
				archivedExecutionGraphStore,
				fatalErrorHandler,
				webMonitorEndpoint.getRestBaseUrl(),
				historyServerArchivist);

			registerShutDownFuture(dispatcher, shutDownFuture);

			LOG.debug("Starting ResourceManager.");
			resourceManager.start();
			resourceManagerRetrievalService.start(resourceManagerGatewayRetriever);

			LOG.debug("Starting Dispatcher.");
			dispatcher.start();
			dispatcherLeaderRetrievalService.start(dispatcherGatewayRetriever);
		}
	}

	protected void registerShutDownFuture(T dispatcher, CompletableFuture<ApplicationStatus> shutDownFuture) {
			dispatcher
				.getTerminationFuture()
				.whenComplete(
					(aVoid, throwable) -> {
						if (throwable != null) {
							shutDownFuture.completeExceptionally(throwable);
						} else {
							shutDownFuture.complete(ApplicationStatus.SUCCEEDED);
						}
					});
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		synchronized (lock) {
			if (state == State.RUNNING) {
				Exception exception = null;

				final Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(4);

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

				if (webMonitorEndpoint != null) {
					terminationFutures.add(webMonitorEndpoint.closeAsync());
				}

				if (dispatcher != null) {
					dispatcher.shutDown();
					terminationFutures.add(dispatcher.getTerminationFuture());
				}

				if (resourceManager != null) {
					resourceManager.shutDown();
					terminationFutures.add(resourceManager.getTerminationFuture());
				}

				if (exception != null) {
					terminationFutures.add(FutureUtils.completedExceptionally(exception));
				}

				final CompletableFuture<Void> componentTerminationFuture = FutureUtils.completeAll(terminationFutures);

				final CompletableFuture<Void> metricGroupTerminationFuture;

				if (jobManagerMetricGroup != null) {
					metricGroupTerminationFuture = FutureUtils.runAfterwards(
						componentTerminationFuture,
						() -> {
							synchronized (lock) {
								jobManagerMetricGroup.close();
							}
						});
				} else {
					metricGroupTerminationFuture = componentTerminationFuture;
				}

				metricGroupTerminationFuture.whenComplete((aVoid, throwable) -> {
					if (throwable != null) {
						terminationFuture.completeExceptionally(throwable);
					} else {
						terminationFuture.complete(aVoid);
					}
				});
			} else if (state == State.CREATED) {
				terminationFuture.complete(null);
			}

			state = State.TERMINATED;
			return terminationFuture;
		}
	}

	/**
	 * Deregister the Flink application from the resource management system by signalling
	 * the {@link ResourceManager}.
	 *
	 * @param applicationStatus to terminate the application with
	 * @param diagnostics additional information about the shut down, can be {@code null}
	 * @return Future which is completed once the shut down
	 */
	public CompletableFuture<Void> deregisterApplication(ApplicationStatus applicationStatus, @Nullable String diagnostics) {
		synchronized (lock) {
			if (resourceManager != null) {
				final ResourceManagerGateway selfGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);
				return selfGateway.deregisterApplication(applicationStatus, diagnostics).thenApply(ack -> null);
			} else {
				return CompletableFuture.completedFuture(null);
			}
		}
	}

	enum State {
		CREATED,
		RUNNING,
		TERMINATED
	}
}
