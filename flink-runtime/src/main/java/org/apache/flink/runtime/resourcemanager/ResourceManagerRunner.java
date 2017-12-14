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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Simple {@link StandaloneResourceManager} runner. It instantiates the resource manager's services
 * and handles fatal errors by shutting the resource manager down.
 */
public class ResourceManagerRunner implements FatalErrorHandler {

	private static final Logger LOG = LoggerFactory.getLogger(ResourceManagerRunner.class);

	private final Object lock = new Object();

	private final ResourceManagerRuntimeServices resourceManagerRuntimeServices;

	private final ResourceManager<?> resourceManager;

	public ResourceManagerRunner(
			final ResourceID resourceId,
			final String resourceManagerEndpointId,
			final Configuration configuration,
			final RpcService rpcService,
			final HighAvailabilityServices highAvailabilityServices,
			final HeartbeatServices heartbeatServices,
			final MetricRegistry metricRegistry) throws Exception {

		Preconditions.checkNotNull(resourceId);
		Preconditions.checkNotNull(configuration);
		Preconditions.checkNotNull(rpcService);
		Preconditions.checkNotNull(highAvailabilityServices);
		Preconditions.checkNotNull(heartbeatServices);
		Preconditions.checkNotNull(metricRegistry);

		final ResourceManagerConfiguration resourceManagerConfiguration = ResourceManagerConfiguration.fromConfiguration(configuration);

		final ResourceManagerRuntimeServicesConfiguration resourceManagerRuntimeServicesConfiguration = ResourceManagerRuntimeServicesConfiguration.fromConfiguration(configuration);

		resourceManagerRuntimeServices = ResourceManagerRuntimeServices.fromConfiguration(
			resourceManagerRuntimeServicesConfiguration,
			highAvailabilityServices,
			rpcService.getScheduledExecutor());

		this.resourceManager = new StandaloneResourceManager(
			rpcService,
			resourceManagerEndpointId,
			resourceId,
			resourceManagerConfiguration,
			highAvailabilityServices,
			heartbeatServices,
			resourceManagerRuntimeServices.getSlotManager(),
			metricRegistry,
			resourceManagerRuntimeServices.getJobLeaderIdService(),
			this);
	}

	//-------------------------------------------------------------------------------------
	// Lifecycle management
	//-------------------------------------------------------------------------------------

	public void start() throws Exception {
		resourceManager.start();
	}

	public void shutDown() throws Exception {
		// wait for the completion
		shutDownInternally().get();
	}

	private CompletableFuture<Void> shutDownInternally() {
		synchronized (lock) {
			resourceManager.shutDown();

			return resourceManager.getTerminationFuture()
				.thenAccept(
					ignored -> {
						try {
							resourceManagerRuntimeServices.shutDown();
						} catch (Exception e) {
							throw new CompletionException(new FlinkException("Could not properly shut down the resource manager runtime services.", e));
						}
					});
		}
	}

	//-------------------------------------------------------------------------------------
	// Fatal error handler
	//-------------------------------------------------------------------------------------

	@Override
	public void onFatalError(Throwable exception) {
		LOG.error("Encountered fatal error.", exception);

		CompletableFuture<Void> shutdownFuture = shutDownInternally();

		shutdownFuture.whenComplete(
			(Void ignored, Throwable throwable) -> {
				if (throwable != null) {
					LOG.error("Could not properly shut down the resource manager runner.", throwable);
				}
			});
	}
}
