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
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.resourcemanager.slotmanager.DefaultSlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerFactory;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple {@link StandaloneResourceManager} runner. It instantiates the resource manager's services
 * and handles fatal errors by shutting the resource manager down.
 */
public class ResourceManagerRunner implements FatalErrorHandler {

	private static final Logger LOG = LoggerFactory.getLogger(ResourceManagerRunner.class);

	private final Object lock = new Object();

	private final ResourceManager<?> resourceManager;

	public ResourceManagerRunner(
			final Configuration configuration,
			final RpcService rpcService,
			final HighAvailabilityServices highAvailabilityServices,
			final MetricRegistry metricRegistry) throws Exception {

		Preconditions.checkNotNull(configuration);
		Preconditions.checkNotNull(rpcService);
		Preconditions.checkNotNull(highAvailabilityServices);
		Preconditions.checkNotNull(metricRegistry);

		final ResourceManagerConfiguration resourceManagerConfiguration = ResourceManagerConfiguration.fromConfiguration(configuration);
		final SlotManagerFactory slotManagerFactory = new DefaultSlotManager.Factory();
		final JobLeaderIdService jobLeaderIdService = new JobLeaderIdService(highAvailabilityServices);

		this.resourceManager = new StandaloneResourceManager(
			rpcService,
			resourceManagerConfiguration,
			highAvailabilityServices,
			slotManagerFactory,
			metricRegistry,
			jobLeaderIdService,
			this);
	}

	//-------------------------------------------------------------------------------------
	// Lifecycle management
	//-------------------------------------------------------------------------------------

	public void start() throws Exception {
		resourceManager.start();
	}

	public void shutDown() throws Exception {
		shutDownInternally();
	}

	private void shutDownInternally() throws Exception {
		synchronized (lock) {
			resourceManager.shutDown();
		}
	}

	//-------------------------------------------------------------------------------------
	// Fatal error handler
	//-------------------------------------------------------------------------------------

	@Override
	public void onFatalError(Throwable exception) {
		LOG.error("Encountered fatal error.", exception);

		try {
			shutDownInternally();
		} catch (Exception e) {
			LOG.error("Could not properly shut down the resource manager.", e);
		}
	}
}
