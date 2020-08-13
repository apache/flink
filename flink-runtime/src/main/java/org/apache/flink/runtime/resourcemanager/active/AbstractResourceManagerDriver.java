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

package org.apache.flink.runtime.resourcemanager.active;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract common base class for implementations of {@link ResourceManagerDriver}.
 */
public abstract class AbstractResourceManagerDriver<WorkerType extends ResourceIDRetrievable>
	implements ResourceManagerDriver<WorkerType> {

	protected final Logger log = LoggerFactory.getLogger(getClass());

	protected final Configuration flinkConfig;
	protected final Configuration flinkClientConfig;

	private ResourceEventHandler<WorkerType> resourceEventHandler = null;
	private ScheduledExecutor mainThreadExecutor = null;

	public AbstractResourceManagerDriver(
			final Configuration flinkConfig,
			final Configuration flinkClientConfig) {
		this.flinkConfig = Preconditions.checkNotNull(flinkConfig);
		this.flinkClientConfig = Preconditions.checkNotNull(flinkClientConfig);
	}

	protected final ResourceEventHandler<WorkerType> getResourceEventHandler() {
		Preconditions.checkState(resourceEventHandler != null,
				"Cannot get resource event handler. Resource manager driver is not initialized.");
		return resourceEventHandler;
	}

	protected final ScheduledExecutor getMainThreadExecutor() {
		Preconditions.checkState(mainThreadExecutor != null,
				"Cannot get main thread executor. Resource manager driver is not initialized.");
		return mainThreadExecutor;
	}

	@Override
	public final void initialize(
			ResourceEventHandler<WorkerType> resourceEventHandler,
			ScheduledExecutor mainThreadExecutor) throws Exception {
		this.resourceEventHandler = Preconditions.checkNotNull(resourceEventHandler);
		this.mainThreadExecutor = Preconditions.checkNotNull(mainThreadExecutor);

		initializeInternal();
	}

	/**
	 * Initialize the deployment specific components.
	 */
	protected abstract void initializeInternal() throws Exception;
}
