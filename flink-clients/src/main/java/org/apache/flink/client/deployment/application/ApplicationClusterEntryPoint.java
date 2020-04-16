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

package org.apache.flink.client.deployment.application;

import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.dispatcher.ArchivedExecutionGraphStore;
import org.apache.flink.runtime.dispatcher.MemoryArchivedExecutionGraphStore;
import org.apache.flink.runtime.dispatcher.SessionDispatcherFactory;
import org.apache.flink.runtime.dispatcher.runner.DefaultDispatcherRunnerFactory;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.resourcemanager.ResourceManagerFactory;
import org.apache.flink.runtime.rest.JobRestEndpointFactory;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for cluster entry points targeting executing applications in "Application Mode".
 * The lifecycle of the enrtypoint is bound to that of the specific application being executed,
 * and the {@code main()} method of the application is run on the cluster.
 */
public class ApplicationClusterEntryPoint extends ClusterEntrypoint {

	private final PackagedProgram program;

	private final ResourceManagerFactory<?> resourceManagerFactory;

	protected ApplicationClusterEntryPoint(
			final Configuration configuration,
			final PackagedProgram program,
			final ResourceManagerFactory<?> resourceManagerFactory) {
		super(configuration);
		this.program = checkNotNull(program);
		this.resourceManagerFactory = checkNotNull(resourceManagerFactory);
	}

	@Override
	protected DispatcherResourceManagerComponentFactory createDispatcherResourceManagerComponentFactory(final Configuration configuration) {
		return new DefaultDispatcherResourceManagerComponentFactory(
				new DefaultDispatcherRunnerFactory(
						ApplicationDispatcherLeaderProcessFactoryFactory
								.create(configuration, SessionDispatcherFactory.INSTANCE, program)),
				resourceManagerFactory,
				JobRestEndpointFactory.INSTANCE);
	}

	@Override
	protected ArchivedExecutionGraphStore createSerializableExecutionGraphStore(
			final Configuration configuration,
			final ScheduledExecutor scheduledExecutor) {
		return new MemoryArchivedExecutionGraphStore();
	}
}
