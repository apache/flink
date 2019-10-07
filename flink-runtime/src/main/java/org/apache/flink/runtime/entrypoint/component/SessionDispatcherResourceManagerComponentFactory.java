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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.SessionDispatcherFactory;
import org.apache.flink.runtime.dispatcher.runner.DispatcherRunner;
import org.apache.flink.runtime.dispatcher.runner.DispatcherRunnerFactory;
import org.apache.flink.runtime.dispatcher.runner.StandaloneDispatcherRunnerFactory;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerFactory;
import org.apache.flink.runtime.rest.SessionRestEndpointFactory;
import org.apache.flink.runtime.webmonitor.WebMonitorEndpoint;

import javax.annotation.Nonnull;

/**
 * {@link DispatcherResourceManagerComponentFactory} for a {@link SessionDispatcherResourceManagerComponent}.
 */
public class SessionDispatcherResourceManagerComponentFactory extends AbstractDispatcherResourceManagerComponentFactory<DispatcherRunner, DispatcherGateway> {

	public SessionDispatcherResourceManagerComponentFactory(@Nonnull ResourceManagerFactory<?> resourceManagerFactory) {
		this(new StandaloneDispatcherRunnerFactory(SessionDispatcherFactory.INSTANCE), resourceManagerFactory);
	}

	@VisibleForTesting
	public SessionDispatcherResourceManagerComponentFactory(
			@Nonnull DispatcherRunnerFactory<? extends DispatcherRunner> dispatcherFactory,
			@Nonnull ResourceManagerFactory<?> resourceManagerFactory) {
		super(dispatcherFactory, resourceManagerFactory, SessionRestEndpointFactory.INSTANCE);
	}

	@Override
	protected DispatcherResourceManagerComponent createDispatcherResourceManagerComponent(
			DispatcherRunner dispatcherRunner,
			ResourceManager<?> resourceManager,
			LeaderRetrievalService dispatcherLeaderRetrievalService,
			LeaderRetrievalService resourceManagerRetrievalService,
			WebMonitorEndpoint<?> webMonitorEndpoint) {
		return new SessionDispatcherResourceManagerComponent(
			dispatcherRunner,
			resourceManager,
			dispatcherLeaderRetrievalService,
			resourceManagerRetrievalService,
			webMonitorEndpoint);
	}
}
