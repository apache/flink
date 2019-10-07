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

import org.apache.flink.runtime.dispatcher.runner.MiniDispatcherRunner;
import org.apache.flink.runtime.dispatcher.runner.MiniDispatcherRunnerFactory;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerFactory;
import org.apache.flink.runtime.rest.JobRestEndpointFactory;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.WebMonitorEndpoint;

import javax.annotation.Nonnull;

/**
 * {@link DispatcherResourceManagerComponentFactory} for a {@link JobDispatcherResourceManagerComponent}.
 */
public class JobDispatcherResourceManagerComponentFactory extends AbstractDispatcherResourceManagerComponentFactory<MiniDispatcherRunner, RestfulGateway> {

	public JobDispatcherResourceManagerComponentFactory(@Nonnull ResourceManagerFactory<?> resourceManagerFactory, @Nonnull JobGraphRetriever jobGraphRetriever) {
		super(new MiniDispatcherRunnerFactory(jobGraphRetriever), resourceManagerFactory, JobRestEndpointFactory.INSTANCE);
	}

	@Override
	protected DispatcherResourceManagerComponent createDispatcherResourceManagerComponent(
			MiniDispatcherRunner dispatcherRunner,
			ResourceManager<?> resourceManager,
			LeaderRetrievalService dispatcherLeaderRetrievalService,
			LeaderRetrievalService resourceManagerRetrievalService,
			WebMonitorEndpoint<?> webMonitorEndpoint) {
		return new JobDispatcherResourceManagerComponent(
			dispatcherRunner,
			resourceManager,
			dispatcherLeaderRetrievalService,
			resourceManagerRetrievalService,
			webMonitorEndpoint);
	}
}
