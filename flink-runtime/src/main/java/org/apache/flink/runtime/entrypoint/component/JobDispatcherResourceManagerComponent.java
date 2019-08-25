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

import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.dispatcher.MiniDispatcher;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.webmonitor.WebMonitorEndpoint;

import java.util.concurrent.CompletableFuture;

/**
 * {@link DispatcherResourceManagerComponent} for a job cluster. The dispatcher component starts
 * a {@link MiniDispatcher}.
 */
class JobDispatcherResourceManagerComponent extends DispatcherResourceManagerComponent<MiniDispatcher> {

	JobDispatcherResourceManagerComponent(
			MiniDispatcher dispatcher,
			ResourceManager<?> resourceManager,
			LeaderRetrievalService dispatcherLeaderRetrievalService,
			LeaderRetrievalService resourceManagerRetrievalService,
			WebMonitorEndpoint<?> webMonitorEndpoint,
			JobManagerMetricGroup jobManagerMetricGroup) {
		super(dispatcher, resourceManager, dispatcherLeaderRetrievalService, resourceManagerRetrievalService, webMonitorEndpoint, jobManagerMetricGroup);

		final CompletableFuture<ApplicationStatus> shutDownFuture = getShutDownFuture();

		dispatcher.getJobTerminationFuture().whenComplete((applicationStatus, throwable) -> {
			if (throwable != null) {
				shutDownFuture.completeExceptionally(throwable);
			} else {
				shutDownFuture.complete(applicationStatus);
			}
		});
	}
}
