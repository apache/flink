/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * {@link DispatcherFactory} services container.
 */
public class PartialDispatcherServicesWithJobGraphStore extends PartialDispatcherServices {

	@Nonnull
	private final JobGraphStore jobGraphStore;

	public PartialDispatcherServicesWithJobGraphStore(
			@Nonnull Configuration configuration,
			@Nonnull HighAvailabilityServices highAvailabilityServices,
			@Nonnull GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
			@Nonnull BlobServer blobServer,
			@Nonnull HeartbeatServices heartbeatServices,
			@Nonnull JobManagerMetricGroupFactory jobManagerMetricGroupFactory,
			@Nonnull ArchivedExecutionGraphStore archivedExecutionGraphStore,
			@Nonnull FatalErrorHandler fatalErrorHandler,
			@Nonnull HistoryServerArchivist historyServerArchivist,
			@Nullable String metricQueryServiceAddress,
			@Nonnull JobGraphStore jobGraphStore) {
		super(
			configuration,
			highAvailabilityServices,
			resourceManagerGatewayRetriever,
			blobServer,
			heartbeatServices,
			jobManagerMetricGroupFactory,
			archivedExecutionGraphStore,
			fatalErrorHandler,
			historyServerArchivist,
			metricQueryServiceAddress);
		this.jobGraphStore = jobGraphStore;
	}

	@Nonnull
	public JobGraphStore getJobGraphStore() {
		return jobGraphStore;
	}

	public static PartialDispatcherServicesWithJobGraphStore from(PartialDispatcherServices partialDispatcherServices, JobGraphStore jobGraphStore) {
		return new PartialDispatcherServicesWithJobGraphStore(
			partialDispatcherServices.getConfiguration(),
			partialDispatcherServices.getHighAvailabilityServices(),
			partialDispatcherServices.getResourceManagerGatewayRetriever(),
			partialDispatcherServices.getBlobServer(),
			partialDispatcherServices.getHeartbeatServices(),
			partialDispatcherServices.getJobManagerMetricGroupFactory(),
			partialDispatcherServices.getArchivedExecutionGraphStore(),
			partialDispatcherServices.getFatalErrorHandler(),
			partialDispatcherServices.getHistoryServerArchivist(),
			partialDispatcherServices.getMetricQueryServiceAddress(),
			jobGraphStore);
	}
}
