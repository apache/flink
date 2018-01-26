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

package org.apache.flink.runtime.webmonitor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.messages.webmonitor.ClusterOverview;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Testing implementation of the {@link RestfulGateway}.
 */
public class TestingRestfulGateway implements RestfulGateway {

	static final Function<JobID, CompletableFuture<? extends AccessExecutionGraph>> DEFAULT_REQUEST_JOB_FUNCTION = jobId -> FutureUtils.completedExceptionally(new UnsupportedOperationException());
	static final Supplier<CompletableFuture<MultipleJobsDetails>> DEFAULT_REQUEST_MULTIPLE_JOB_DETAILS_SUPPLIER = () -> CompletableFuture.completedFuture(new MultipleJobsDetails(Collections.emptyList()));
	static final Supplier<CompletableFuture<ClusterOverview>> DEFAULT_REQUEST_CLUSTER_OVERVIEW_SUPPLIER = () -> CompletableFuture.completedFuture(new ClusterOverview(0, 0, 0, 0, 0, 0, 0));
	static final Supplier<CompletableFuture<Collection<String>>> DEFAULT_REQUEST_METRIC_QUERY_SERVICE_PATHS_SUPPLIER = () -> CompletableFuture.completedFuture(Collections.emptyList());
	static final Supplier<CompletableFuture<Collection<Tuple2<ResourceID, String>>>> DEFAULT_REQUEST_TASK_MANAGER_METRIC_QUERY_SERVICE_PATHS_SUPPLIER = () -> CompletableFuture.completedFuture(Collections.emptyList());
	static final String LOCALHOST = "localhost";

	protected String address;

	protected String hostname;

	protected String restAddress;

	protected Function<JobID, CompletableFuture<? extends AccessExecutionGraph>> requestJobFunction;

	protected Supplier<CompletableFuture<MultipleJobsDetails>> requestMultipleJobDetailsSupplier;

	protected Supplier<CompletableFuture<ClusterOverview>> requestClusterOverviewSupplier;

	protected Supplier<CompletableFuture<Collection<String>>> requestMetricQueryServicePathsSupplier;

	protected Supplier<CompletableFuture<Collection<Tuple2<ResourceID, String>>>> requestTaskManagerMetricQueryServicePathsSupplier;

	public TestingRestfulGateway() {
		this(
			LOCALHOST,
			LOCALHOST,
			LOCALHOST,
			DEFAULT_REQUEST_JOB_FUNCTION,
			DEFAULT_REQUEST_MULTIPLE_JOB_DETAILS_SUPPLIER,
			DEFAULT_REQUEST_CLUSTER_OVERVIEW_SUPPLIER,
			DEFAULT_REQUEST_METRIC_QUERY_SERVICE_PATHS_SUPPLIER,
			DEFAULT_REQUEST_TASK_MANAGER_METRIC_QUERY_SERVICE_PATHS_SUPPLIER);
	}

	public TestingRestfulGateway(
			String address,
			String hostname,
			String restAddress,
			Function<JobID, CompletableFuture<? extends AccessExecutionGraph>> requestJobFunction,
			Supplier<CompletableFuture<MultipleJobsDetails>> requestMultipleJobDetailsSupplier,
			Supplier<CompletableFuture<ClusterOverview>> requestClusterOverviewSupplier,
			Supplier<CompletableFuture<Collection<String>>> requestMetricQueryServicePathsSupplier,
			Supplier<CompletableFuture<Collection<Tuple2<ResourceID, String>>>> requestTaskManagerMetricQueryServicePathsSupplier) {
		this.address = address;
		this.hostname = hostname;
		this.restAddress = restAddress;
		this.requestJobFunction = requestJobFunction;
		this.requestMultipleJobDetailsSupplier = requestMultipleJobDetailsSupplier;
		this.requestClusterOverviewSupplier = requestClusterOverviewSupplier;
		this.requestMetricQueryServicePathsSupplier = requestMetricQueryServicePathsSupplier;
		this.requestTaskManagerMetricQueryServicePathsSupplier = requestTaskManagerMetricQueryServicePathsSupplier;
	}

	@Override
	public CompletableFuture<String> requestRestAddress(Time timeout) {
		return CompletableFuture.completedFuture(restAddress);
	}

	@Override
	public CompletableFuture<? extends AccessExecutionGraph> requestJob(JobID jobId, Time timeout) {
		return requestJobFunction.apply(jobId);
	}

	@Override
	public CompletableFuture<MultipleJobsDetails> requestMultipleJobDetails(Time timeout) {
		return requestMultipleJobDetailsSupplier.get();
	}

	@Override
	public CompletableFuture<ClusterOverview> requestClusterOverview(Time timeout) {
		return requestClusterOverviewSupplier.get();
	}

	@Override
	public CompletableFuture<Collection<String>> requestMetricQueryServicePaths(Time timeout) {
		return requestMetricQueryServicePathsSupplier.get();
	}

	@Override
	public CompletableFuture<Collection<Tuple2<ResourceID, String>>> requestTaskManagerMetricQueryServicePaths(Time timeout) {
		return requestTaskManagerMetricQueryServicePathsSupplier.get();
	}

	@Override
	public String getAddress() {
		return address;
	}

	@Override
	public String getHostname() {
		return hostname;
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	/**
	 * Builder for the {@link TestingRestfulGateway}.
	 */
	public static final class Builder {
		private String address = LOCALHOST;
		private String hostname = LOCALHOST;
		private String restAddress = LOCALHOST;
		private Function<JobID, CompletableFuture<? extends AccessExecutionGraph>> requestJobFunction;
		private Supplier<CompletableFuture<MultipleJobsDetails>> requestMultipleJobDetailsSupplier;
		private Supplier<CompletableFuture<ClusterOverview>> requestClusterOverviewSupplier;
		private Supplier<CompletableFuture<Collection<String>>> requestMetricQueryServicePathsSupplier;
		private Supplier<CompletableFuture<Collection<Tuple2<ResourceID, String>>>> requestTaskManagerMetricQueryServicePathsSupplier;

		public Builder() {
			requestJobFunction = DEFAULT_REQUEST_JOB_FUNCTION;
			requestMultipleJobDetailsSupplier = DEFAULT_REQUEST_MULTIPLE_JOB_DETAILS_SUPPLIER;
			requestClusterOverviewSupplier = DEFAULT_REQUEST_CLUSTER_OVERVIEW_SUPPLIER;
			requestMetricQueryServicePathsSupplier = DEFAULT_REQUEST_METRIC_QUERY_SERVICE_PATHS_SUPPLIER;
			requestTaskManagerMetricQueryServicePathsSupplier = DEFAULT_REQUEST_TASK_MANAGER_METRIC_QUERY_SERVICE_PATHS_SUPPLIER;
		}

		public Builder setAddress(String address) {
			this.address = address;
			return this;
		}

		public Builder setHostname(String hostname) {
			this.hostname = hostname;
			return this;
		}

		public Builder setRestAddress(String restAddress) {
			this.restAddress = restAddress;
			return this;
		}

		public Builder setRequestJobFunction(Function<JobID, CompletableFuture<? extends AccessExecutionGraph>> requestJobFunction) {
			this.requestJobFunction = requestJobFunction;
			return this;
		}

		public Builder setRequestMultipleJobDetailsSupplier(Supplier<CompletableFuture<MultipleJobsDetails>> requestMultipleJobDetailsSupplier) {
			this.requestMultipleJobDetailsSupplier = requestMultipleJobDetailsSupplier;
			return this;
		}

		public Builder setRequestClusterOverviewSupplier(Supplier<CompletableFuture<ClusterOverview>> requestClusterOverviewSupplier) {
			this.requestClusterOverviewSupplier = requestClusterOverviewSupplier;
			return this;
		}

		public Builder setRequestMetricQueryServicePathsSupplier(Supplier<CompletableFuture<Collection<String>>> requestMetricQueryServicePathsSupplier) {
			this.requestMetricQueryServicePathsSupplier = requestMetricQueryServicePathsSupplier;
			return this;
		}

		public Builder setRequestTaskManagerMetricQueryServicePathsSupplier(Supplier<CompletableFuture<Collection<Tuple2<ResourceID, String>>>> requestTaskManagerMetricQueryServicePathsSupplier) {
			this.requestTaskManagerMetricQueryServicePathsSupplier = requestTaskManagerMetricQueryServicePathsSupplier;
			return this;
		}

		public TestingRestfulGateway build() {
			return new TestingRestfulGateway(address, hostname, restAddress, requestJobFunction, requestMultipleJobDetailsSupplier, requestClusterOverviewSupplier, requestMetricQueryServicePathsSupplier, requestTaskManagerMetricQueryServicePathsSupplier);
		}
	}
}
