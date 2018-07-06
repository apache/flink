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
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.ClusterOverview;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStatsResponse;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Testing implementation of the {@link RestfulGateway}.
 */
public class TestingRestfulGateway implements RestfulGateway {

	static final Function<JobID, CompletableFuture<Acknowledge>> DEFAULT_CANCEL_JOB_FUNCTION = jobId -> CompletableFuture.completedFuture(Acknowledge.get());
	static final Function<JobID, CompletableFuture<Acknowledge>> DEFAULT_STOP_JOB_FUNCTION = jobId -> CompletableFuture.completedFuture(Acknowledge.get());
	static final Function<JobID, CompletableFuture<JobResult>> DEFAULT_REQUEST_JOB_RESULT_FUNCTION = jobId -> FutureUtils.completedExceptionally(new UnsupportedOperationException());
	static final Function<JobID, CompletableFuture<? extends AccessExecutionGraph>> DEFAULT_REQUEST_JOB_FUNCTION = jobId -> FutureUtils.completedExceptionally(new UnsupportedOperationException());
	static final Function<JobID, CompletableFuture<JobStatus>> DEFAULT_REQUEST_JOB_STATUS_FUNCTION = jobId -> FutureUtils.completedExceptionally(new UnsupportedOperationException());
	static final Supplier<CompletableFuture<MultipleJobsDetails>> DEFAULT_REQUEST_MULTIPLE_JOB_DETAILS_SUPPLIER = () -> CompletableFuture.completedFuture(new MultipleJobsDetails(Collections.emptyList()));
	static final Supplier<CompletableFuture<ClusterOverview>> DEFAULT_REQUEST_CLUSTER_OVERVIEW_SUPPLIER = () -> CompletableFuture.completedFuture(new ClusterOverview(0, 0, 0, 0, 0, 0, 0));
	static final Supplier<CompletableFuture<Collection<String>>> DEFAULT_REQUEST_METRIC_QUERY_SERVICE_PATHS_SUPPLIER = () -> CompletableFuture.completedFuture(Collections.emptyList());
	static final Supplier<CompletableFuture<Collection<Tuple2<ResourceID, String>>>> DEFAULT_REQUEST_TASK_MANAGER_METRIC_QUERY_SERVICE_PATHS_SUPPLIER = () -> CompletableFuture.completedFuture(Collections.emptyList());
	static final BiFunction<JobID, JobVertexID, CompletableFuture<OperatorBackPressureStatsResponse>> DEFAULT_REQUEST_OPERATOR_BACK_PRESSURE_STATS_SUPPLIER = (jobId, jobVertexId) -> FutureUtils.completedExceptionally(new UnsupportedOperationException());
	static final BiFunction<JobID, String, CompletableFuture<String>> DEFAULT_TRIGGER_SAVEPOINT_FUNCTION = (JobID jobId, String targetDirectory) -> FutureUtils.completedExceptionally(new UnsupportedOperationException());
	static final String LOCALHOST = "localhost";

	protected String address;

	protected String hostname;

	protected String restAddress;

	protected Function<JobID, CompletableFuture<Acknowledge>> cancelJobFunction;

	protected Function<JobID, CompletableFuture<Acknowledge>> stopJobFunction;

	protected Function<JobID, CompletableFuture<? extends AccessExecutionGraph>> requestJobFunction;

	protected Function<JobID, CompletableFuture<JobResult>> requestJobResultFunction;

	protected Function<JobID, CompletableFuture<JobStatus>> requestJobStatusFunction;

	protected Supplier<CompletableFuture<MultipleJobsDetails>> requestMultipleJobDetailsSupplier;

	protected Supplier<CompletableFuture<ClusterOverview>> requestClusterOverviewSupplier;

	protected Supplier<CompletableFuture<Collection<String>>> requestMetricQueryServicePathsSupplier;

	protected Supplier<CompletableFuture<Collection<Tuple2<ResourceID, String>>>> requestTaskManagerMetricQueryServicePathsSupplier;

	protected BiFunction<JobID, JobVertexID, CompletableFuture<OperatorBackPressureStatsResponse>> requestOperatorBackPressureStatsFunction;

	protected BiFunction<JobID, String, CompletableFuture<String>> triggerSavepointFunction;

	public TestingRestfulGateway() {
		this(
			LOCALHOST,
			LOCALHOST,
			LOCALHOST,
			DEFAULT_CANCEL_JOB_FUNCTION,
			DEFAULT_STOP_JOB_FUNCTION,
			DEFAULT_REQUEST_JOB_FUNCTION,
			DEFAULT_REQUEST_JOB_RESULT_FUNCTION,
			DEFAULT_REQUEST_JOB_STATUS_FUNCTION,
			DEFAULT_REQUEST_MULTIPLE_JOB_DETAILS_SUPPLIER,
			DEFAULT_REQUEST_CLUSTER_OVERVIEW_SUPPLIER,
			DEFAULT_REQUEST_METRIC_QUERY_SERVICE_PATHS_SUPPLIER,
			DEFAULT_REQUEST_TASK_MANAGER_METRIC_QUERY_SERVICE_PATHS_SUPPLIER,
			DEFAULT_REQUEST_OPERATOR_BACK_PRESSURE_STATS_SUPPLIER,
			DEFAULT_TRIGGER_SAVEPOINT_FUNCTION);
	}

	public TestingRestfulGateway(
			String address,
			String hostname,
			String restAddress,
			Function<JobID, CompletableFuture<Acknowledge>> cancelJobFunction,
			Function<JobID, CompletableFuture<Acknowledge>> stopJobFunction,
			Function<JobID, CompletableFuture<? extends AccessExecutionGraph>> requestJobFunction,
			Function<JobID, CompletableFuture<JobResult>> requestJobResultFunction,
			Function<JobID, CompletableFuture<JobStatus>> requestJobStatusFunction,
			Supplier<CompletableFuture<MultipleJobsDetails>> requestMultipleJobDetailsSupplier,
			Supplier<CompletableFuture<ClusterOverview>> requestClusterOverviewSupplier,
			Supplier<CompletableFuture<Collection<String>>> requestMetricQueryServicePathsSupplier,
			Supplier<CompletableFuture<Collection<Tuple2<ResourceID, String>>>> requestTaskManagerMetricQueryServicePathsSupplier,
			BiFunction<JobID, JobVertexID, CompletableFuture<OperatorBackPressureStatsResponse>> requestOperatorBackPressureStatsFunction,
			BiFunction<JobID, String, CompletableFuture<String>> triggerSavepointFunction) {
		this.address = address;
		this.hostname = hostname;
		this.restAddress = restAddress;
		this.cancelJobFunction = cancelJobFunction;
		this.stopJobFunction = stopJobFunction;
		this.requestJobFunction = requestJobFunction;
		this.requestJobResultFunction = requestJobResultFunction;
		this.requestJobStatusFunction = requestJobStatusFunction;
		this.requestMultipleJobDetailsSupplier = requestMultipleJobDetailsSupplier;
		this.requestClusterOverviewSupplier = requestClusterOverviewSupplier;
		this.requestMetricQueryServicePathsSupplier = requestMetricQueryServicePathsSupplier;
		this.requestTaskManagerMetricQueryServicePathsSupplier = requestTaskManagerMetricQueryServicePathsSupplier;
		this.requestOperatorBackPressureStatsFunction = requestOperatorBackPressureStatsFunction;
		this.triggerSavepointFunction = triggerSavepointFunction;
	}

	@Override
	public CompletableFuture<Acknowledge> cancelJob(JobID jobId, Time timeout) {
		return cancelJobFunction.apply(jobId);
	}

	@Override
	public CompletableFuture<Acknowledge> stopJob(JobID jobId, Time timeout) {
		return stopJobFunction.apply(jobId);
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
	public CompletableFuture<JobResult> requestJobResult(JobID jobId, Time timeout) {
		return requestJobResultFunction.apply(jobId);
	}

	@Override
	public CompletableFuture<JobStatus> requestJobStatus(JobID jobId, Time timeout) {
		return requestJobStatusFunction.apply(jobId);
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
	public CompletableFuture<OperatorBackPressureStatsResponse> requestOperatorBackPressureStats(final JobID jobId, final JobVertexID jobVertexId) {
		return requestOperatorBackPressureStatsFunction.apply(jobId, jobVertexId);
	}

	@Override
	public CompletableFuture<String> triggerSavepoint(JobID jobId, String targetDirectory, boolean cancelJob, Time timeout) {
		return triggerSavepointFunction.apply(jobId, targetDirectory);
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
	public static class Builder {
		protected String address = LOCALHOST;
		protected String hostname = LOCALHOST;
		protected String restAddress = LOCALHOST;
		protected Function<JobID, CompletableFuture<Acknowledge>> cancelJobFunction;
		protected Function<JobID, CompletableFuture<Acknowledge>> stopJobFunction;
		protected Function<JobID, CompletableFuture<? extends AccessExecutionGraph>> requestJobFunction;
		protected Function<JobID, CompletableFuture<JobResult>> requestJobResultFunction;
		protected Function<JobID, CompletableFuture<JobStatus>> requestJobStatusFunction;
		protected Supplier<CompletableFuture<MultipleJobsDetails>> requestMultipleJobDetailsSupplier;
		protected Supplier<CompletableFuture<ClusterOverview>> requestClusterOverviewSupplier;
		protected Supplier<CompletableFuture<Collection<String>>> requestMetricQueryServicePathsSupplier;
		protected Supplier<CompletableFuture<Collection<Tuple2<ResourceID, String>>>> requestTaskManagerMetricQueryServicePathsSupplier;
		protected BiFunction<JobID, JobVertexID, CompletableFuture<OperatorBackPressureStatsResponse>> requestOperatorBackPressureStatsFunction;
		protected BiFunction<JobID, String, CompletableFuture<String>> triggerSavepointFunction;

		public Builder() {
			cancelJobFunction = DEFAULT_CANCEL_JOB_FUNCTION;
			stopJobFunction = DEFAULT_STOP_JOB_FUNCTION;
			requestJobFunction = DEFAULT_REQUEST_JOB_FUNCTION;
			requestJobResultFunction = DEFAULT_REQUEST_JOB_RESULT_FUNCTION;
			requestJobStatusFunction = DEFAULT_REQUEST_JOB_STATUS_FUNCTION;
			requestMultipleJobDetailsSupplier = DEFAULT_REQUEST_MULTIPLE_JOB_DETAILS_SUPPLIER;
			requestClusterOverviewSupplier = DEFAULT_REQUEST_CLUSTER_OVERVIEW_SUPPLIER;
			requestMetricQueryServicePathsSupplier = DEFAULT_REQUEST_METRIC_QUERY_SERVICE_PATHS_SUPPLIER;
			requestTaskManagerMetricQueryServicePathsSupplier = DEFAULT_REQUEST_TASK_MANAGER_METRIC_QUERY_SERVICE_PATHS_SUPPLIER;
			requestOperatorBackPressureStatsFunction = DEFAULT_REQUEST_OPERATOR_BACK_PRESSURE_STATS_SUPPLIER;
			triggerSavepointFunction = DEFAULT_TRIGGER_SAVEPOINT_FUNCTION;
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

		public Builder setRequestJobResultFunction(Function<JobID, CompletableFuture<JobResult>> requestJobResultFunction) {
			this.requestJobResultFunction = requestJobResultFunction;
			return this;
		}

		public Builder setRequestJobStatusFunction(Function<JobID, CompletableFuture<JobStatus>> requestJobStatusFunction) {
			this.requestJobStatusFunction = requestJobStatusFunction;
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

		public Builder setRequestOperatorBackPressureStatsFunction(BiFunction<JobID, JobVertexID, CompletableFuture<OperatorBackPressureStatsResponse>> requestOeratorBackPressureStatsFunction) {
			this.requestOperatorBackPressureStatsFunction = requestOeratorBackPressureStatsFunction;
			return this;
		}

		public Builder setCancelJobFunction(Function<JobID, CompletableFuture<Acknowledge>> cancelJobFunction) {
			this.cancelJobFunction = cancelJobFunction;
			return this;
		}

		public Builder setStopJobFunction(Function<JobID, CompletableFuture<Acknowledge>> stopJobFunction) {
			this.stopJobFunction = stopJobFunction;
			return this;
		}

		public Builder setTriggerSavepointFunction(BiFunction<JobID, String, CompletableFuture<String>> triggerSavepointFunction) {
			this.triggerSavepointFunction = triggerSavepointFunction;
			return this;
		}

		public TestingRestfulGateway build() {
			return new TestingRestfulGateway(
				address,
				hostname,
				restAddress,
				cancelJobFunction,
				stopJobFunction,
				requestJobFunction,
				requestJobResultFunction,
				requestJobStatusFunction,
				requestMultipleJobDetailsSupplier,
				requestClusterOverviewSupplier,
				requestMetricQueryServicePathsSupplier,
				requestTaskManagerMetricQueryServicePathsSupplier,
				requestOperatorBackPressureStatsFunction,
				triggerSavepointFunction);
		}
	}
}
