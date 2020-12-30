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
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.ClusterOverview;
import org.apache.flink.runtime.messages.webmonitor.JobsOverview;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStatsResponse;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.function.TriFunction;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/** Testing implementation of the {@link RestfulGateway}. */
public class TestingRestfulGateway implements RestfulGateway {

    static final Function<JobID, CompletableFuture<Acknowledge>> DEFAULT_CANCEL_JOB_FUNCTION =
            jobId -> CompletableFuture.completedFuture(Acknowledge.get());
    static final Function<JobID, CompletableFuture<JobResult>> DEFAULT_REQUEST_JOB_RESULT_FUNCTION =
            jobId -> FutureUtils.completedExceptionally(new UnsupportedOperationException());
    static final Function<JobID, CompletableFuture<ArchivedExecutionGraph>>
            DEFAULT_REQUEST_JOB_FUNCTION =
                    jobId ->
                            FutureUtils.completedExceptionally(new UnsupportedOperationException());
    static final Function<JobID, CompletableFuture<JobStatus>> DEFAULT_REQUEST_JOB_STATUS_FUNCTION =
            jobId -> CompletableFuture.completedFuture(JobStatus.RUNNING);
    static final Supplier<CompletableFuture<MultipleJobsDetails>>
            DEFAULT_REQUEST_MULTIPLE_JOB_DETAILS_SUPPLIER =
                    () ->
                            CompletableFuture.completedFuture(
                                    new MultipleJobsDetails(Collections.emptyList()));
    static final Supplier<CompletableFuture<ClusterOverview>>
            DEFAULT_REQUEST_CLUSTER_OVERVIEW_SUPPLIER =
                    () ->
                            CompletableFuture.completedFuture(
                                    new ClusterOverview(0, 0, 0, 0, 0, 0, 0));
    static final Supplier<CompletableFuture<Collection<String>>>
            DEFAULT_REQUEST_METRIC_QUERY_SERVICE_PATHS_SUPPLIER =
                    () -> CompletableFuture.completedFuture(Collections.emptyList());
    static final Supplier<CompletableFuture<Collection<Tuple2<ResourceID, String>>>>
            DEFAULT_REQUEST_TASK_MANAGER_METRIC_QUERY_SERVICE_PATHS_SUPPLIER =
                    () -> CompletableFuture.completedFuture(Collections.emptyList());
    static final Supplier<CompletableFuture<Acknowledge>> DEFAULT_CLUSTER_SHUTDOWN_SUPPLIER =
            () -> CompletableFuture.completedFuture(Acknowledge.get());
    static final BiFunction<
                    JobID, JobVertexID, CompletableFuture<OperatorBackPressureStatsResponse>>
            DEFAULT_REQUEST_OPERATOR_BACK_PRESSURE_STATS_SUPPLIER =
                    (jobId, jobVertexId) ->
                            FutureUtils.completedExceptionally(new UnsupportedOperationException());
    static final BiFunction<JobID, String, CompletableFuture<String>>
            DEFAULT_TRIGGER_SAVEPOINT_FUNCTION =
                    (JobID jobId, String targetDirectory) ->
                            FutureUtils.completedExceptionally(new UnsupportedOperationException());
    static final BiFunction<JobID, String, CompletableFuture<String>>
            DEFAULT_STOP_WITH_SAVEPOINT_FUNCTION =
                    (JobID jobId, String targetDirectory) ->
                            FutureUtils.completedExceptionally(new UnsupportedOperationException());
    static final TriFunction<
                    JobID,
                    OperatorID,
                    SerializedValue<CoordinationRequest>,
                    CompletableFuture<CoordinationResponse>>
            DEFAULT_DELIVER_COORDINATION_REQUEST_TO_COORDINATOR_FUNCTION =
                    (JobID jobId,
                            OperatorID operatorId,
                            SerializedValue<CoordinationRequest> serializedRequest) ->
                            FutureUtils.completedExceptionally(new UnsupportedOperationException());
    static final String LOCALHOST = "localhost";

    protected String address;

    protected String hostname;

    protected String restAddress;

    protected Function<JobID, CompletableFuture<Acknowledge>> cancelJobFunction;

    protected Supplier<CompletableFuture<Acknowledge>> clusterShutdownSupplier;

    protected Function<JobID, CompletableFuture<ArchivedExecutionGraph>> requestJobFunction;

    protected Function<JobID, CompletableFuture<JobResult>> requestJobResultFunction;

    protected Function<JobID, CompletableFuture<JobStatus>> requestJobStatusFunction;

    protected Supplier<CompletableFuture<MultipleJobsDetails>> requestMultipleJobDetailsSupplier;

    protected Supplier<CompletableFuture<ClusterOverview>> requestClusterOverviewSupplier;

    protected Supplier<CompletableFuture<Collection<String>>>
            requestMetricQueryServiceAddressesSupplier;

    protected Supplier<CompletableFuture<Collection<Tuple2<ResourceID, String>>>>
            requestTaskManagerMetricQueryServiceAddressesSupplier;

    protected BiFunction<JobID, JobVertexID, CompletableFuture<OperatorBackPressureStatsResponse>>
            requestOperatorBackPressureStatsFunction;

    protected BiFunction<JobID, String, CompletableFuture<String>> triggerSavepointFunction;

    protected BiFunction<JobID, String, CompletableFuture<String>> stopWithSavepointFunction;

    protected TriFunction<
                    JobID,
                    OperatorID,
                    SerializedValue<CoordinationRequest>,
                    CompletableFuture<CoordinationResponse>>
            deliverCoordinationRequestToCoordinatorFunction;

    public TestingRestfulGateway() {
        this(
                LOCALHOST,
                LOCALHOST,
                DEFAULT_CANCEL_JOB_FUNCTION,
                DEFAULT_REQUEST_JOB_FUNCTION,
                DEFAULT_REQUEST_JOB_RESULT_FUNCTION,
                DEFAULT_REQUEST_JOB_STATUS_FUNCTION,
                DEFAULT_REQUEST_MULTIPLE_JOB_DETAILS_SUPPLIER,
                DEFAULT_REQUEST_CLUSTER_OVERVIEW_SUPPLIER,
                DEFAULT_REQUEST_METRIC_QUERY_SERVICE_PATHS_SUPPLIER,
                DEFAULT_REQUEST_TASK_MANAGER_METRIC_QUERY_SERVICE_PATHS_SUPPLIER,
                DEFAULT_REQUEST_OPERATOR_BACK_PRESSURE_STATS_SUPPLIER,
                DEFAULT_TRIGGER_SAVEPOINT_FUNCTION,
                DEFAULT_STOP_WITH_SAVEPOINT_FUNCTION,
                DEFAULT_CLUSTER_SHUTDOWN_SUPPLIER,
                DEFAULT_DELIVER_COORDINATION_REQUEST_TO_COORDINATOR_FUNCTION);
    }

    public TestingRestfulGateway(
            String address,
            String hostname,
            Function<JobID, CompletableFuture<Acknowledge>> cancelJobFunction,
            Function<JobID, CompletableFuture<ArchivedExecutionGraph>> requestJobFunction,
            Function<JobID, CompletableFuture<JobResult>> requestJobResultFunction,
            Function<JobID, CompletableFuture<JobStatus>> requestJobStatusFunction,
            Supplier<CompletableFuture<MultipleJobsDetails>> requestMultipleJobDetailsSupplier,
            Supplier<CompletableFuture<ClusterOverview>> requestClusterOverviewSupplier,
            Supplier<CompletableFuture<Collection<String>>>
                    requestMetricQueryServiceAddressesSupplier,
            Supplier<CompletableFuture<Collection<Tuple2<ResourceID, String>>>>
                    requestTaskManagerMetricQueryServiceAddressesSupplier,
            BiFunction<JobID, JobVertexID, CompletableFuture<OperatorBackPressureStatsResponse>>
                    requestOperatorBackPressureStatsFunction,
            BiFunction<JobID, String, CompletableFuture<String>> triggerSavepointFunction,
            BiFunction<JobID, String, CompletableFuture<String>> stopWithSavepointFunction,
            Supplier<CompletableFuture<Acknowledge>> clusterShutdownSupplier,
            TriFunction<
                            JobID,
                            OperatorID,
                            SerializedValue<CoordinationRequest>,
                            CompletableFuture<CoordinationResponse>>
                    deliverCoordinationRequestToCoordinatorFunction) {
        this.address = address;
        this.hostname = hostname;
        this.cancelJobFunction = cancelJobFunction;
        this.requestJobFunction = requestJobFunction;
        this.requestJobResultFunction = requestJobResultFunction;
        this.requestJobStatusFunction = requestJobStatusFunction;
        this.requestMultipleJobDetailsSupplier = requestMultipleJobDetailsSupplier;
        this.requestClusterOverviewSupplier = requestClusterOverviewSupplier;
        this.requestMetricQueryServiceAddressesSupplier =
                requestMetricQueryServiceAddressesSupplier;
        this.requestTaskManagerMetricQueryServiceAddressesSupplier =
                requestTaskManagerMetricQueryServiceAddressesSupplier;
        this.requestOperatorBackPressureStatsFunction = requestOperatorBackPressureStatsFunction;
        this.triggerSavepointFunction = triggerSavepointFunction;
        this.stopWithSavepointFunction = stopWithSavepointFunction;
        this.clusterShutdownSupplier = clusterShutdownSupplier;
        this.deliverCoordinationRequestToCoordinatorFunction =
                deliverCoordinationRequestToCoordinatorFunction;
    }

    @Override
    public CompletableFuture<Acknowledge> cancelJob(JobID jobId, Time timeout) {
        return cancelJobFunction.apply(jobId);
    }

    @Override
    public CompletableFuture<Acknowledge> shutDownCluster() {
        return clusterShutdownSupplier.get();
    }

    @Override
    public CompletableFuture<ArchivedExecutionGraph> requestJob(JobID jobId, Time timeout) {
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
    public CompletableFuture<Collection<String>> requestMetricQueryServiceAddresses(Time timeout) {
        return requestMetricQueryServiceAddressesSupplier.get();
    }

    @Override
    public CompletableFuture<Collection<Tuple2<ResourceID, String>>>
            requestTaskManagerMetricQueryServiceAddresses(Time timeout) {
        return requestTaskManagerMetricQueryServiceAddressesSupplier.get();
    }

    @Override
    public CompletableFuture<OperatorBackPressureStatsResponse> requestOperatorBackPressureStats(
            final JobID jobId, final JobVertexID jobVertexId) {
        return requestOperatorBackPressureStatsFunction.apply(jobId, jobVertexId);
    }

    @Override
    public CompletableFuture<String> triggerSavepoint(
            JobID jobId, String targetDirectory, boolean cancelJob, Time timeout) {
        return triggerSavepointFunction.apply(jobId, targetDirectory);
    }

    @Override
    public CompletableFuture<String> stopWithSavepoint(
            JobID jobId, String targetDirectory, boolean advanceToEndOfTime, Time timeout) {
        return stopWithSavepointFunction.apply(jobId, targetDirectory);
    }

    @Override
    public CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
            JobID jobId,
            OperatorID operatorId,
            SerializedValue<CoordinationRequest> serializedRequest,
            Time timeout) {
        return deliverCoordinationRequestToCoordinatorFunction.apply(
                jobId, operatorId, serializedRequest);
    }

    @Override
    public String getAddress() {
        return address;
    }

    @Override
    public String getHostname() {
        return hostname;
    }

    /**
     * Abstract builder class for {@link TestingRestfulGateway} and its subclasses.
     *
     * @param <T> type of sub class
     */
    protected abstract static class AbstractBuilder<T extends AbstractBuilder> {
        protected String address = LOCALHOST;
        protected String hostname = LOCALHOST;
        protected Function<JobID, CompletableFuture<Acknowledge>> cancelJobFunction;
        protected Function<JobID, CompletableFuture<ArchivedExecutionGraph>> requestJobFunction;
        protected Function<JobID, CompletableFuture<JobResult>> requestJobResultFunction;
        protected Function<JobID, CompletableFuture<JobStatus>> requestJobStatusFunction;
        protected Supplier<CompletableFuture<MultipleJobsDetails>>
                requestMultipleJobDetailsSupplier;
        protected Supplier<CompletableFuture<ClusterOverview>> requestClusterOverviewSupplier;
        protected Supplier<CompletableFuture<JobsOverview>> requestOverviewForAllJobsSupplier;
        protected Supplier<CompletableFuture<Collection<String>>>
                requestMetricQueryServiceGatewaysSupplier;
        protected Supplier<CompletableFuture<Collection<Tuple2<ResourceID, String>>>>
                requestTaskManagerMetricQueryServiceGatewaysSupplier;
        protected Supplier<CompletableFuture<Acknowledge>> clusterShutdownSupplier;
        protected BiFunction<
                        JobID, JobVertexID, CompletableFuture<OperatorBackPressureStatsResponse>>
                requestOperatorBackPressureStatsFunction;
        protected BiFunction<JobID, String, CompletableFuture<String>> triggerSavepointFunction;
        protected BiFunction<JobID, String, CompletableFuture<String>> stopWithSavepointFunction;
        protected TriFunction<
                        JobID,
                        OperatorID,
                        SerializedValue<CoordinationRequest>,
                        CompletableFuture<CoordinationResponse>>
                deliverCoordinationRequestToCoordinatorFunction;

        protected AbstractBuilder() {
            cancelJobFunction = DEFAULT_CANCEL_JOB_FUNCTION;
            requestJobFunction = DEFAULT_REQUEST_JOB_FUNCTION;
            requestJobResultFunction = DEFAULT_REQUEST_JOB_RESULT_FUNCTION;
            requestJobStatusFunction = DEFAULT_REQUEST_JOB_STATUS_FUNCTION;
            requestMultipleJobDetailsSupplier = DEFAULT_REQUEST_MULTIPLE_JOB_DETAILS_SUPPLIER;
            requestClusterOverviewSupplier = DEFAULT_REQUEST_CLUSTER_OVERVIEW_SUPPLIER;
            requestMetricQueryServiceGatewaysSupplier =
                    DEFAULT_REQUEST_METRIC_QUERY_SERVICE_PATHS_SUPPLIER;
            requestTaskManagerMetricQueryServiceGatewaysSupplier =
                    DEFAULT_REQUEST_TASK_MANAGER_METRIC_QUERY_SERVICE_PATHS_SUPPLIER;
            requestOperatorBackPressureStatsFunction =
                    DEFAULT_REQUEST_OPERATOR_BACK_PRESSURE_STATS_SUPPLIER;
            triggerSavepointFunction = DEFAULT_TRIGGER_SAVEPOINT_FUNCTION;
            stopWithSavepointFunction = DEFAULT_STOP_WITH_SAVEPOINT_FUNCTION;
            clusterShutdownSupplier = DEFAULT_CLUSTER_SHUTDOWN_SUPPLIER;
            deliverCoordinationRequestToCoordinatorFunction =
                    DEFAULT_DELIVER_COORDINATION_REQUEST_TO_COORDINATOR_FUNCTION;
        }

        public T setAddress(String address) {
            this.address = address;
            return self();
        }

        public T setHostname(String hostname) {
            this.hostname = hostname;
            return self();
        }

        public T setClusterShutdownSupplier(
                Supplier<CompletableFuture<Acknowledge>> clusterShutdownSupplier) {
            this.clusterShutdownSupplier = clusterShutdownSupplier;
            return self();
        }

        public T setRequestJobFunction(
                Function<JobID, CompletableFuture<ArchivedExecutionGraph>> requestJobFunction) {
            this.requestJobFunction = requestJobFunction;
            return self();
        }

        public T setRequestJobResultFunction(
                Function<JobID, CompletableFuture<JobResult>> requestJobResultFunction) {
            this.requestJobResultFunction = requestJobResultFunction;
            return self();
        }

        public T setRequestJobStatusFunction(
                Function<JobID, CompletableFuture<JobStatus>> requestJobStatusFunction) {
            this.requestJobStatusFunction = requestJobStatusFunction;
            return self();
        }

        public T setRequestMultipleJobDetailsSupplier(
                Supplier<CompletableFuture<MultipleJobsDetails>>
                        requestMultipleJobDetailsSupplier) {
            this.requestMultipleJobDetailsSupplier = requestMultipleJobDetailsSupplier;
            return self();
        }

        public T setRequestClusterOverviewSupplier(
                Supplier<CompletableFuture<ClusterOverview>> requestClusterOverviewSupplier) {
            this.requestClusterOverviewSupplier = requestClusterOverviewSupplier;
            return self();
        }

        public T setRequestMetricQueryServiceGatewaysSupplier(
                Supplier<CompletableFuture<Collection<String>>>
                        requestMetricQueryServiceGatewaysSupplier) {
            this.requestMetricQueryServiceGatewaysSupplier =
                    requestMetricQueryServiceGatewaysSupplier;
            return self();
        }

        public T setRequestTaskManagerMetricQueryServiceGatewaysSupplier(
                Supplier<CompletableFuture<Collection<Tuple2<ResourceID, String>>>>
                        requestTaskManagerMetricQueryServiceGatewaysSupplier) {
            this.requestTaskManagerMetricQueryServiceGatewaysSupplier =
                    requestTaskManagerMetricQueryServiceGatewaysSupplier;
            return self();
        }

        public T setRequestOperatorBackPressureStatsFunction(
                BiFunction<JobID, JobVertexID, CompletableFuture<OperatorBackPressureStatsResponse>>
                        requestOeratorBackPressureStatsFunction) {
            this.requestOperatorBackPressureStatsFunction = requestOeratorBackPressureStatsFunction;
            return self();
        }

        public T setCancelJobFunction(
                Function<JobID, CompletableFuture<Acknowledge>> cancelJobFunction) {
            this.cancelJobFunction = cancelJobFunction;
            return self();
        }

        public T setTriggerSavepointFunction(
                BiFunction<JobID, String, CompletableFuture<String>> triggerSavepointFunction) {
            this.triggerSavepointFunction = triggerSavepointFunction;
            return self();
        }

        public T setStopWithSavepointFunction(
                BiFunction<JobID, String, CompletableFuture<String>> stopWithSavepointFunction) {
            this.stopWithSavepointFunction = stopWithSavepointFunction;
            return self();
        }

        public T setDeliverCoordinationRequestToCoordinatorFunction(
                TriFunction<
                                JobID,
                                OperatorID,
                                SerializedValue<CoordinationRequest>,
                                CompletableFuture<CoordinationResponse>>
                        deliverCoordinationRequestToCoordinatorFunction) {
            this.deliverCoordinationRequestToCoordinatorFunction =
                    deliverCoordinationRequestToCoordinatorFunction;
            return self();
        }

        protected abstract T self();

        public abstract TestingRestfulGateway build();
    }

    /** Builder for the {@link TestingRestfulGateway}. */
    public static class Builder extends AbstractBuilder<Builder> {

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public TestingRestfulGateway build() {
            return new TestingRestfulGateway(
                    address,
                    hostname,
                    cancelJobFunction,
                    requestJobFunction,
                    requestJobResultFunction,
                    requestJobStatusFunction,
                    requestMultipleJobDetailsSupplier,
                    requestClusterOverviewSupplier,
                    requestMetricQueryServiceGatewaysSupplier,
                    requestTaskManagerMetricQueryServiceGatewaysSupplier,
                    requestOperatorBackPressureStatsFunction,
                    triggerSavepointFunction,
                    stopWithSavepointFunction,
                    clusterShutdownSupplier,
                    deliverCoordinationRequestToCoordinatorFunction);
        }
    }
}
