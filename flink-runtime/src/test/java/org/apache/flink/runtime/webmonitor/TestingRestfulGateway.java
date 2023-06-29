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
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.dispatcher.TriggerSavepointMode;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.ClusterOverview;
import org.apache.flink.runtime.messages.webmonitor.JobsOverview;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.rest.handler.async.OperationResult;
import org.apache.flink.runtime.rest.handler.job.AsynchronousJobOperationKey;
import org.apache.flink.runtime.rest.messages.ThreadDumpInfo;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.concurrent.FutureUtils;
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
    static final Function<JobID, CompletableFuture<ExecutionGraphInfo>>
            DEFAULT_REQUEST_EXECUTION_GRAPH_INFO =
                    jobId ->
                            FutureUtils.completedExceptionally(new UnsupportedOperationException());
    static final Function<JobID, CompletableFuture<CheckpointStatsSnapshot>>
            DEFAULT_REQUEST_CHECKPOINT_STATS_SNAPSHOT =
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
                                    new ClusterOverview(0, 0, 0, 0, 0, 0, 0, 0, 0));
    static final Supplier<CompletableFuture<Collection<String>>>
            DEFAULT_REQUEST_METRIC_QUERY_SERVICE_PATHS_SUPPLIER =
                    () -> CompletableFuture.completedFuture(Collections.emptyList());
    static final Supplier<CompletableFuture<Collection<Tuple2<ResourceID, String>>>>
            DEFAULT_REQUEST_TASK_MANAGER_METRIC_QUERY_SERVICE_PATHS_SUPPLIER =
                    () -> CompletableFuture.completedFuture(Collections.emptyList());
    static final Supplier<CompletableFuture<ThreadDumpInfo>> DEFAULT_REQUEST_THREAD_DUMP_SUPPLIER =
            () -> FutureUtils.completedExceptionally(new UnsupportedOperationException());
    static final Supplier<CompletableFuture<Acknowledge>> DEFAULT_CLUSTER_SHUTDOWN_SUPPLIER =
            () -> CompletableFuture.completedFuture(Acknowledge.get());
    static final BiFunction<
                    AsynchronousJobOperationKey, CheckpointType, CompletableFuture<Acknowledge>>
            DEFAULT_TRIGGER_CHECKPOINT_FUNCTION =
                    (AsynchronousJobOperationKey operationKey, CheckpointType checkpointType) ->
                            FutureUtils.completedExceptionally(new UnsupportedOperationException());
    static final Function<AsynchronousJobOperationKey, CompletableFuture<OperationResult<Long>>>
            DEFAULT_GET_CHECKPOINT_STATUS_FUNCTION =
                    (AsynchronousJobOperationKey operationKey) ->
                            FutureUtils.completedExceptionally(new UnsupportedOperationException());

    static final TriFunction<
                    AsynchronousJobOperationKey,
                    String,
                    SavepointFormatType,
                    CompletableFuture<Acknowledge>>
            DEFAULT_TRIGGER_SAVEPOINT_FUNCTION =
                    (AsynchronousJobOperationKey operationKey,
                            String targetDirectory,
                            SavepointFormatType formatType) ->
                            FutureUtils.completedExceptionally(new UnsupportedOperationException());
    static final TriFunction<
                    AsynchronousJobOperationKey,
                    String,
                    SavepointFormatType,
                    CompletableFuture<Acknowledge>>
            DEFAULT_STOP_WITH_SAVEPOINT_FUNCTION =
                    (AsynchronousJobOperationKey operationKey,
                            String targetDirectory,
                            SavepointFormatType formatType) ->
                            FutureUtils.completedExceptionally(new UnsupportedOperationException());
    static final Function<AsynchronousJobOperationKey, CompletableFuture<OperationResult<String>>>
            DEFAULT_GET_SAVEPOINT_STATUS_FUNCTION =
                    (AsynchronousJobOperationKey operationKey) ->
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

    protected Function<JobID, CompletableFuture<ExecutionGraphInfo>>
            requestExecutionGraphInfoFunction;

    protected Function<JobID, CompletableFuture<CheckpointStatsSnapshot>>
            requestCheckpointStatsSnapshotFunction;

    protected Function<JobID, CompletableFuture<JobResult>> requestJobResultFunction;

    protected Function<JobID, CompletableFuture<JobStatus>> requestJobStatusFunction;

    protected Supplier<CompletableFuture<MultipleJobsDetails>> requestMultipleJobDetailsSupplier;

    protected Supplier<CompletableFuture<ClusterOverview>> requestClusterOverviewSupplier;

    protected Supplier<CompletableFuture<Collection<String>>>
            requestMetricQueryServiceAddressesSupplier;

    protected Supplier<CompletableFuture<Collection<Tuple2<ResourceID, String>>>>
            requestTaskManagerMetricQueryServiceAddressesSupplier;

    protected Supplier<CompletableFuture<ThreadDumpInfo>> requestThreadDumpSupplier;

    protected BiFunction<
                    AsynchronousJobOperationKey, CheckpointType, CompletableFuture<Acknowledge>>
            triggerCheckpointFunction;

    protected Function<AsynchronousJobOperationKey, CompletableFuture<OperationResult<Long>>>
            getCheckpointStatusFunction;

    protected TriFunction<
                    AsynchronousJobOperationKey,
                    String,
                    SavepointFormatType,
                    CompletableFuture<Acknowledge>>
            triggerSavepointFunction;

    protected TriFunction<
                    AsynchronousJobOperationKey,
                    String,
                    SavepointFormatType,
                    CompletableFuture<Acknowledge>>
            stopWithSavepointFunction;

    protected Function<AsynchronousJobOperationKey, CompletableFuture<OperationResult<String>>>
            getSavepointStatusFunction;

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
                DEFAULT_REQUEST_EXECUTION_GRAPH_INFO,
                DEFAULT_REQUEST_CHECKPOINT_STATS_SNAPSHOT,
                DEFAULT_REQUEST_JOB_RESULT_FUNCTION,
                DEFAULT_REQUEST_JOB_STATUS_FUNCTION,
                DEFAULT_REQUEST_MULTIPLE_JOB_DETAILS_SUPPLIER,
                DEFAULT_REQUEST_CLUSTER_OVERVIEW_SUPPLIER,
                DEFAULT_REQUEST_METRIC_QUERY_SERVICE_PATHS_SUPPLIER,
                DEFAULT_REQUEST_TASK_MANAGER_METRIC_QUERY_SERVICE_PATHS_SUPPLIER,
                DEFAULT_REQUEST_THREAD_DUMP_SUPPLIER,
                DEFAULT_TRIGGER_CHECKPOINT_FUNCTION,
                DEFAULT_GET_CHECKPOINT_STATUS_FUNCTION,
                DEFAULT_TRIGGER_SAVEPOINT_FUNCTION,
                DEFAULT_STOP_WITH_SAVEPOINT_FUNCTION,
                DEFAULT_GET_SAVEPOINT_STATUS_FUNCTION,
                DEFAULT_CLUSTER_SHUTDOWN_SUPPLIER,
                DEFAULT_DELIVER_COORDINATION_REQUEST_TO_COORDINATOR_FUNCTION);
    }

    public TestingRestfulGateway(
            String address,
            String hostname,
            Function<JobID, CompletableFuture<Acknowledge>> cancelJobFunction,
            Function<JobID, CompletableFuture<ArchivedExecutionGraph>> requestJobFunction,
            Function<JobID, CompletableFuture<ExecutionGraphInfo>>
                    requestExecutionGraphInfoFunction,
            Function<JobID, CompletableFuture<CheckpointStatsSnapshot>>
                    requestCheckpointStatsSnapshotFunction,
            Function<JobID, CompletableFuture<JobResult>> requestJobResultFunction,
            Function<JobID, CompletableFuture<JobStatus>> requestJobStatusFunction,
            Supplier<CompletableFuture<MultipleJobsDetails>> requestMultipleJobDetailsSupplier,
            Supplier<CompletableFuture<ClusterOverview>> requestClusterOverviewSupplier,
            Supplier<CompletableFuture<Collection<String>>>
                    requestMetricQueryServiceAddressesSupplier,
            Supplier<CompletableFuture<Collection<Tuple2<ResourceID, String>>>>
                    requestTaskManagerMetricQueryServiceAddressesSupplier,
            Supplier<CompletableFuture<ThreadDumpInfo>> requestThreadDumpSupplier,
            BiFunction<AsynchronousJobOperationKey, CheckpointType, CompletableFuture<Acknowledge>>
                    triggerCheckpointFunction,
            Function<AsynchronousJobOperationKey, CompletableFuture<OperationResult<Long>>>
                    getCheckpointStatusFunction,
            TriFunction<
                            AsynchronousJobOperationKey,
                            String,
                            SavepointFormatType,
                            CompletableFuture<Acknowledge>>
                    triggerSavepointFunction,
            TriFunction<
                            AsynchronousJobOperationKey,
                            String,
                            SavepointFormatType,
                            CompletableFuture<Acknowledge>>
                    stopWithSavepointFunction,
            Function<AsynchronousJobOperationKey, CompletableFuture<OperationResult<String>>>
                    getSavepointStatusFunction,
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
        this.requestExecutionGraphInfoFunction = requestExecutionGraphInfoFunction;
        this.requestCheckpointStatsSnapshotFunction = requestCheckpointStatsSnapshotFunction;
        this.requestJobResultFunction = requestJobResultFunction;
        this.requestJobStatusFunction = requestJobStatusFunction;
        this.requestMultipleJobDetailsSupplier = requestMultipleJobDetailsSupplier;
        this.requestClusterOverviewSupplier = requestClusterOverviewSupplier;
        this.requestMetricQueryServiceAddressesSupplier =
                requestMetricQueryServiceAddressesSupplier;
        this.requestTaskManagerMetricQueryServiceAddressesSupplier =
                requestTaskManagerMetricQueryServiceAddressesSupplier;
        this.requestThreadDumpSupplier = requestThreadDumpSupplier;
        this.triggerCheckpointFunction = triggerCheckpointFunction;
        this.getCheckpointStatusFunction = getCheckpointStatusFunction;
        this.triggerSavepointFunction = triggerSavepointFunction;
        this.stopWithSavepointFunction = stopWithSavepointFunction;
        this.getSavepointStatusFunction = getSavepointStatusFunction;
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
    public CompletableFuture<ExecutionGraphInfo> requestExecutionGraphInfo(
            JobID jobId, Time timeout) {
        return requestExecutionGraphInfoFunction.apply(jobId);
    }

    @Override
    public CompletableFuture<CheckpointStatsSnapshot> requestCheckpointStats(
            JobID jobId, Time timeout) {
        return requestCheckpointStatsSnapshotFunction.apply(jobId);
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
    public CompletableFuture<ThreadDumpInfo> requestThreadDump(Time timeout) {
        return null;
    }

    @Override
    public CompletableFuture<Acknowledge> triggerCheckpoint(
            AsynchronousJobOperationKey operationKey, CheckpointType checkpointType, Time timeout) {
        return triggerCheckpointFunction.apply(operationKey, checkpointType);
    }

    @Override
    public CompletableFuture<OperationResult<Long>> getTriggeredCheckpointStatus(
            AsynchronousJobOperationKey operationKey) {
        return getCheckpointStatusFunction.apply(operationKey);
    }

    @Override
    public CompletableFuture<Acknowledge> triggerSavepoint(
            AsynchronousJobOperationKey operationKey,
            String targetDirectory,
            SavepointFormatType formatType,
            TriggerSavepointMode savepointMode,
            Time timeout) {
        return triggerSavepointFunction.apply(operationKey, targetDirectory, formatType);
    }

    @Override
    public CompletableFuture<Acknowledge> stopWithSavepoint(
            AsynchronousJobOperationKey operationKey,
            String targetDirectory,
            SavepointFormatType formatType,
            TriggerSavepointMode savepointMode,
            Time timeout) {
        return stopWithSavepointFunction.apply(operationKey, targetDirectory, formatType);
    }

    @Override
    public CompletableFuture<OperationResult<String>> getTriggeredSavepointStatus(
            AsynchronousJobOperationKey operationKey) {
        return getSavepointStatusFunction.apply(operationKey);
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
        protected Function<JobID, CompletableFuture<ExecutionGraphInfo>>
                requestExecutionGraphInfoFunction;
        protected Function<JobID, CompletableFuture<CheckpointStatsSnapshot>>
                requestCheckpointStatsSnapshotFunction;
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
        protected Supplier<CompletableFuture<ThreadDumpInfo>> requestThreadDumpSupplier;
        protected Supplier<CompletableFuture<Acknowledge>> clusterShutdownSupplier;
        protected BiFunction<
                        AsynchronousJobOperationKey, CheckpointType, CompletableFuture<Acknowledge>>
                triggerCheckpointFunction;
        protected Function<AsynchronousJobOperationKey, CompletableFuture<OperationResult<Long>>>
                getCheckpointStatusFunction;
        protected TriFunction<
                        AsynchronousJobOperationKey,
                        String,
                        SavepointFormatType,
                        CompletableFuture<Acknowledge>>
                triggerSavepointFunction;
        protected TriFunction<
                        AsynchronousJobOperationKey,
                        String,
                        SavepointFormatType,
                        CompletableFuture<Acknowledge>>
                stopWithSavepointFunction;
        protected Function<AsynchronousJobOperationKey, CompletableFuture<OperationResult<String>>>
                getSavepointStatusFunction;
        protected TriFunction<
                        JobID,
                        OperatorID,
                        SerializedValue<CoordinationRequest>,
                        CompletableFuture<CoordinationResponse>>
                deliverCoordinationRequestToCoordinatorFunction;

        protected AbstractBuilder() {
            cancelJobFunction = DEFAULT_CANCEL_JOB_FUNCTION;
            requestJobFunction = DEFAULT_REQUEST_JOB_FUNCTION;
            requestExecutionGraphInfoFunction = DEFAULT_REQUEST_EXECUTION_GRAPH_INFO;
            requestCheckpointStatsSnapshotFunction = DEFAULT_REQUEST_CHECKPOINT_STATS_SNAPSHOT;
            requestJobResultFunction = DEFAULT_REQUEST_JOB_RESULT_FUNCTION;
            requestJobStatusFunction = DEFAULT_REQUEST_JOB_STATUS_FUNCTION;
            requestMultipleJobDetailsSupplier = DEFAULT_REQUEST_MULTIPLE_JOB_DETAILS_SUPPLIER;
            requestClusterOverviewSupplier = DEFAULT_REQUEST_CLUSTER_OVERVIEW_SUPPLIER;
            requestMetricQueryServiceGatewaysSupplier =
                    DEFAULT_REQUEST_METRIC_QUERY_SERVICE_PATHS_SUPPLIER;
            requestTaskManagerMetricQueryServiceGatewaysSupplier =
                    DEFAULT_REQUEST_TASK_MANAGER_METRIC_QUERY_SERVICE_PATHS_SUPPLIER;
            triggerCheckpointFunction = DEFAULT_TRIGGER_CHECKPOINT_FUNCTION;
            getCheckpointStatusFunction = DEFAULT_GET_CHECKPOINT_STATUS_FUNCTION;
            triggerSavepointFunction = DEFAULT_TRIGGER_SAVEPOINT_FUNCTION;
            stopWithSavepointFunction = DEFAULT_STOP_WITH_SAVEPOINT_FUNCTION;
            getSavepointStatusFunction = DEFAULT_GET_SAVEPOINT_STATUS_FUNCTION;
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

        public T setRequestExecutionGraphInfoFunction(
                Function<JobID, CompletableFuture<ExecutionGraphInfo>>
                        requestExecutionGraphInfoFunction) {
            this.requestExecutionGraphInfoFunction = requestExecutionGraphInfoFunction;
            return self();
        }

        public T setRequestCheckpointStatsSnapshotFunction(
                Function<JobID, CompletableFuture<CheckpointStatsSnapshot>>
                        requestCheckpointStatsSnapshotFunction) {
            this.requestCheckpointStatsSnapshotFunction = requestCheckpointStatsSnapshotFunction;
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

        public T setCancelJobFunction(
                Function<JobID, CompletableFuture<Acknowledge>> cancelJobFunction) {
            this.cancelJobFunction = cancelJobFunction;
            return self();
        }

        public T setTriggerCheckpointFunction(
                BiFunction<
                                AsynchronousJobOperationKey,
                                CheckpointType,
                                CompletableFuture<Acknowledge>>
                        triggerCheckpointFunction) {
            this.triggerCheckpointFunction = triggerCheckpointFunction;
            return self();
        }

        public T setGetCheckpointStatusFunction(
                Function<AsynchronousJobOperationKey, CompletableFuture<OperationResult<Long>>>
                        getCheckpointStatusFunction) {
            this.getCheckpointStatusFunction = getCheckpointStatusFunction;
            return self();
        }

        public T setTriggerSavepointFunction(
                TriFunction<
                                AsynchronousJobOperationKey,
                                String,
                                SavepointFormatType,
                                CompletableFuture<Acknowledge>>
                        triggerSavepointFunction) {
            this.triggerSavepointFunction = triggerSavepointFunction;
            return self();
        }

        public T setStopWithSavepointFunction(
                TriFunction<
                                AsynchronousJobOperationKey,
                                String,
                                SavepointFormatType,
                                CompletableFuture<Acknowledge>>
                        stopWithSavepointFunction) {
            this.stopWithSavepointFunction = stopWithSavepointFunction;
            return self();
        }

        public T setGetSavepointStatusFunction(
                Function<AsynchronousJobOperationKey, CompletableFuture<OperationResult<String>>>
                        getSavepointStatusFunction) {
            this.getSavepointStatusFunction = getSavepointStatusFunction;
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
                    requestExecutionGraphInfoFunction,
                    requestCheckpointStatsSnapshotFunction,
                    requestJobResultFunction,
                    requestJobStatusFunction,
                    requestMultipleJobDetailsSupplier,
                    requestClusterOverviewSupplier,
                    requestMetricQueryServiceGatewaysSupplier,
                    requestTaskManagerMetricQueryServiceGatewaysSupplier,
                    requestThreadDumpSupplier,
                    triggerCheckpointFunction,
                    getCheckpointStatusFunction,
                    triggerSavepointFunction,
                    stopWithSavepointFunction,
                    getSavepointStatusFunction,
                    clusterShutdownSupplier,
                    deliverCoordinationRequestToCoordinatorFunction);
        }
    }
}
