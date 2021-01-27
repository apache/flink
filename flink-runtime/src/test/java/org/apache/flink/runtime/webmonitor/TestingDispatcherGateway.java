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
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.ClusterOverview;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.function.TriFunction;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/** Testing implementation of the {@link DispatcherGateway}. */
public final class TestingDispatcherGateway extends TestingRestfulGateway
        implements DispatcherGateway {

    static final Function<JobGraph, CompletableFuture<Acknowledge>> DEFAULT_SUBMIT_FUNCTION =
            jobGraph -> CompletableFuture.completedFuture(Acknowledge.get());
    static final Supplier<CompletableFuture<Collection<JobID>>> DEFAULT_LIST_FUNCTION =
            () -> CompletableFuture.completedFuture(Collections.emptyList());
    static final int DEFAULT_BLOB_SERVER_PORT = 1234;
    static final DispatcherId DEFAULT_FENCING_TOKEN = DispatcherId.generate();
    static final Function<JobID, CompletableFuture<ArchivedExecutionGraph>>
            DEFAULT_REQUEST_ARCHIVED_JOB_FUNCTION =
                    jobID -> CompletableFuture.completedFuture(null);
    static final Function<ApplicationStatus, CompletableFuture<Acknowledge>>
            DEFAULT_SHUTDOWN_WITH_STATUS_FUNCTION =
                    status -> CompletableFuture.completedFuture(Acknowledge.get());

    private Function<JobGraph, CompletableFuture<Acknowledge>> submitFunction;
    private Supplier<CompletableFuture<Collection<JobID>>> listFunction;
    private int blobServerPort;
    private DispatcherId fencingToken;
    private Function<JobID, CompletableFuture<ArchivedExecutionGraph>> requestArchivedJobFunction;
    private Function<ApplicationStatus, CompletableFuture<Acknowledge>>
            clusterShutdownWithStatusFunction;

    public TestingDispatcherGateway() {
        super();
        submitFunction = DEFAULT_SUBMIT_FUNCTION;
        listFunction = DEFAULT_LIST_FUNCTION;
        blobServerPort = DEFAULT_BLOB_SERVER_PORT;
        fencingToken = DEFAULT_FENCING_TOKEN;
        requestArchivedJobFunction = DEFAULT_REQUEST_ARCHIVED_JOB_FUNCTION;
        clusterShutdownWithStatusFunction = DEFAULT_SHUTDOWN_WITH_STATUS_FUNCTION;
    }

    public TestingDispatcherGateway(
            String address,
            String hostname,
            Function<JobID, CompletableFuture<Acknowledge>> cancelJobFunction,
            Function<JobID, CompletableFuture<ArchivedExecutionGraph>> requestJobFunction,
            Function<JobID, CompletableFuture<ExecutionGraphInfo>>
                    requestExecutionGraphInfoFunction,
            Function<JobID, CompletableFuture<JobResult>> requestJobResultFunction,
            Function<JobID, CompletableFuture<JobStatus>> requestJobStatusFunction,
            Supplier<CompletableFuture<MultipleJobsDetails>> requestMultipleJobDetailsSupplier,
            Supplier<CompletableFuture<ClusterOverview>> requestClusterOverviewSupplier,
            Supplier<CompletableFuture<Collection<String>>>
                    requestMetricQueryServiceAddressesSupplier,
            Supplier<CompletableFuture<Collection<Tuple2<ResourceID, String>>>>
                    requestTaskManagerMetricQueryServiceGatewaysSupplier,
            BiFunction<JobID, String, CompletableFuture<String>> triggerSavepointFunction,
            BiFunction<JobID, String, CompletableFuture<String>> stopWithSavepointFunction,
            Function<JobGraph, CompletableFuture<Acknowledge>> submitFunction,
            Supplier<CompletableFuture<Collection<JobID>>> listFunction,
            int blobServerPort,
            DispatcherId fencingToken,
            Function<JobID, CompletableFuture<ArchivedExecutionGraph>> requestArchivedJobFunction,
            Supplier<CompletableFuture<Acknowledge>> clusterShutdownSupplier,
            Function<ApplicationStatus, CompletableFuture<Acknowledge>>
                    clusterShutdownWithStatusFunction,
            TriFunction<
                            JobID,
                            OperatorID,
                            SerializedValue<CoordinationRequest>,
                            CompletableFuture<CoordinationResponse>>
                    deliverCoordinationRequestToCoordinatorFunction) {
        super(
                address,
                hostname,
                cancelJobFunction,
                requestJobFunction,
                requestExecutionGraphInfoFunction,
                requestJobResultFunction,
                requestJobStatusFunction,
                requestMultipleJobDetailsSupplier,
                requestClusterOverviewSupplier,
                requestMetricQueryServiceAddressesSupplier,
                requestTaskManagerMetricQueryServiceGatewaysSupplier,
                triggerSavepointFunction,
                stopWithSavepointFunction,
                clusterShutdownSupplier,
                deliverCoordinationRequestToCoordinatorFunction);
        this.submitFunction = submitFunction;
        this.listFunction = listFunction;
        this.blobServerPort = blobServerPort;
        this.fencingToken = fencingToken;
        this.requestArchivedJobFunction = requestArchivedJobFunction;
        this.clusterShutdownWithStatusFunction = clusterShutdownWithStatusFunction;
    }

    @Override
    public CompletableFuture<Acknowledge> submitJob(JobGraph jobGraph, Time timeout) {
        return submitFunction.apply(jobGraph);
    }

    @Override
    public CompletableFuture<Collection<JobID>> listJobs(Time timeout) {
        return listFunction.get();
    }

    @Override
    public CompletableFuture<Integer> getBlobServerPort(Time timeout) {
        return CompletableFuture.completedFuture(blobServerPort);
    }

    @Override
    public DispatcherId getFencingToken() {
        return DEFAULT_FENCING_TOKEN;
    }

    public CompletableFuture<ArchivedExecutionGraph> requestJob(
            JobID jobId, @RpcTimeout Time timeout) {
        return requestArchivedJobFunction.apply(jobId);
    }

    @Override
    public CompletableFuture<Acknowledge> shutDownCluster(ApplicationStatus applicationStatus) {
        return clusterShutdownWithStatusFunction.apply(applicationStatus);
    }

    /** Builder for the {@link TestingDispatcherGateway}. */
    public static final class Builder extends TestingRestfulGateway.AbstractBuilder<Builder> {

        private Function<JobGraph, CompletableFuture<Acknowledge>> submitFunction;
        private Supplier<CompletableFuture<Collection<JobID>>> listFunction;
        private int blobServerPort;
        private DispatcherId fencingToken;
        private Function<JobID, CompletableFuture<ArchivedExecutionGraph>>
                requestArchivedJobFunction;
        private Function<ApplicationStatus, CompletableFuture<Acknowledge>>
                clusterShutdownWithStatusFunction = DEFAULT_SHUTDOWN_WITH_STATUS_FUNCTION;

        public Builder setSubmitFunction(
                Function<JobGraph, CompletableFuture<Acknowledge>> submitFunction) {
            this.submitFunction = submitFunction;
            return this;
        }

        public Builder setListFunction(
                Supplier<CompletableFuture<Collection<JobID>>> listFunction) {
            this.listFunction = listFunction;
            return this;
        }

        public Builder setRequestArchivedJobFunction(
                Function<JobID, CompletableFuture<ArchivedExecutionGraph>> requestJobFunction) {
            requestArchivedJobFunction = requestJobFunction;
            return this;
        }

        public Builder setClusterShutdownFunction(
                Function<ApplicationStatus, CompletableFuture<Acknowledge>>
                        clusterShutdownFunction) {
            this.clusterShutdownWithStatusFunction = clusterShutdownFunction;
            return this;
        }

        @Override
        public Builder setRequestJobFunction(
                Function<JobID, CompletableFuture<ArchivedExecutionGraph>> requestJobFunction) {
            // signature clash
            throw new UnsupportedOperationException("Use setRequestArchivedJobFunction() instead.");
        }

        @Override
        protected Builder self() {
            return this;
        }

        public Builder setBlobServerPort(int blobServerPort) {
            this.blobServerPort = blobServerPort;
            return this;
        }

        public Builder setFencingToken(DispatcherId fencingToken) {
            this.fencingToken = fencingToken;
            return this;
        }

        public TestingDispatcherGateway build() {
            return new TestingDispatcherGateway(
                    address,
                    hostname,
                    cancelJobFunction,
                    requestJobFunction,
                    requestExecutionGraphInfoFunction,
                    requestJobResultFunction,
                    requestJobStatusFunction,
                    requestMultipleJobDetailsSupplier,
                    requestClusterOverviewSupplier,
                    requestMetricQueryServiceGatewaysSupplier,
                    requestTaskManagerMetricQueryServiceGatewaysSupplier,
                    triggerSavepointFunction,
                    stopWithSavepointFunction,
                    submitFunction,
                    listFunction,
                    blobServerPort,
                    fencingToken,
                    requestArchivedJobFunction,
                    clusterShutdownSupplier,
                    clusterShutdownWithStatusFunction,
                    deliverCoordinationRequestToCoordinatorFunction);
        }
    }
}
