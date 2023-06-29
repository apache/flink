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
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.ClusterOverview;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.rest.messages.ThreadDumpInfo;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * * Gateway for restful endpoints without leader retrieval logic. * *
 *
 * <p>Gateways which implement this method run a REST endpoint which is reachable under the returned
 * address, and can be used in {@link org.apache.flink.runtime.rest.handler.AbstractHandler} without
 * leader retrieval logic.
 */
public class NonLeaderRetrievalRestfulGateway implements RestfulGateway {
    private static final String MESSAGE =
            "NonLeaderRetrievalRestfulGateway doesn't support the operation.";

    public static final NonLeaderRetrievalRestfulGateway INSTANCE =
            new NonLeaderRetrievalRestfulGateway();

    private NonLeaderRetrievalRestfulGateway() {}

    @Override
    public String getAddress() {
        throw new UnsupportedOperationException(MESSAGE);
    }

    @Override
    public String getHostname() {
        throw new UnsupportedOperationException(MESSAGE);
    }

    @Override
    public CompletableFuture<Acknowledge> cancelJob(JobID jobId, Time timeout) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    @Override
    public CompletableFuture<ExecutionGraphInfo> requestExecutionGraphInfo(
            JobID jobId, Time timeout) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    @Override
    public CompletableFuture<CheckpointStatsSnapshot> requestCheckpointStats(
            JobID jobId, Time timeout) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    @Override
    public CompletableFuture<JobResult> requestJobResult(JobID jobId, Time timeout) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    @Override
    public CompletableFuture<MultipleJobsDetails> requestMultipleJobDetails(Time timeout) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    @Override
    public CompletableFuture<ClusterOverview> requestClusterOverview(Time timeout) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    @Override
    public CompletableFuture<Collection<String>> requestMetricQueryServiceAddresses(Time timeout) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    @Override
    public CompletableFuture<Collection<Tuple2<ResourceID, String>>>
            requestTaskManagerMetricQueryServiceAddresses(Time timeout) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    @Override
    public CompletableFuture<ThreadDumpInfo> requestThreadDump(Time timeout) {
        throw new UnsupportedOperationException(MESSAGE);
    }
}
