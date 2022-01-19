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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.webmonitor.RestfulGateway;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/** Gateway for the Dispatcher component. */
public interface DispatcherGateway extends FencedRpcGateway<DispatcherId>, RestfulGateway {

    /**
     * Submit a job to the dispatcher.
     *
     * @param jobGraph JobGraph to submit
     * @param timeout RPC timeout
     * @return A future acknowledge if the submission succeeded
     */
    CompletableFuture<Acknowledge> submitJob(JobGraph jobGraph, @RpcTimeout Time timeout);

    CompletableFuture<Acknowledge> submitFailedJob(
            JobID jobId, String jobName, Throwable exception);

    /**
     * List the current set of submitted jobs.
     *
     * @param timeout RPC timeout
     * @return A future collection of currently submitted jobs
     */
    CompletableFuture<Collection<JobID>> listJobs(@RpcTimeout Time timeout);

    /**
     * Returns the port of the blob server.
     *
     * @param timeout of the operation
     * @return A future integer of the blob server port
     */
    CompletableFuture<Integer> getBlobServerPort(@RpcTimeout Time timeout);

    default CompletableFuture<Acknowledge> shutDownCluster(ApplicationStatus applicationStatus) {
        return shutDownCluster();
    }

    default CompletableFuture<String> triggerCheckpoint(JobID jobID, @RpcTimeout Time timeout) {
        throw new UnsupportedOperationException();
    }

    /**
     * Triggers a savepoint with the given savepoint directory as a target, returning a future that
     * completes with the savepoint location when it is complete.
     *
     * @param jobId the job id
     * @param targetDirectory Target directory for the savepoint.
     * @param formatType Binary format of the savepoint.
     * @param savepointMode context of the savepoint operation
     * @param timeout Timeout for the asynchronous operation
     * @return Future which is completed once the operation is triggered successfully
     */
    default CompletableFuture<String> triggerSavepointAndGetLocation(
            JobID jobId,
            String targetDirectory,
            SavepointFormatType formatType,
            TriggerSavepointMode savepointMode,
            @RpcTimeout Time timeout) {
        throw new UnsupportedOperationException();
    }

    /**
     * Stops the job with a savepoint, returning a future that completes with the savepoint location
     * when the savepoint is completed.
     *
     * @param jobId the job id
     * @param targetDirectory Target directory for the savepoint.
     * @param savepointMode context of the savepoint operation
     * @param timeout for the rpc call
     * @return Future which is completed with the savepoint location once it is completed
     */
    default CompletableFuture<String> stopWithSavepointAndGetLocation(
            JobID jobId,
            String targetDirectory,
            SavepointFormatType formatType,
            TriggerSavepointMode savepointMode,
            @RpcTimeout final Time timeout) {
        throw new UnsupportedOperationException();
    }
}
